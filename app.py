import os
import time
import threading
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests
from flask import Flask, jsonify, render_template_string

# ============================================================
# CONFIGURAÇÃO
# ============================================================

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
BASE_URL = "https://api.twelvedata.com/time_series"

TIMEFRAME = "5min"
TIMEZONE = "America/Sao_Paulo"
OUTPUTSIZE = 100

# Plano gratuito da Twelve Data:
# 3 ativos x 1 consulta a cada 5 minutos = 864 créditos/dia se rodar 24h.
# Para ficar abaixo dos 800 créditos/dia, este projeto opera das 06h às 22h.
HORA_INICIO = 6
HORA_FIM = 22

ATIVOS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "EURJPY": "EUR/JPY",
}

PONTOS_MINIMOS = 6
LEAD_MINIMO = 2

app = Flask(__name__)

status_robo = {
    "status": "iniciando",
    "ultima_atualizacao": None,
    "ultimo_sinal": None,
    "ativos": {},
    "mensagem": "Aguardando primeira leitura..."
}

lock_status = threading.Lock()
ultimo_candle_processado = {}


# ============================================================
# DADOS
# ============================================================

def obter_candles(symbol):
    if not API_KEY:
        raise RuntimeError(
            "TWELVE_DATA_API_KEY não configurada no Render."
        )

    params = {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "outputsize": OUTPUTSIZE,
        "timezone": TIMEZONE,
        "order": "desc",
        "apikey": API_KEY,
    }

    resposta = requests.get(BASE_URL, params=params, timeout=20)
    resposta.raise_for_status()
    dados = resposta.json()

    if "status" in dados and dados["status"] == "error":
        raise RuntimeError(dados.get("message", "Erro da Twelve Data."))

    values = dados.get("values")
    if not values:
        raise RuntimeError("Twelve Data não retornou candles.")

    df = pd.DataFrame(values)

    colunas = ["datetime", "open", "high", "low", "close"]
    df = df[colunas].copy()

    for coluna in ["open", "high", "low", "close"]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna().sort_values("datetime").reset_index(drop=True)

    if len(df) < 40:
        raise RuntimeError("Poucos candles recebidos para calcular os indicadores.")

    return df


# ============================================================
# INDICADORES
# ============================================================

def calcular_indicadores(df):
    df = df.copy()

    df["ema5"] = df["close"].ewm(span=5, adjust=False).mean()
    df["ema13"] = df["close"].ewm(span=13, adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()

    delta = df["close"].diff()
    ganhos = delta.clip(lower=0)
    perdas = -delta.clip(upper=0)

    media_ganho = ganhos.ewm(alpha=1 / 14, adjust=False).mean()
    media_perda = perdas.ewm(alpha=1 / 14, adjust=False).mean()

    rs = media_ganho / media_perda.replace(0, pd.NA)
    df["rsi14"] = 100 - (100 / (1 + rs))
    df["rsi14"] = df["rsi14"].fillna(50)

    anterior_close = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - anterior_close).abs()
    tr3 = (df["low"] - anterior_close).abs()

    df["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr14"] = df["tr"].rolling(14).mean()

    df["body"] = (df["close"] - df["open"]).abs()
    df["range"] = (df["high"] - df["low"]).replace(0, pd.NA)
    df["body_ratio"] = (df["body"] / df["range"]).fillna(0)

    return df.dropna().reset_index(drop=True)


def somente_velas_fechadas(df):
    df = df.copy()

    agora = datetime.now(pytz.timezone(TIMEZONE)).replace(tzinfo=None)

    # A última vela pode ainda estar aberta.
    if len(df):
        ultima = df.iloc[-1]["datetime"]

        # Vela de 5 minutos: consideramos fechada 5 minutos após o início.
        if agora < ultima + timedelta(minutes=5):
            df = df.iloc[:-1].copy()

    return df.reset_index(drop=True)


# ============================================================
# ESTRATÉGIA
# ============================================================

def analisar(df):
    if len(df) < 30:
        return {
            "sinal": "AGUARDAR",
            "pontos": 0,
            "confianca": 0,
            "motivos": ["Dados insuficientes."]
        }

    atual = df.iloc[-1]
    anterior = df.iloc[-2]

    call = 0
    put = 0
    motivos_call = []
    motivos_put = []
    bloqueios = []

    # 1) Tendência
    if atual["close"] > atual["ema21"] and atual["ema5"] > atual["ema13"]:
        call += 2
        motivos_call.append("Preço e EMA5 acima da EMA21.")
    elif atual["close"] < atual["ema21"] and atual["ema5"] < atual["ema13"]:
        put += 2
        motivos_put.append("Preço e EMA5 abaixo da EMA21.")

    # 2) Alinhamento das médias
    if atual["ema5"] > atual["ema13"] > atual["ema21"]:
        call += 2
        motivos_call.append("EMAs 5/13/21 alinhadas para alta.")
    elif atual["ema5"] < atual["ema13"] < atual["ema21"]:
        put += 2
        motivos_put.append("EMAs 5/13/21 alinhadas para baixa.")

    # 3) RSI
    if 52 <= atual["rsi14"] <= 70:
        call += 1
        motivos_call.append(f"RSI favorável à alta ({atual['rsi14']:.1f}).")
    elif 30 <= atual["rsi14"] <= 48:
        put += 1
        motivos_put.append(f"RSI favorável à baixa ({atual['rsi14']:.1f}).")

    # 4) Força do candle
    if atual["body_ratio"] >= 0.55:
        if atual["close"] > atual["open"]:
            call += 2
            motivos_call.append("Candle de alta com corpo forte.")
        elif atual["close"] < atual["open"]:
            put += 2
            motivos_put.append("Candle de baixa com corpo forte.")

    # 5) Momentum
    if atual["close"] > anterior["close"]:
        call += 1
        motivos_call.append("Fechamento acima do candle anterior.")
    elif atual["close"] < anterior["close"]:
        put += 1
        motivos_put.append("Fechamento abaixo do candle anterior.")

    # Filtros de mercado ruim
    media_atr = df["atr14"].tail(10).mean()

    if pd.notna(media_atr) and atual["atr14"] < media_atr * 0.80:
        bloqueios.append("Volatilidade abaixo da média.")

    if abs(atual["ema5"] - atual["ema21"]) < atual["atr14"] * 0.15:
        bloqueios.append("Mercado muito lateral.")

    if bloqueios:
        return {
            "sinal": "AGUARDAR",
            "pontos": max(call, put),
            "confianca": 0,
            "motivos": bloqueios
        }

    if call > put:
        pontos = call
        lead = call - put
        sinal = "CALL"
        motivos = motivos_call
    elif put > call:
        pontos = put
        lead = put - call
        sinal = "PUT"
        motivos = motivos_put
    else:
        return {
            "sinal": "AGUARDAR",
            "pontos": 0,
            "confianca": 0,
            "motivos": ["Confluência dividida entre CALL e PUT."]
        }

    if pontos < PONTOS_MINIMOS or lead < LEAD_MINIMO:
        return {
            "sinal": "AGUARDAR",
            "pontos": pontos,
            "confianca": round((pontos / 8) * 100),
            "motivos": ["Confluência insuficiente para liberar entrada."]
        }

    return {
        "sinal": sinal,
        "pontos": pontos,
        "confianca": min(100, round((pontos / 8) * 100)),
        "motivos": motivos
    }


# ============================================================
# PROCESSAMENTO
# ============================================================

def processar_ativo(codigo, symbol):
    try:
        df = obter_candles(symbol)
        df = calcular_indicadores(df)
        df = somente_velas_fechadas(df)

        if len(df) < 30:
            raise RuntimeError("Poucos candles fechados.")

        analise = analisar(df)
        atual = df.iloc[-1]

        candle_time = atual["datetime"]
        entrada = candle_time + timedelta(minutes=5)
        expiracao = entrada + timedelta(minutes=5)

        resultado = {
            "ativo": codigo,
            "symbol": symbol,
            "sinal": analise["sinal"],
            "preco": round(float(atual["close"]), 5),
            "rsi": round(float(atual["rsi14"]), 2),
            "pontos": int(analise["pontos"]),
            "confianca": int(analise["confianca"]),
            "candle": str(candle_time),
            "entrada": str(entrada),
            "expiracao": str(expiracao),
            "motivos": analise["motivos"],
            "atualizado_em": datetime.now(
                pytz.timezone(TIMEZONE)
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }

        with lock_status:
            status_robo["ativos"][codigo] = resultado
            status_robo["ultima_atualizacao"] = resultado["atualizado_em"]
            status_robo["status"] = "online"
            status_robo["mensagem"] = "Leitura concluída."

            if analise["sinal"] in ("CALL", "PUT"):
                ultimo = ultimo_candle_processado.get(codigo)

                if ultimo != str(candle_time):
                    ultimo_candle_processado[codigo] = str(candle_time)
                    status_robo["ultimo_sinal"] = resultado

        return resultado

    except Exception as e:
        resultado = {
            "ativo": codigo,
            "symbol": symbol,
            "sinal": "ERRO",
            "mensagem": str(e),
            "atualizado_em": datetime.now(
                pytz.timezone(TIMEZONE)
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }

        with lock_status:
            status_robo["ativos"][codigo] = resultado
            status_robo["status"] = "atenção"

        return resultado


def dentro_do_horario():
    agora = datetime.now(pytz.timezone(TIMEZONE))
    return (
        agora.weekday() < 5
        and HORA_INICIO <= agora.hour < HORA_FIM
    )


def esperar_proxima_leitura():
    tz = pytz.timezone(TIMEZONE)

    while True:
        agora = datetime.now(tz)

        # Executa aproximadamente 1 minuto após cada fechamento de 5 minutos.
        minutos = agora.minute
        alvo = (minutos // 5) * 5 + 1

        if alvo >= 60:
            proximo = (agora + timedelta(hours=1)).replace(
                minute=1, second=0, microsecond=0
            )
        else:
            proximo = agora.replace(
                minute=alvo, second=0, microsecond=0
            )

        segundos = (proximo - agora).total_seconds()

        if segundos > 0:
            time.sleep(min(segundos, 30))
        else:
            return


def loop_robo():
    while True:
        try:
            if not dentro_do_horario():
                with lock_status:
                    status_robo["status"] = "fora do horário"
                    status_robo["mensagem"] = (
                        f"Operação configurada entre {HORA_INICIO:02d}:00 "
                        f"e {HORA_FIM:02d}:00 (Brasília), segunda a sexta."
                    )

                time.sleep(30)
                continue

            esperar_proxima_leitura()

            if not dentro_do_horario():
                continue

            for codigo, symbol in ATIVOS.items():
                processar_ativo(codigo, symbol)
                time.sleep(1)

        except Exception as e:
            with lock_status:
                status_robo["status"] = "erro"
                status_robo["mensagem"] = str(e)

            time.sleep(30)


# ============================================================
# INTERFACE
# ============================================================

HTML = """
<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bot Forex 5M</title>
<style>
body{font-family:Arial,sans-serif;background:#111;color:#eee;margin:0;padding:16px}
h1{font-size:22px;margin:0 0 6px}
.sub{color:#aaa;font-size:13px;margin-bottom:16px}
.card{background:#1c1c1c;border:1px solid #333;border-radius:14px;padding:15px;margin-bottom:12px}
.ativo{font-size:18px;font-weight:bold}
.sinal{font-size:28px;font-weight:bold;margin:8px 0}
.call{color:#32d583}.put{color:#ff6b6b}.wait{color:#ffd166}.erro{color:#ff6b6b}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px}
.item{background:#151515;border-radius:8px;padding:8px}
.motivos{color:#bbb;font-size:13px;margin-top:10px}
small{color:#888}
</style>
</head>
<body>
<h1>🤖 Bot Forex 5M</h1>
<div class="sub">
EUR/USD • GBP/USD • EUR/JPY • mercado aberto • sinais para próxima vela
</div>

<div id="app">Carregando...</div>

<script>
async function atualizar(){
    const r = await fetch('/dados');
    const d = await r.json();

    let html = `
      <div class="card">
        <b>Status:</b> ${d.status}<br>
        <small>${d.mensagem || ''}</small><br>
        <small>Última atualização: ${d.ultima_atualizacao || '-'}</small>
      </div>
    `;

    for(const [codigo,a] of Object.entries(d.ativos || {})){
        let classe = 'wait';
        if(a.sinal === 'CALL') classe='call';
        if(a.sinal === 'PUT') classe='put';
        if(a.sinal === 'ERRO') classe='erro';

        html += `
        <div class="card">
          <div class="ativo">${a.ativo || codigo} (${a.symbol || ''})</div>
          <div class="sinal ${classe}">${a.sinal || 'AGUARDAR'}</div>
          <div class="grid">
            <div class="item">Preço<br><b>${a.preco ?? '-'}</b></div>
            <div class="item">RSI<br><b>${a.rsi ?? '-'}</b></div>
            <div class="item">Confluência<br><b>${a.pontos ?? '-'} pts</b></div>
            <div class="item">Score<br><b>${a.confianca ?? '-'}%</b></div>
          </div>
          <div class="motivos">
            ${(a.motivos || [a.mensagem || '']).map(x=>'• '+x).join('<br>')}
          </div>
          <br>
          <small>Entrada: ${a.entrada || '-'}</small><br>
          <small>Expiração: ${a.expiracao || '-'}</small>
        </div>`;
    }

    document.getElementById('app').innerHTML = html;
}

atualizar();
setInterval(atualizar, 15000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/dados")
def dados():
    with lock_status:
        return jsonify(status_robo)


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now(
            pytz.timezone(TIMEZONE)
        ).isoformat()
    })


# ============================================================
# START
# ============================================================

threading.Thread(target=loop_robo, daemon=True).start()

if __name__ == "__main__":
    porta = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=porta, debug=False)
