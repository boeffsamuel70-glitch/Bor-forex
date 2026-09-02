import os
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
TIMEFRAME = "5min"
TIMEZONE = "America/Sao_Paulo"
TZ = ZoneInfo(TIMEZONE)
OUTPUTSIZE = 100
HORA_INICIO = 6
HORA_FIM = 22
MAX_ATRASO_MINUTOS = 8

ATIVOS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "EURJPY": "EUR/JPY",
}

estado = {
    codigo: {
        "symbol": symbol, "sinal": "AGUARDAR", "score": 0,
        "preco": None, "timestamp": None,
        "motivo": "Aguardando leitura", "status": "aguardando",
        "atualidade_min": None
    } for codigo, symbol in ATIVOS.items()
}

status_robo = {
    "status": "iniciando",
    "mensagem": "Preparando primeira leitura...",
    "ultima_leitura": None,
    "erro": None,
}

def log(msg):
    print(f"[BOT] {msg}", flush=True)

def dentro_do_horario():
    return HORA_INICIO <= datetime.now(TZ).hour < HORA_FIM

def obter_candles(symbol):
    if not API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY não configurada no Render.")
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol, "interval": TIMEFRAME, "outputsize": OUTPUTSIZE,
        "timezone": TIMEZONE, "apikey": API_KEY
    }
    log(f"Consultando Twelve Data: {symbol}")
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("status") == "error":
        raise RuntimeError(data.get("message", "Erro da Twelve Data."))
    values = data.get("values")
    if not values:
        raise RuntimeError(f"Nenhum candle retornado para {symbol}.")
    log(f"{symbol}: {len(values)} candles recebidos.")
    return values

def validar_atualidade(candles, symbol):
    datas = []
    for c in candles:
        txt = c.get("datetime")
        if not txt:
            continue
        try:
            dt = datetime.fromisoformat(txt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            datas.append(dt.astimezone(TZ))
        except ValueError:
            continue

    if not datas:
        return False, None, "Data dos candles inválida."

    ultimo = max(datas)
    atraso = (datetime.now(TZ) - ultimo).total_seconds() / 60
    atual = atraso <= MAX_ATRASO_MINUTOS
    log(f"{symbol}: último candle {ultimo.strftime('%Y-%m-%d %H:%M:%S')} | atraso={atraso:.1f} min | atual={'SIM' if atual else 'NÃO'}")
    if not atual:
        return False, atraso, f"Dados atrasados ({atraso:.1f} min). Sem sinal até receber candle atual."
    return True, atraso, ""

def somente_velas_fechadas(candles):
    agora = datetime.now(TZ)
    fechadas = []
    for c in candles:
        txt = c.get("datetime")
        if not txt:
            continue
        try:
            dt = datetime.fromisoformat(txt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
        except ValueError:
            continue
        if dt + timedelta(minutes=5) <= agora:
            fechadas.append(c)
    return fechadas if fechadas else candles[1:]

def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    result = sum(values[:period]) / period
    for value in values[period:]:
        result = value * k + result * (1 - k)
    return result

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h = float(candles[i]["high"])
        l = float(candles[i]["low"])
        pc = float(candles[i-1]["close"])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return sum(trs[-period:]) / period if len(trs) >= period else None

def analisar(candles):
    closes = [float(c["close"]) for c in candles]
    opens = [float(c["open"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    if len(closes) < 30:
        return "AGUARDAR", 0, "Poucos candles para análise."

    e5, e13, e21 = ema(closes,5), ema(closes,13), ema(closes,21)
    r = rsi(closes,14)
    a = atr(candles,14)
    if None in (e5, e13, e21, r, a):
        return "AGUARDAR", 0, "Indicadores ainda não disponíveis."

    close, op, high, low = closes[-1], opens[-1], highs[-1], lows[-1]
    strength = abs(close-op) / max(high-low, 1e-12)
    call = put = 0
    mc, mp = [], []

    if e5 > e13 > e21:
        call += 2; mc.append("EMA 5>13>21")
    elif e5 < e13 < e21:
        put += 2; mp.append("EMA 5<13<21")

    if 52 <= r <= 68:
        call += 1; mc.append("RSI favorável")
    elif 32 <= r <= 48:
        put += 1; mp.append("RSI favorável")

    if strength >= 0.55:
        if close > op:
            call += 1; mc.append("candle comprador forte")
        elif close < op:
            put += 1; mp.append("candle vendedor forte")

    if len(closes) >= 4:
        mom = close - closes[-4]
        if mom > 0:
            call += 1; mc.append("momentum positivo")
        elif mom < 0:
            put += 1; mp.append("momentum negativo")

    recent_range = max(closes[-10:]) - min(closes[-10:])
    if recent_range < a * 0.60:
        return "AGUARDAR", max(call, put), "Baixa volatilidade/lateralização."

    if call >= 4 and call > put:
        return "CALL", call, " + ".join(mc)
    if put >= 4 and put > call:
        return "PUT", put, " + ".join(mp)
    return "AGUARDAR", max(call, put), "Sem confluência suficiente."

def processar_ativo(codigo, symbol):
    try:
        candles_brutos = obter_candles(symbol)
        atual, atraso, motivo_atualidade = validar_atualidade(candles_brutos, symbol)
        if not atual:
            estado[codigo].update({
                "sinal": "AGUARDAR", "score": 0, "preco": None,
                "timestamp": None, "motivo": motivo_atualidade,
                "status": "dados_atrasados",
                "atualidade_min": round(atraso, 1) if atraso is not None else None
            })
            log(f"{symbol} -> BLOQUEADO: {motivo_atualidade}")
            return

        candles = somente_velas_fechadas(candles_brutos)
        if len(candles) < 30:
            estado[codigo].update({
                "sinal": "AGUARDAR", "score": 0, "preco": None,
                "timestamp": None, "motivo": "Poucos candles fechados disponíveis.",
                "status": "aguardando", "atualidade_min": round(atraso, 1)
            })
            return

        sinal, score, motivo = analisar(candles)
        ultimo = candles[-1]
        preco = float(ultimo["close"])
        ts = ultimo.get("datetime")
        estado[codigo].update({
            "sinal": sinal, "score": score, "preco": preco,
            "timestamp": ts, "motivo": motivo, "status": "ok",
            "atualidade_min": round(atraso, 1)
        })
        log(f"{symbol} -> {sinal} | score={score} | preço={preco} | vela={ts}")
    except Exception as e:
        estado[codigo]["status"] = "erro"
        estado[codigo]["motivo"] = str(e)
        log(f"ERRO {symbol}: {e}")

def esperar_ate_proxima_leitura():
    agora = datetime.now(TZ)
    boundary = ((agora.minute // 5) + 1) * 5
    if boundary >= 60:
        proxima = (agora + timedelta(hours=1)).replace(
            minute=1, second=0, microsecond=0)
    else:
        proxima = agora.replace(
            minute=boundary+1, second=0, microsecond=0)
    segundos = max(1, (proxima-agora).total_seconds())
    log(f"Próxima leitura: {proxima.strftime('%H:%M:%S')} BRT.")
    time.sleep(segundos)

def loop_robo():
    primeira = True
    log("Loop iniciado.")
    log(f"Ativos: {', '.join(ATIVOS.values())}")
    log(f"Horário: {HORA_INICIO:02d}:00–{HORA_FIM:02d}:00 BRT")
    log(f"API KEY configurada: {'SIM' if API_KEY else 'NÃO'}")
    log(f"Limite de atraso: {MAX_ATRASO_MINUTOS} minutos.")

    while True:
        if not dentro_do_horario():
            status_robo.update({"status":"fora_do_horario",
                "mensagem":"Fora do horário de operação.","erro":None})
            time.sleep(30)
            continue

        if primeira:
            log("Executando primeira leitura imediatamente.")
            primeira = False
        else:
            esperar_ate_proxima_leitura()

        status_robo.update({"status":"processando",
            "mensagem":"Analisando os ativos...","erro":None})

        for codigo, symbol in ATIVOS.items():
            processar_ativo(codigo, symbol)
            time.sleep(1)

        agora = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        status_robo.update({"status":"online",
            "mensagem":"Leitura concluída.","ultima_leitura":agora,"erro":None})
        log(f"Leitura concluída às {agora} BRT.")

HTML = """<!doctype html><html lang="pt-br"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bot Forex 5M</title>
<style>
body{font-family:Arial;background:#111;color:#eee;margin:0;padding:16px}
h1{font-size:22px}.box,.card{background:#1b1b1b;border-radius:14px;padding:14px;margin:10px 0}
.sinal{font-size:25px;font-weight:bold;margin:8px 0}.small{color:#aaa;font-size:13px}
</style></head><body><h1>🤖 Bot Forex — 5 minutos</h1>
<div class="box" id="status">Carregando...</div><div id="cards"></div>
<script>
async function atualizar(){
 try{
  const d=await (await fetch('/dados?ts='+Date.now())).json();
  document.getElementById('status').innerHTML='<b>Status:</b> '+d.robo.status+
  ' — '+d.robo.mensagem+(d.robo.ultima_leitura?'<br><span class="small">Última leitura: '+d.robo.ultima_leitura+'</span>':'')+
  (d.robo.erro?'<br><span class="small">Erro: '+d.robo.erro+'</span>':'');
  let h='';
  for(const a of Object.values(d.ativos)) h+=
  '<div class="card"><b>'+a.symbol+'</b><div class="sinal">'+a.sinal+
  '</div>Score: <b>'+a.score+'</b><br>Preço: '+(a.preco??'-')+
  '<br>Vela: '+(a.timestamp??'-')+'<br>Idade do dado: '+(a.atualidade_min??'-')+' min<br><span class="small">'+a.motivo+'</span></div>';
  document.getElementById('cards').innerHTML=h;
 }catch(e){document.getElementById('status').innerText='Erro: '+e}
}
atualizar();setInterval(atualizar,10000);
</script></body></html>"""

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/dados")
def dados():
    return jsonify({"robo":status_robo,"ativos":estado,
                    "servidor":datetime.now(TZ).isoformat()})

@app.route("/health")
def health():
    return jsonify({"status":"ok","api_key_configurada":bool(API_KEY)})

threading.Thread(target=loop_robo, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","10000")))
