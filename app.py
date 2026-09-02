```python
import os
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ============================================================
# CONFIGURAÇÕES
# ============================================================

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

TIMEFRAME = "5min"
TIMEZONE = "America/Sao_Paulo"
TZ = ZoneInfo(TIMEZONE)

OUTPUTSIZE = 100

HORA_INICIO = 6
HORA_FIM = 22

# Máximo de atraso permitido para os dados
MAX_ATRASO_MINUTOS = 8

ATIVOS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "EURJPY": "EUR/JPY",
}

# ============================================================
# ESTADO
# ============================================================

estado = {
    codigo: {
        "symbol": symbol,
        "sinal": "AGUARDAR",
        "score": 0,
        "preco": None,
        "timestamp": None,
        "motivo": "Aguardando leitura",
        "status": "aguardando",
        "atualidade_min": None,
    }
    for codigo, symbol in ATIVOS.items()
}

status_robo = {
    "status": "iniciando",
    "mensagem": "Preparando primeira leitura...",
    "ultima_leitura": None,
    "erro": None,
}

# Controle de inicialização do robô
_robo_lock = threading.Lock()
_robo_started = False


# ============================================================
# LOG
# ============================================================

def log(msg):
    print(f"[BOT] {msg}", flush=True)


# ============================================================
# HORÁRIO
# ============================================================

def dentro_do_horario():
    agora = datetime.now(TZ)
    return HORA_INICIO <= agora.hour < HORA_FIM


# ============================================================
# DATETIME DOS CANDLES
# ============================================================

def parse_datetime_candle(txt):
    """
    Converte o datetime recebido pela Twelve Data para
    America/Sao_Paulo.

    A API pode devolver timestamp com ou sem timezone.
    """

    if not txt:
        return None

    try:
        txt = str(txt).strip()

        # Trata Z no final
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"

        dt = datetime.fromisoformat(txt)

        # Se veio sem timezone, considera BRT
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)

        return dt.astimezone(TZ)

    except Exception:
        return None


# ============================================================
# ORDENAÇÃO DOS CANDLES
# ============================================================

def ordenar_candles(candles):
    """
    A Twelve Data pode devolver os candles do mais novo
    para o mais antigo.

    Aqui garantimos:
    mais antigo -> mais novo
    """

    validos = []

    for candle in candles:
        dt = parse_datetime_candle(candle.get("datetime"))

        if dt is not None:
            item = dict(candle)
            item["_dt"] = dt
            validos.append(item)

    validos.sort(key=lambda x: x["_dt"])

    return validos


# ============================================================
# TWELVE DATA
# ============================================================

def obter_candles(symbol):

    if not API_KEY:
        raise RuntimeError(
            "TWELVE_DATA_API_KEY não configurada no Render."
        )

    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "outputsize": OUTPUTSIZE,
        "timezone": TIMEZONE,
        "apikey": API_KEY,
    }

    log(f"Consultando Twelve Data: {symbol}")

    r = requests.get(
        url,
        params=params,
        timeout=20
    )

    r.raise_for_status()

    data = r.json()

    if data.get("status") == "error":
        raise RuntimeError(
            data.get(
                "message",
                "Erro da Twelve Data."
            )
        )

    values = data.get("values")

    if not values:
        raise RuntimeError(
            f"Nenhum candle retornado para {symbol}."
        )

    log(
        f"{symbol}: {len(values)} candles recebidos."
    )

    return values


# ============================================================
# SOMENTE CANDLES FECHADOS
# ============================================================

def somente_velas_fechadas(candles):

    agora = datetime.now(TZ)

    ordenados = ordenar_candles(candles)

    fechadas = []

    for candle in ordenados:

        dt = candle.get("_dt")

        if dt is None:
            continue

        # Candle de 5 minutos só é considerado fechado
        # quando passaram 5 minutos desde seu início.
        fechamento = dt + timedelta(minutes=5)

        if fechamento <= agora:
            fechadas.append(candle)

    return fechadas


# ============================================================
# IDADE DO ÚLTIMO DADO RECEBIDO
# ============================================================

def idade_do_ultimo_candle(candles):

    ordenados = ordenar_candles(candles)

    if not ordenados:
        return None, None

    ultimo = ordenados[-1]

    dt = ultimo["_dt"]

    agora = datetime.now(TZ)

    idade = (agora - dt).total_seconds() / 60

    return dt, idade


# ============================================================
# EMA
# ============================================================

def ema(values, period):

    if len(values) < period:
        return None

    k = 2 / (period + 1)

    result = sum(values[:period]) / period

    for value in values[period:]:
        result = (
            value * k
            + result * (1 - k)
        )

    return result


# ============================================================
# RSI
# ============================================================

def rsi(closes, period=14):

    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):

        d = closes[i] - closes[i - 1]

        gains.append(max(d, 0))
        losses.append(max(-d, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


# ============================================================
# ATR
# ============================================================

def atr(candles, period=14):

    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):

        h = float(candles[i]["high"])
        l = float(candles[i]["low"])

        pc = float(
            candles[i - 1]["close"]
        )

        trs.append(
            max(
                h - l,
                abs(h - pc),
                abs(l - pc)
            )
        )

    if len(trs) < period:
        return None

    return sum(trs[-period:]) / period


# ============================================================
# ANÁLISE
# ============================================================

def analisar(candles):

    # Mantemos a ordem cronológica:
    # antigo -> novo

    closes = [
        float(c["close"])
        for c in candles
    ]

    opens = [
        float(c["open"])
        for c in candles
    ]

    highs = [
        float(c["high"])
        for c in candles
    ]

    lows = [
        float(c["low"])
        for c in candles
    ]

    if len(closes) < 30:
        return (
            "AGUARDAR",
            0,
            "Poucos candles para análise."
        )

    e5 = ema(closes, 5)
    e13 = ema(closes, 13)
    e21 = ema(closes, 21)

    r = rsi(closes, 14)

    a = atr(candles, 14)

    if None in (e5, e13, e21, r, a):
        return (
            "AGUARDAR",
            0,
            "Indicadores ainda não disponíveis."
        )

    close = closes[-1]
    op = opens[-1]
    high = highs[-1]
    low = lows[-1]

    strength = (
        abs(close - op)
        / max(high - low, 1e-12)
    )

    call = 0
    put = 0

    mc = []
    mp = []

    # EMA
    if e5 > e13 > e21:

        call += 2
        mc.append("EMA 5>13>21")

    elif e5 < e13 < e21:

        put += 2
        mp.append("EMA 5<13<21")

    # RSI
    if 52 <= r <= 68:

        call += 1
        mc.append("RSI favorável")

    elif 32 <= r <= 48:

        put += 1
        mp.append("RSI favorável")

    # Força do candle
    if strength >= 0.55:

        if close > op:

            call += 1
            mc.append(
                "candle comprador forte"
            )

        elif close < op:

            put += 1
            mp.append(
                "candle vendedor forte"
            )

    # Momentum
    if len(closes) >= 4:

        mom = close - closes[-4]

        if mom > 0:

            call += 1
            mc.append("momentum positivo")

        elif mom < 0:

            put += 1
            mp.append("momentum negativo")

    # Lateralização
    recent_range = (
        max(closes[-10:])
        - min(closes[-10:])
    )

    if recent_range < a * 0.60:

        return (
            "AGUARDAR",
            max(call, put),
            "Baixa volatilidade/lateralização."
        )

    # Sinal
    if call >= 4 and call > put:

        return (
            "CALL",
            call,
            " + ".join(mc)
        )

    if put >= 4 and put > call:

        return (
            "PUT",
            put,
            " + ".join(mp)
        )

    return (
        "AGUARDAR",
        max(call, put),
        "Sem confluência suficiente."
    )


# ============================================================
# PROCESSAMENTO DO ATIVO
# ============================================================

def processar_ativo(codigo, symbol):

    try:

        # ----------------------------------------------------
        # 1. Buscar dados
        # ----------------------------------------------------

        raw_candles = obter_candles(symbol)

        # ----------------------------------------------------
        # 2. Verificar qual é o candle mais recente
        # ----------------------------------------------------

        ultimo_raw, atraso = idade_do_ultimo_candle(
            raw_candles
        )

        if ultimo_raw is None:

            raise RuntimeError(
                "Não foi possível identificar o timestamp dos candles."
            )

        log(
            f"{symbol} | último candle="
            f"{ultimo_raw.strftime('%Y-%m-%d %H:%M:%S')} BRT"
            f" | atraso={atraso:.1f} min"
        )

        # ----------------------------------------------------
        # 3. Bloquear dados muito antigos
        # ----------------------------------------------------

        if atraso > MAX_ATRASO_MINUTOS:

            estado[codigo].update({
                "sinal": "AGUARDAR",
                "score": 0,
                "preco": None,
                "timestamp": ultimo_raw.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "motivo": (
                    f"Dados atrasados "
                    f"({atraso:.1f} min)."
                ),
                "status": "dados_atrasados",
                "atualidade_min": round(
                    atraso, 1
                ),
            })

            log(
                f"{symbol} -> AGUARDAR | "
                f"dados atrasados "
                f"({atraso:.1f} min)"
            )

            return

        # ----------------------------------------------------
        # 4. Separar somente candles fechados
        # ----------------------------------------------------

        fechadas = somente_velas_fechadas(
            raw_candles
        )

        if len(fechadas) < 30:

            raise RuntimeError(
                "Poucos candles fechados para análise."
            )

        # ----------------------------------------------------
        # 5. Última vela FECHADA
        # ----------------------------------------------------

        ultimo = fechadas[-1]

        preco = float(
            ultimo["close"]
        )

        dt_vela = ultimo["_dt"]

        ts = dt_vela.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # ----------------------------------------------------
        # 6. Análise
        # ----------------------------------------------------

        sinal, score, motivo = analisar(
            fechadas
        )

        estado[codigo].update({
            "sinal": sinal,
            "score": score,
            "preco": preco,
            "timestamp": ts,
            "motivo": motivo,
            "status": "ok",
            "atualidade_min": round(
                atraso, 1
            ),
        })

        log(
            f"{symbol} -> {sinal} | "
            f"score={score} | "
            f"preço={preco} | "
            f"vela={ts} | "
            f"atraso={atraso:.1f} min"
        )

    except Exception as e:

        estado[codigo].update({
            "status": "erro",
            "motivo": str(e),
        })

        log(
            f"ERRO {symbol}: {e}"
        )


# ============================================================
# ESPERAR PRÓXIMA VELA
# ============================================================

def esperar_ate_proxima_leitura():

    agora = datetime.now(TZ)

    minutos_desde_hora = (
        agora.hour * 60
        + agora.minute
    )

    proximo_bloco = (
        (minutos_desde_hora // 5) + 1
    ) * 5

    hora = proximo_bloco // 60
    minuto = proximo_bloco % 60

    if hora >= 24:

        proxima = (
            agora
            + timedelta(days=1)
        ).replace(
            hour=0,
            minute=0,
            second=5,
            microsecond=0
        )

    else:

        proxima = agora.replace(
            hour=hora,
            minute=minuto,
            second=5,
            microsecond=0
        )

    segundos = max(
        1,
        (proxima - agora).total_seconds()
    )

    log(
        f"Próxima leitura: "
        f"{proxima.strftime('%H:%M:%S')} BRT."
    )

    time.sleep(segundos)


# ============================================================
# LOOP DO ROBÔ
# ============================================================

def loop_robo():

    primeira = True

    log("Loop iniciado.")

    log(
        f"Ativos: {', '.join(ATIVOS.values())}"
    )

    log(
        f"Horário: "
        f"{HORA_INICIO:02d}:00–"
        f"{HORA_FIM:02d}:00 BRT"
    )

    log(
        "API KEY configurada: "
        f"{'SIM' if API_KEY else 'NÃO'}"
    )

    while True:

        if not dentro_do_horario():

            status_robo.update({
                "status": "fora_do_horario",
                "mensagem": (
                    "Fora do horário de operação."
                ),
                "erro": None,
            })

            time.sleep(30)

            continue

        if primeira:

            log(
                "Executando primeira leitura imediatamente."
            )

            primeira = False

        else:

            esperar_ate_proxima_leitura()

        status_robo.update({
            "status": "processando",
            "mensagem": (
                "Analisando os ativos..."
            ),
            "erro": None,
        })

        for codigo, symbol in ATIVOS.items():

            processar_ativo(
                codigo,
                symbol
            )

            time.sleep(1)

        agora = datetime.now(TZ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        status_robo.update({
            "status": "online",
            "mensagem": "Leitura concluída.",
            "ultima_leitura": agora,
            "erro": None,
        })

        log(
            f"Leitura concluída às "
            f"{agora} BRT."
        )


# ============================================================
# INICIALIZAÇÃO SEGURA DO ROBÔ
# ============================================================

def garantir_robo_iniciado():

    global _robo_started

    if _robo_started:
        return

    with _robo_lock:

        if _robo_started:
            return

        _robo_started = True

        threading.Thread(
            target=loop_robo,
            daemon=True
        ).start()

        log(
            "Thread do robô iniciada no worker web."
        )


@app.before_request
def iniciar_robo():

    garantir_robo_iniciado()


# ============================================================
# HTML
# ============================================================

HTML = """
<!doctype html>

<html lang="pt-br">

<head>

<meta charset="utf-8">

<meta name="viewport"
      content="width=device-width,initial-scale=1">

<title>Bot Forex 5M</title>

<style>

body{
    font-family:Arial;
    background:#111;
    color:#eee;
    margin:0;
    padding:16px;
}

h1{
    font-size:22px;
}

.box,.card{
    background:#1b1b1b;
    border-radius:14px;
    padding:14px;
    margin:10px 0;
}

.sinal{
    font-size:25px;
    font-weight:bold;
    margin:8px 0;
}

.small{
    color:#aaa;
    font-size:13px;
}

</style>

</head>

<body>

<h1>🤖 Bot Forex — 5 minutos</h1>

<div class="box" id="status">
Carregando...
</div>

<div id="cards"></div>

<script>

async function atualizar(){

    try{

        const d =
            await (
                await fetch(
                    '/dados?ts='
                    + Date.now()
                )
            ).json();

        document.getElementById(
            'status'
        ).innerHTML =
            '<b>Status:</b> '
            + d.robo.status
            + ' — '
            + d.robo.mensagem

            + (
                d.robo.ultima_leitura
                ?
                '<br><span class="small">'
                + 'Última leitura: '
                + d.robo.ultima_leitura
                + '</span>'
                :
                ''
            )

            + (
                d.robo.erro
                ?
                '<br><span class="small">'
                + 'Erro: '
                + d.robo.erro
                + '</span>'
                :
                ''
            );

        let h = '';

        for(
            const a
            of Object.values(d.ativos)
        ){

            h +=

            '<div class="card">'

            + '<b>'
            + a.symbol
            + '</b>'

            + '<div class="sinal">'
            + a.sinal
            + '</div>'

            + 'Score: <b>'
            + a.score
            + '</b>'

            + '<br>Preço: '
            + (a.preco ?? '-')

            + '<br>Vela: '
            + (a.timestamp ?? '-')

            + '<br>Idade do dado: '
            + (
                a.atualidade_min != null
                ?
                a.atualidade_min
                + ' min'
                :
                '-'
            )

            + '<br><span class="small">'
            + a.motivo
            + '</span>'

            + '</div>';
        }

        document.getElementById(
            'cards'
        ).innerHTML = h;

    }

    catch(e){

        document.getElementById(
            'status'
        ).innerText =
            'Erro: ' + e;
    }
}

atualizar();

setInterval(
    atualizar,
    10000
);

</script>

</body>

</html>
"""


# ============================================================
# ROTAS
# ============================================================

@app.route("/")
def index():

    garantir_robo_iniciado()

    return render_template_string(
        HTML
    )


@app.route("/dados")
def dados():

    garantir_robo_iniciado()

    return jsonify({
        "robo": status_robo,
        "ativos": estado,
        "servidor": datetime.now(
            TZ
        ).isoformat(),
    })


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "api_key_configurada": bool(
            API_KEY
        ),
    })


# ============================================================
# EXECUÇÃO LOCAL
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        )
    )
```
