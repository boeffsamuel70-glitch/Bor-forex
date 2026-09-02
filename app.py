import os
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(**name**)

# ============================================================

# CONFIGURAÇÃO

# ============================================================

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

TIMEFRAME = "5min"
TIMEZONE = "America/Sao_Paulo"
TZ = ZoneInfo(TIMEZONE)

OUTPUTSIZE = 100

HORA_INICIO = 6
HORA_FIM = 22

# Se o último dado estiver mais atrasado que isso,

# o robô não gera sinal.

MAX_ATRASO_MINUTOS = 8

ATIVOS = {
"EURUSD": "EUR/USD",
"GBPUSD": "GBP/USD",
"EURJPY": "EUR/JPY",
}

# ============================================================

# ESTADO DO ROBÔ

# ============================================================

estado = {
"ativo": "-",
"sinal": "AGUARDAR",
"score": 0,
"preco": "-",
"vela": "-",
"atualizado": "-",
"atualidade_min": "-",
"mensagem": "Aguardando primeira leitura.",
"detalhes": {
"score_call": "-",
"score_put": "-",
"rsi": "-",
"ema5": "-",
"ema13": "-",
"ema21": "-",
},
}

_robo_lock = threading.Lock()
_robo_started = False

# ============================================================

# LOG

# ============================================================

def log(msg):
agora = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
print(f"[BOT] {msg}", flush=True)

# ============================================================

# DATA E HORA

# ============================================================

def agora_brt():
return datetime.now(TZ)

def parse_datetime_candle(txt):
if not txt:
return None

```
txt = str(txt).strip()

try:
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"

    dt = datetime.fromisoformat(txt)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)

    return dt.astimezone(TZ)

except Exception:
    formatos = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in formatos:
        try:
            return datetime.strptime(
                str(txt),
                fmt
            ).replace(tzinfo=TZ)
        except Exception:
            pass

return None
```

# ============================================================

# ORDENAÇÃO DAS VELAS

# ============================================================

def ordenar_candles(candles):
resultado = []

```
for candle in candles:
    item = dict(candle)

    dt = parse_datetime_candle(
        item.get("datetime")
    )

    if dt is not None:
        item["_dt"] = dt
        resultado.append(item)

# Twelve Data normalmente retorna do mais novo
# para o mais antigo. Aqui forçamos ordem crescente.
resultado.sort(
    key=lambda x: x["_dt"]
)

return resultado
```

def somente_velas_fechadas(candles):
candles = ordenar_candles(candles)

```
agora = agora_brt()

fechadas = []

for candle in candles:
    dt = candle["_dt"]

    # Timeframe de 5 minutos.
    # A vela só entra na análise depois de fechada.
    if dt + timedelta(minutes=5) <= agora:
        fechadas.append(candle)

return fechadas
```

def idade_do_ultimo_candle(candles):
ordenadas = ordenar_candles(candles)

```
if not ordenadas:
    return None, None

ultimo = ordenadas[-1]

dt = ultimo["_dt"]

idade = (
    agora_brt() - dt
).total_seconds() / 60

return ultimo, idade
```

# ============================================================

# TWELVE DATA

# ============================================================

def obter_candles(symbol):
if not API_KEY:
raise RuntimeError(
"TWELVE_DATA_API_KEY não configurada no Render."
)

```
url = (
    "https://api.twelvedata.com/time_series"
)

params = {
    "symbol": symbol,
    "interval": TIMEFRAME,
    "outputsize": OUTPUTSIZE,
    "timezone": TIMEZONE,
    "apikey": API_KEY,
}

resposta = requests.get(
    url,
    params=params,
    timeout=20,
)

resposta.raise_for_status()

dados = resposta.json()

if dados.get("status") == "error":
    raise RuntimeError(
        dados.get(
            "message",
            "Erro retornado pela Twelve Data."
        )
    )

values = dados.get("values")

if not values:
    raise RuntimeError(
        f"Nenhuma vela recebida para {symbol}: {dados}"
    )

candles = ordenar_candles(values)

if not candles:
    raise RuntimeError(
        f"Não foi possível interpretar as datas de {symbol}."
    )

return candles
```

# ============================================================

# INDICADORES

# ============================================================

def closes(candles):
return [
float(c["close"])
for c in candles
]

def highs(candles):
return [
float(c["high"])
for c in candles
]

def lows(candles):
return [
float(c["low"])
for c in candles
]

def ema(values, period):
if len(values) < period:
return None

```
k = 2 / (period + 1)

valor = sum(
    values[:period]
) / period

for preco in values[period:]:
    valor = (
        preco * k
    ) + (
        valor * (1 - k)
    )

return valor
```

def rsi(values, period=14):
if len(values) < period + 1:
return None

```
ganhos = []
perdas = []

for i in range(1, len(values)):
    diferenca = (
        values[i] - values[i - 1]
    )

    ganhos.append(
        max(diferenca, 0)
    )

    perdas.append(
        max(-diferenca, 0)
    )

avg_gain = (
    sum(ganhos[:period])
    / period
)

avg_loss = (
    sum(perdas[:period])
    / period
)

for i in range(
    period,
    len(ganhos)
):
    avg_gain = (
        (
            avg_gain * (period - 1)
        )
        + ganhos[i]
    ) / period

    avg_loss = (
        (
            avg_loss * (period - 1)
        )
        + perdas[i]
    ) / period

if avg_loss == 0:
    return 100.0

rs = avg_gain / avg_loss

return 100 - (
    100 / (1 + rs)
)
```

def atr(candles, period=14):
if len(candles) < period + 1:
return None

```
trs = []

for i in range(1, len(candles)):
    atual = candles[i]
    anterior = candles[i - 1]

    high = float(atual["high"])
    low = float(atual["low"])

    close_anterior = float(
        anterior["close"]
    )

    tr = max(
        high - low,
        abs(
            high - close_anterior
        ),
        abs(
            low - close_anterior
        ),
    )

    trs.append(tr)

if len(trs) < period:
    return None

return (
    sum(trs[-period:])
    / period
)
```

# ============================================================

# ANÁLISE

# ============================================================

def analisar(candles):

```
if len(candles) < 30:

    return {
        "sinal": "AGUARDAR",
        "score": 0,
        "preco": (
            float(candles[-1]["close"])
            if candles
            else 0
        ),
        "vela": (
            candles[-1]["_dt"]
            if candles
            else None
        ),
        "mensagem": (
            "Poucas velas para análise."
        ),
    }

c = closes(candles)
h = highs(candles)
l = lows(candles)

preco = c[-1]

ema5 = ema(c, 5)
ema13 = ema(c, 13)
ema21 = ema(c, 21)

rsi14 = rsi(c, 14)
atr14 = atr(candles, 14)

ultima = candles[-1]

abertura = float(
    ultima["open"]
)

fechamento = float(
    ultima["close"]
)

maxima = float(
    ultima["high"]
)

minima = float(
    ultima["low"]
)

corpo = abs(
    fechamento - abertura
)

range_vela = max(
    maxima - minima,
    1e-10
)

forca_corpo = (
    corpo / range_vela
)

score_call = 0
score_put = 0

# --------------------------------------------------------
# TENDÊNCIA POR EMA
# --------------------------------------------------------

if (
    ema5 is not None
    and ema13 is not None
    and ema21 is not None
):

    if ema5 > ema13 > ema21:
        score_call += 2

    elif ema5 < ema13 < ema21:
        score_put += 2

    elif ema5 > ema13:
        score_call += 1

    elif ema5 < ema13:
        score_put += 1

# --------------------------------------------------------
# RSI
# --------------------------------------------------------

if rsi14 is not None:

    if 50 <= rsi14 <= 70:
        score_call += 1

    elif 30 <= rsi14 < 50:
        score_put += 1

    # Evita perseguir movimento extremamente esticado.
    if rsi14 > 75:
        score_call -= 1

    if rsi14 < 25:
        score_put -= 1

# --------------------------------------------------------
# FORÇA DA ÚLTIMA VELA
# --------------------------------------------------------

if (
    fechamento > abertura
    and forca_corpo >= 0.50
):
    score_call += 1

elif (
    fechamento < abertura
    and forca_corpo >= 0.50
):
    score_put += 1

# --------------------------------------------------------
# MOMENTUM
# --------------------------------------------------------

if len(c) >= 4:

    movimento = (
        c[-1] - c[-4]
    )

    if movimento > 0:
        score_call += 1

    elif movimento < 0:
        score_put += 1

# --------------------------------------------------------
# FILTRO DE BAIXA VOLATILIDADE
# --------------------------------------------------------

if (
    atr14 is not None
    and preco != 0
):

    atr_percentual = (
        atr14 / preco
    )

    if atr_percentual < 0.00008:

        score_call = min(
            score_call,
            2
        )

        score_put = min(
            score_put,
            2
        )

# --------------------------------------------------------
# DECISÃO
# --------------------------------------------------------

diferenca = abs(
    score_call - score_put
)

if (
    score_call >= 4
    and diferenca >= 2
):

    sinal = "CALL"
    score = score_call

elif (
    score_put >= 4
    and diferenca >= 2
):

    sinal = "PUT"
    score = score_put

else:

    sinal = "AGUARDAR"
    score = max(
        score_call,
        score_put
    )

mensagem = (
    f"CALL={score_call} | "
    f"PUT={score_put}"
)

if rsi14 is not None:
    mensagem += (
        f" | RSI={rsi14:.2f}"
    )

return {
    "sinal": sinal,
    "score": score,
    "preco": preco,
    "vela": ultima["_dt"],
    "rsi": rsi14,
    "ema5": ema5,
    "ema13": ema13,
    "ema21": ema21,
    "atr": atr14,
    "corpo_forca": forca_corpo,
    "score_call": score_call,
    "score_put": score_put,
    "mensagem": mensagem,
}
```

# ============================================================

# PROCESSAMENTO DE CADA ATIVO

# ============================================================

def processar_ativo(chave, symbol):

```
global estado

try:

    log(
        f"Consultando Twelve Data: {symbol}"
    )

    candles = obter_candles(
        symbol
    )

    log(
        f"{symbol}: "
        f"{len(candles)} candles recebidos."
    )

    ultimo_raw, idade = (
        idade_do_ultimo_candle(
            candles
        )
    )

    if ultimo_raw is None:
        raise RuntimeError(
            "Não foi possível identificar "
            "o último candle."
        )

    log(
        f"{symbol} | "
        f"último candle="
        f"{ultimo_raw['_dt'].strftime('%Y-%m-%d %H:%M:%S')} BRT | "
        f"atraso={idade:.2f} min"
    )

    # ----------------------------------------------------
    # FILTRO DE DADO ATRASADO
    # ----------------------------------------------------

    if idade > MAX_ATRASO_MINUTOS:

        resultado = {
            "sinal": "AGUARDAR",
            "score": 0,
            "preco": float(
                ultimo_raw["close"]
            ),
            "vela": ultimo_raw["_dt"],
            "mensagem": (
                f"Dado atrasado "
                f"({idade:.1f} min). "
                f"Aguardando atualização."
            ),
        }

    else:

        fechadas = (
            somente_velas_fechadas(
                candles
            )
        )

        if len(fechadas) < 30:

            resultado = {
                "sinal": "AGUARDAR",
                "score": 0,
                "preco": float(
                    ultimo_raw["close"]
                ),
                "vela": ultimo_raw["_dt"],
                "mensagem": (
                    "Velas fechadas "
                    "insuficientes."
                ),
            }

        else:

            # IMPORTANTE:
            # A análise usa a última vela fechada.
            resultado = analisar(
                fechadas
            )

    # ----------------------------------------------------
    # ATUALIZA ESTADO
    # ----------------------------------------------------

    estado["ativo"] = symbol

    estado["sinal"] = (
        resultado["sinal"]
    )

    estado["score"] = (
        resultado["score"]
    )

    preco_resultado = (
        resultado.get("preco")
    )

    if isinstance(
        preco_resultado,
        (float, int)
    ):

        estado["preco"] = (
            f"{preco_resultado:.5f}"
        )

    else:

        estado["preco"] = (
            preco_resultado
            or "-"
        )

    vela = resultado.get(
        "vela"
    )

    if isinstance(
        vela,
        datetime
    ):

        estado["vela"] = (
            vela.strftime(
                "%Y-%m-%d %H:%M:%S BRT"
            )
        )

    else:

        estado["vela"] = "-"

    estado["atualizado"] = (
        agora_brt().strftime(
            "%H:%M:%S BRT"
        )
    )

    estado["atualidade_min"] = (
        f"{idade:.1f} min"
    )

    estado["mensagem"] = (
        resultado.get(
            "mensagem",
            ""
        )
    )

    estado["detalhes"] = {

        "score_call": (
            resultado.get(
                "score_call",
                "-"
            )
        ),

        "score_put": (
            resultado.get(
                "score_put",
                "-"
            )
        ),

        "rsi": (
            f"{resultado['rsi']:.2f}"
            if isinstance(
                resultado.get("rsi"),
                (float, int)
            )
            else "-"
        ),

        "ema5": (
            f"{resultado['ema5']:.5f}"
            if isinstance(
                resultado.get("ema5"),
                (float, int)
            )
            else "-"
        ),

        "ema13": (
            f"{resultado['ema13']:.5f}"
            if isinstance(
                resultado.get("ema13"),
                (float, int)
            )
            else "-"
        ),

        "ema21": (
            f"{resultado['ema21']:.5f}"
            if isinstance(
                resultado.get("ema21"),
                (float, int)
            )
            else "-"
        ),
    }

    log(
        f"{symbol} -> "
        f"{resultado['sinal']} | "
        f"score={resultado['score']} | "
        f"preço={estado['preco']} | "
        f"vela={estado['vela']} | "
        f"dado={idade:.1f} min"
    )

    return resultado

except Exception as e:

    log(
        f"ERRO em {symbol}: {e}"
    )

    estado["ativo"] = symbol
    estado["sinal"] = "AGUARDAR"
    estado["score"] = 0
    estado["preco"] = "-"
    estado["vela"] = "-"

    estado["atualizado"] = (
        agora_brt().strftime(
            "%H:%M:%S BRT"
        )
    )

    estado["atualidade_min"] = "-"

    estado["mensagem"] = (
        f"Erro: {e}"
    )

    return None
```

# ============================================================

# LEITURA DOS ATIVOS

# ============================================================

def dentro_do_horario():

```
hora = agora_brt().hour

return (
    HORA_INICIO
    <= hora
    < HORA_FIM
)
```

def executar_leitura():

```
log(
    "Iniciando leitura dos ativos."
)

if not API_KEY:

    log(
        "ERRO: TWELVE_DATA_API_KEY "
        "não configurada."
    )

    estado["sinal"] = "AGUARDAR"

    estado["mensagem"] = (
        "Configure TWELVE_DATA_API_KEY "
        "no Render."
    )

    return

if not dentro_do_horario():

    agora = agora_brt()

    log(
        f"Fora do horário configurado "
        f"({HORA_INICIO:02d}:00 às "
        f"{HORA_FIM:02d}:00 BRT)."
    )

    estado["sinal"] = "AGUARDAR"

    estado["mensagem"] = (
        "Fora do horário configurado."
    )

    estado["atualizado"] = (
        agora.strftime(
            "%H:%M:%S BRT"
        )
    )

    return

for chave, symbol in ATIVOS.items():

    processar_ativo(
        chave,
        symbol
    )

log(
    "Leitura concluída às "
    f"{agora_brt().strftime('%Y-%m-%d %H:%M:%S BRT')}."
)
```

# ============================================================

# AGENDAMENTO

# ============================================================

def esperar_ate_proxima_leitura():

```
agora = agora_brt()

minuto_atual = (
    agora.minute
)

proximo_bloco = (
    (minuto_atual // 5) + 1
) * 5

if proximo_bloco >= 60:

    proxima = (
        agora + timedelta(
            hours=1
        )
    ).replace(
        minute=0,
        second=5,
        microsecond=0
    )

else:

    proxima = agora.replace(
        minute=proximo_bloco,
        second=5,
        microsecond=0
    )

segundos = max(
    (
        proxima - agora
    ).total_seconds(),
    1
)

log(
    "Próxima leitura: "
    f"{proxima.strftime('%H:%M:%S BRT')}."
)

time.sleep(
    segundos
)
```

def loop_robo():

```
log(
    "Loop do robô iniciado."
)

log(
    "Executando primeira leitura imediatamente."
)

executar_leitura()

while True:

    try:

        esperar_ate_proxima_leitura()

        executar_leitura()

    except Exception as e:

        log(
            f"Erro no loop principal: {e}"
        )

        time.sleep(10)
```

# ============================================================

# INICIALIZAÇÃO SEGURA COM GUNICORN

# ============================================================

def garantir_robo_iniciado():

```
global _robo_started

if _robo_started:
    return

with _robo_lock:

    if _robo_started:
        return

    _robo_started = True

    thread = threading.Thread(
        target=loop_robo,
        daemon=True,
        name="robo-forex"
    )

    thread.start()

    log(
        "Thread do robô iniciada "
        "no worker web."
    )
```

@app.before_request
def iniciar_robo():

```
garantir_robo_iniciado()
```

# ============================================================

# PÁGINA WEB

# ============================================================

HTML = """

<!DOCTYPE html>

<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<meta name="viewport"
   content="width=device-width,
            initial-scale=1.0">

<title>Robô Forex 5M</title>

<style>

body {
    font-family: Arial, sans-serif;
    background: #111;
    color: #fff;
    margin: 0;
    padding: 20px;
}

.container {
    max-width: 700px;
    margin: auto;
}

h1 {
    text-align: center;
    margin-bottom: 20px;
}

.card {
    background: #1d1d1d;
    border-radius: 15px;
    padding: 20px;
    margin-bottom: 15px;
    box-shadow: 0 4px 15px rgba(0,0,0,.25);
}

.sinal {
    font-size: 42px;
    font-weight: bold;
    text-align: center;
    margin: 15px 0;
}

.linha {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    padding: 8px 0;
    border-bottom: 1px solid #333;
}

.linha:last-child {
    border-bottom: none;
}

.valor {
    font-weight: bold;
}

.observacao {
    text-align: center;
    color: #bbb;
    font-size: 14px;
    line-height: 1.5;
}

.atualizacao {
    text-align: center;
    color: #aaa;
    font-size: 13px;
    margin-top: 15px;
}

</style>

</head>

<body>

<div class="container">

<h1>🤖 Robô Forex 5M</h1>

<div class="card">

<div class="linha">
<span>Ativo</span>
<span class="valor">
{{ estado.ativo }}
</span>
</div>

<div class="sinal">
{{ estado.sinal }}
</div>

<div class="linha">
<span>Score</span>
<span class="valor">
{{ estado.score }}
</span>
</div>

<div class="linha">
<span>Preço</span>
<span class="valor">
{{ estado.preco }}
</span>
</div>

<div class="linha">
<span>Vela analisada</span>
<span class="valor">
{{ estado.vela }}
</span>
</div>

<div class="linha">
<span>Idade do dado</span>
<span class="valor">
{{ estado.atualidade_min }}
</span>
</div>

<div class="linha">
<span>Atualizado</span>
<span class="valor">
{{ estado.atualizado }}
</span>
</div>

</div>

<div class="card">

<div class="linha">
<span>Score CALL</span>
<span class="valor">
{{ estado.detalhes.score_call }}
</span>
</div>

<div class="linha">
<span>Score PUT</span>
<span class="valor">
{{ estado.detalhes.score_put }}
</span>
</div>

<div class="linha">
<span>RSI 14</span>
<span class="valor">
{{ estado.detalhes.rsi }}
</span>
</div>

<div class="linha">
<span>EMA 5</span>
<span class="valor">
{{ estado.detalhes.ema5 }}
</span>
</div>

<div class="linha">
<span>EMA 13</span>
<span class="valor">
{{ estado.detalhes.ema13 }}
</span>
</div>

<div class="linha">
<span>EMA 21</span>
<span class="valor">
{{ estado.detalhes.ema21 }}
</span>
</div>

</div>

<div class="card">

<div class="observacao">

{{ estado.mensagem }}

<br><br>

O sinal é uma análise técnica
auxiliar e não garante resultado.

Use primeiro em conta demo
e/ou backtest.

</div>

</div>

<div class="atualizacao">

A página atualiza automaticamente.

</div>

</div>

<script>

setTimeout(
    function() {
        location.reload();
    },
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

```
garantir_robo_iniciado()

return render_template_string(
    HTML,
    estado=estado
)
```

@app.route("/dados")
def dados():

```
garantir_robo_iniciado()

return jsonify(estado)
```

@app.route("/health")
def health():

```
return jsonify({
    "status": "ok",
    "bot_iniciado": _robo_started,
    "horario_brt": (
        agora_brt().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    ),
})
```

# ============================================================

# EXECUÇÃO LOCAL

# ============================================================

if **name** == "**main**":

```
garantir_robo_iniciado()

app.run(
    host="0.0.0.0",
    port=int(
        os.getenv(
            "PORT",
            "10000"
        )
    ),
    debug=False
)
```
