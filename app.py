import os
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ============================================================
# CONFIGURAÇÃO
# ============================================================

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN", ""
).strip()

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID", ""
).strip()

TIMEFRAME = "5min"
TIMEFRAME_TREND = "15min"

TIMEZONE = "America/Sao_Paulo"
TZ = ZoneInfo(TIMEZONE)

OUTPUTSIZE = 150
OUTPUTSIZE_15M = 100

HORA_INICIO = 6
HORA_FIM = 22

MAX_ATRASO_MINUTOS = 8


# ============================================================
# ATIVOS
# ============================================================

ATIVOS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "GBPJPY": "GBP/JPY",
}


# ============================================================
# ESTADO INDIVIDUAL DE CADA ATIVO
# ============================================================

def criar_estado_ativo():

    return {
        "ativo": "-",
        "sinal": "AGUARDAR",
        "score": 0,
        "preco": "-",
        "vela": "-",
        "atualizado": "-",
        "atualidade_min": "-",

        "mensagem":
            "Aguardando primeira leitura.",

        "detalhes": {

            "score_call": "-",
            "score_put": "-",
            "rsi": "-",
            "ema5": "-",
            "ema13": "-",
            "ema21": "-",

            "tendencia_5m": "-",
            "tendencia_15m": "-",

            "pullback": "-",
            "confirmacao": "-",
            "lateral": "-",
            "atr": "-",
        },
    }


# Cada ativo tem seu próprio estado.
estado_ativos = {
    symbol: criar_estado_ativo()
    for symbol in ATIVOS.values()
}


# ============================================================
# ESTADO GERAL
# ============================================================

estado = {
    "ativos": estado_ativos,

    "estatisticas": {
        "total": 0,
        "wins": 0,
        "losses": 0,
        "dojis": 0,
        "taxa": 0.0,
    },
}


_robo_lock = threading.Lock()
_robo_started = False


# ============================================================
# CONTROLE DE SINAIS
# ============================================================

_ultimos_sinais_telegram = {}

_operacoes_pendentes = {}

_historico_resultados = []


# ============================================================
# UTILITÁRIOS
# ============================================================

def log(msg):

    agora = datetime.now(TZ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    print(
        f"[BOT] {msg}",
        flush=True
    )


def agora_brt():

    return datetime.now(TZ)


def parse_datetime_candle(txt):

    if not txt:
        return None

    txt = str(txt).strip()

    try:

        if txt.endswith("Z"):

            txt = (
                txt[:-1]
                +
                "+00:00"
            )

        dt = datetime.fromisoformat(txt)

        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=TZ
            )

        return dt.astimezone(TZ)

    except Exception:

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
        ):

            try:

                return datetime.strptime(
                    txt,
                    fmt
                ).replace(
                    tzinfo=TZ
                )

            except Exception:

                pass

    return None


def ordenar_candles(candles):

    resultado = []

    for candle in candles:

        item = dict(candle)

        dt = parse_datetime_candle(
            item.get("datetime")
        )

        if dt is not None:

            item["_dt"] = dt

            resultado.append(item)

    resultado.sort(
        key=lambda x: x["_dt"]
    )

    return resultado


def somente_velas_fechadas(
    candles,
    minutos
):

    candles = ordenar_candles(
        candles
    )

    agora = agora_brt()

    return [

        candle

        for candle in candles

        if (
            candle["_dt"]
            +
            timedelta(minutes=minutos)
            <=
            agora
        )
    ]


def idade_do_ultimo_candle(
    candles
):

    ordenadas = ordenar_candles(
        candles
    )

    if not ordenadas:

        return None, None

    ultimo = ordenadas[-1]

    idade = (
        agora_brt()
        -
        ultimo["_dt"]
    ).total_seconds() / 60

    return ultimo, idade


# ============================================================
# TWELVE DATA
# ============================================================

def obter_candles(
    symbol,
    interval=TIMEFRAME,
    outputsize=OUTPUTSIZE
):

    if not API_KEY:

        raise RuntimeError(
            "TWELVE_DATA_API_KEY "
            "nao configurada no Render."
        )

    resposta = requests.get(

        "https://api.twelvedata.com/time_series",

        params={

            "symbol": symbol,

            "interval": interval,

            "outputsize": outputsize,

            "timezone": TIMEZONE,

            "apikey": API_KEY,
        },

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
            f"Nenhuma vela recebida "
            f"para {symbol}: {dados}"
        )

    candles = ordenar_candles(
        values
    )

    if not candles:

        raise RuntimeError(
            f"Nao foi possivel interpretar "
            f"as datas de {symbol}."
        )

    return candles


# ============================================================
# INDICADORES
# ============================================================

def closes(candles):

    return [
        float(c["close"])
        for c in candles
    ]


def ema(
    values,
    period
):

    if len(values) < period:

        return None

    k = 2 / (
        period + 1
    )

    valor = (
        sum(values[:period])
        /
        period
    )

    for preco in values[period:]:

        valor = (
            preco * k
            +
            valor * (1 - k)
        )

    return valor


def rsi(
    values,
    period=14
):

    if len(values) < period + 1:

        return None

    ganhos = []
    perdas = []

    for i in range(
        1,
        len(values)
    ):

        diferenca = (
            values[i]
            -
            values[i - 1]
        )

        ganhos.append(
            max(
                diferenca,
                0
            )
        )

        perdas.append(
            max(
                -diferenca,
                0
            )
        )

    avg_gain = (
        sum(
            ganhos[:period]
        )
        /
        period
    )

    avg_loss = (
        sum(
            perdas[:period]
        )
        /
        period
    )

    for i in range(
        period,
        len(ganhos)
    ):

        avg_gain = (
            (
                avg_gain
                *
                (period - 1)
            )
            +
            ganhos[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                *
                (period - 1)
            )
            +
            perdas[i]
        ) / period

    if avg_loss == 0:

        return 100.0

    rs = (
        avg_gain
        /
        avg_loss
    )

    return (
        100
        -
        (
            100
            /
            (1 + rs)
        )
    )


def atr(
    candles,
    period=14
):

    if len(candles) < period + 1:

        return None

    trs = []

    for i in range(
        1,
        len(candles)
    ):

        atual = candles[i]

        anterior = candles[i - 1]

        high = float(
            atual["high"]
        )

        low = float(
            atual["low"]
        )

        close_anterior = float(
            anterior["close"]
        )

        trs.append(
            max(

                high - low,

                abs(
                    high
                    -
                    close_anterior
                ),

                abs(
                    low
                    -
                    close_anterior
                ),
            )
        )

    if len(trs) < period:

        return None

    return (
        sum(
            trs[-period:]
        )
        /
        period
    )


# ============================================================
# INFORMAÇÕES DA VELA
# ============================================================

def candle_info(candle):

    abertura = float(
        candle["open"]
    )

    fechamento = float(
        candle["close"]
    )

    maxima = float(
        candle["high"]
    )

    minima = float(
        candle["low"]
    )

    range_vela = max(
        maxima - minima,
        1e-10
    )

    corpo = abs(
        fechamento - abertura
    )

    pavio_superior = (
        maxima
        -
        max(
            abertura,
            fechamento
        )
    )

    pavio_inferior = (
        min(
            abertura,
            fechamento
        )
        -
        minima
    )

    body_ratio = (
        corpo
        /
        range_vela
    )

    return {

        "open": abertura,

        "close": fechamento,

        "high": maxima,

        "low": minima,

        "range": range_vela,

        "body": corpo,

        "upper_wick": max(
            pavio_superior,
            0
        ),

        "lower_wick": max(
            pavio_inferior,
            0
        ),

        "body_ratio": body_ratio,
    }


def percentual_distancia(
    preco,
    referencia
):

    if referencia == 0:

        return 999.0

    return (
        abs(
            preco
            -
            referencia
        )
        /
        abs(referencia)
    )


# ============================================================
# TENDÊNCIA
# ============================================================

def tendencia_timeframe(
    candles
):

    if len(candles) < 40:

        return "NEUTRA"

    valores = closes(
        candles
    )

    ema5 = ema(
        valores,
        5
    )

    ema13 = ema(
        valores,
        13
    )

    ema21 = ema(
        valores,
        21
    )

    if not (
        ema5
        and
        ema13
        and
        ema21
    ):

        return "NEUTRA"

    if (
        ema5
        >
        ema13
        >
        ema21
    ):

        return "ALTA"

    if (
        ema5
        <
        ema13
        <
        ema21
    ):

        return "BAIXA"

    return "NEUTRA"


# ============================================================
# PULLBACK
# ============================================================

def pullback_call_na_vela(
    info,
    ema13,
    ema21
):

    if not ema13 or not ema21:

        return False

    tocou_ema13 = (
        info["low"]
        <=
        ema13
        <=
        info["high"]
    )

    tocou_ema21 = (
        info["low"]
        <=
        ema21
        <=
        info["high"]
    )

    perto_ema13 = (
        percentual_distancia(
            info["low"],
            ema13
        )
        <=
        0.0012
    )

    perto_ema21 = (
        percentual_distancia(
            info["low"],
            ema21
        )
        <=
        0.0012
    )

    return (
        tocou_ema13
        or
        tocou_ema21
        or
        perto_ema13
        or
        perto_ema21
    )


def pullback_put_na_vela(
    info,
    ema13,
    ema21
):

    if not ema13 or not ema21:

        return False

    tocou_ema13 = (
        info["low"]
        <=
        ema13
        <=
        info["high"]
    )

    tocou_ema21 = (
        info["low"]
        <=
        ema21
        <=
        info["high"]
    )

    perto_ema13 = (
        percentual_distancia(
            info["high"],
            ema13
        )
        <=
        0.0012
    )

    perto_ema21 = (
        percentual_distancia(
            info["high"],
            ema21
        )
        <=
        0.0012
    )

    return (
        tocou_ema13
        or
        tocou_ema21
        or
        perto_ema13
        or
        perto_ema21
    )


# ============================================================
# MERCADO LATERAL
# ============================================================

def mercado_lateral(
    preco,
    ema5,
    ema13,
    ema21,
    atr14
):

    if not (
        preco
        and
        ema5
        and
        ema13
        and
        ema21
    ):

        return True

    distancia_5_21 = (
        abs(
            ema5
            -
            ema21
        )
        /
        preco
    )

    if distancia_5_21 < 0.00025:

        return True

    if atr14:

        atr_ratio = (
            atr14
            /
            preco
        )

        if atr_ratio < 0.00008:

            return True

    return False


# ============================================================
# ESTRATÉGIA
# ============================================================

def analisar_pullback(
    candles_5m,
    candles_15m
):

    if len(candles_5m) < 40:

        return {

            "sinal": "AGUARDAR",

            "score": 0,

            "preco": (
                float(
                    candles_5m[-1]["close"]
                )
                if candles_5m
                else 0
            ),

            "vela": (
                candles_5m[-1]["_dt"]
                if candles_5m
                else None
            ),

            "mensagem":
                "Poucas velas para análise.",

            "score_call": 0,

            "score_put": 0,
        }

    if len(candles_15m) < 40:

        return {

            "sinal": "AGUARDAR",

            "score": 0,

            "preco":
                float(
                    candles_5m[-1]["close"]
                ),

            "vela":
                candles_5m[-1]["_dt"],

            "mensagem":
                "Poucas velas de 15M.",

            "score_call": 0,

            "score_put": 0,
        }

    c = closes(
        candles_5m
    )

    preco = c[-1]

    ema5 = ema(
        c,
        5
    )

    ema13 = ema(
        c,
        13
    )

    ema21 = ema(
        c,
        21
    )

    rsi14 = rsi(
        c,
        14
    )

    atr14 = atr(
        candles_5m,
        14
    )

    tendencia_5m = (
        tendencia_timeframe(
            candles_5m
        )
    )

    tendencia_15m = (
        tendencia_timeframe(
            candles_15m
        )
    )

    confirmacao = candle_info(
        candles_5m[-1]
    )

    pullback_1 = candle_info(
        candles_5m[-2]
    )

    pullback_2 = candle_info(
        candles_5m[-3]
    )

    # ========================================================
    # PULLBACK
    # ========================================================

    pullback_call = (

        pullback_call_na_vela(
            pullback_1,
            ema13,
            ema21
        )

        or

        pullback_call_na_vela(
            pullback_2,
            ema13,
            ema21
        )
    )

    pullback_put = (

        pullback_put_na_vela(
            pullback_1,
            ema13,
            ema21
        )

        or

        pullback_put_na_vela(
            pullback_2,
            ema13,
            ema21
        )
    )

    # ========================================================
    # CONFIRMAÇÃO CALL
    # ========================================================

    confirmacao_call = False

    if (
        confirmacao["close"]
        >
        confirmacao["open"]
    ):

        rejeicao_inferior = (

            confirmacao["lower_wick"]
            >=
            confirmacao["body"] * 0.40

            and

            confirmacao["lower_wick"]
            >
            confirmacao["upper_wick"]
        )

        fechamento_forte = (

            confirmacao["body_ratio"]
            >=
            0.45

            and

            (
                (
                    confirmacao["high"]
                    -
                    confirmacao["close"]
                )
                /
                confirmacao["range"]
            )
            <=
            0.30
        )

        rompeu_pullback = (
            confirmacao["close"]
            >
            pullback_1["high"]
        )

        if (
            (
                rejeicao_inferior
                or
                fechamento_forte
            )
            and
            rompeu_pullback
        ):

            confirmacao_call = True

    # ========================================================
    # CONFIRMAÇÃO PUT
    # ========================================================

    confirmacao_put = False

    if (
        confirmacao["close"]
        <
        confirmacao["open"]
    ):

        rejeicao_superior = (

            confirmacao["upper_wick"]
            >=
            confirmacao["body"] * 0.40

            and

            confirmacao["upper_wick"]
            >
            confirmacao["lower_wick"]
        )

        fechamento_forte = (

            confirmacao["body_ratio"]
            >=
            0.45

            and

            (
                (
                    confirmacao["close"]
                    -
                    confirmacao["low"]
                )
                /
                confirmacao["range"]
            )
            <=
            0.30
        )

        rompeu_pullback = (
            confirmacao["close"]
            <
            pullback_1["low"]
        )

        if (
            (
                rejeicao_superior
                or
                fechamento_forte
            )
            and
            rompeu_pullback
        ):

            confirmacao_put = True

    # ========================================================
    # CONTEXTO
    # ========================================================

    movimento_4 = (
        c[-1]
        -
        c[-4]
    )

    movimento_8 = (
        c[-1]
        -
        c[-8]
    )

    contexto_call = (
        movimento_4 > 0
        and
        movimento_8 > 0
    )

    contexto_put = (
        movimento_4 < 0
        and
        movimento_8 < 0
    )

    # ========================================================
    # RSI
    # ========================================================

    rsi_call_ok = (
        rsi14 is not None
        and
        52 <= rsi14 <= 68
    )

    rsi_put_ok = (
        rsi14 is not None
        and
        32 <= rsi14 <= 48
    )

    rsi_extremo = (
        rsi14 is not None
        and
        (
            rsi14 >= 72
            or
            rsi14 <= 28
        )
    )

    # ========================================================
    # ATR
    # ========================================================

    atr_ok = True

    if (
        atr14 is not None
        and
        preco != 0
    ):

        atr_ratio = (
            atr14
            /
            preco
        )

        if atr_ratio < 0.00008:

            atr_ok = False

        if atr_ratio > 0.0035:

            atr_ok = False

    # ========================================================
    # LATERAL
    # ========================================================

    lateral = mercado_lateral(
        preco,
        ema5,
        ema13,
        ema21,
        atr14
    )

    # ========================================================
    # SCORE
    # ========================================================

    score_call = 0
    score_put = 0

    if tendencia_5m == "ALTA":

        score_call += 3

    if tendencia_5m == "BAIXA":

        score_put += 3

    if tendencia_15m == "ALTA":

        score_call += 2

    if tendencia_15m == "BAIXA":

        score_put += 2

    if pullback_call:

        score_call += 2

    if pullback_put:

        score_put += 2

    if confirmacao_call:

        score_call += 2

    if confirmacao_put:

        score_put += 2

    if rsi_call_ok:

        score_call += 1

    if rsi_put_ok:

        score_put += 1

    if contexto_call:

        score_call += 1

    if contexto_put:

        score_put += 1

    if confirmacao["body_ratio"] >= 0.25:

        if (
            confirmacao["close"]
            >
            confirmacao["open"]
        ):

            score_call += 1

        elif (
            confirmacao["close"]
            <
            confirmacao["open"]
        ):

            score_put += 1

    # ========================================================
    # REGRAS FINAIS
    # ========================================================

    sinal = "AGUARDAR"

    score = max(
        score_call,
        score_put
    )

    bloqueio = None

    if lateral:

        bloqueio = (
            "Mercado lateral ou "
            "tendencia fraca."
        )

    elif not atr_ok:

        bloqueio = (
            "ATR fora da faixa ideal."
        )

    elif rsi_extremo:

        bloqueio = (
            f"RSI extremo "
            f"({rsi14:.2f})."
        )

    elif tendencia_5m == "ALTA":

        if tendencia_15m != "ALTA":

            bloqueio = (
                "5M em alta, mas "
                "15M nao confirma."
            )

        elif (
            score_call >= 9
            and
            pullback_call
            and
            confirmacao_call
            and
            rsi_call_ok
            and
            contexto_call
        ):

            sinal = "CALL"

    elif tendencia_5m == "BAIXA":

        if tendencia_15m != "BAIXA":

            bloqueio = (
                "5M em baixa, mas "
                "15M nao confirma."
            )

        elif (
            score_put >= 9
            and
            pullback_put
            and
            confirmacao_put
            and
            rsi_put_ok
            and
            contexto_put
        ):

            sinal = "PUT"

    # ========================================================
    # DETALHES
    # ========================================================

    detalhes_pullback = (

        "CONFIRMADO EM VELA ANTERIOR"

        if (

            (
                pullback_call
                and
                tendencia_5m == "ALTA"
            )

            or

            (
                pullback_put
                and
                tendencia_5m == "BAIXA"
            )
        )

        else

        "NAO"
    )

    detalhes_confirmacao = (

        "CONFIRMADA"

        if (

            (
                confirmacao_call
                and
                tendencia_5m == "ALTA"
            )

            or

            (
                confirmacao_put
                and
                tendencia_5m == "BAIXA"
            )
        )

        else

        "NAO"
    )

    # ========================================================
    # MENSAGEM
    # ========================================================

    if sinal == "CALL":

        mensagem = (
            "CALL FORTE | "
            "5M ALTA + 15M ALTA | "
            "Pullback em vela anterior | "
            "Confirmacao separada | "
            f"RSI={rsi14:.2f}"
        )

    elif sinal == "PUT":

        mensagem = (
            "PUT FORTE | "
            "5M BAIXA + 15M BAIXA | "
            "Pullback em vela anterior | "
            "Confirmacao separada | "
            f"RSI={rsi14:.2f}"
        )

    elif bloqueio:

        mensagem = (
            f"AGUARDAR | {bloqueio}"
        )

    else:

        mensagem = (
            f"AGUARDAR | "
            f"5M={tendencia_5m} | "
            f"15M={tendencia_15m} | "
            f"Pullback={detalhes_pullback} | "
            f"Confirmacao={detalhes_confirmacao} | "
            f"CALL={score_call} | "
            f"PUT={score_put}"
        )

    return {

        "sinal": sinal,

        "score": score,

        "preco": preco,

        "vela":
            candles_5m[-1]["_dt"],

        "rsi": rsi14,

        "ema5": ema5,

        "ema13": ema13,

        "ema21": ema21,

        "atr": atr14,

        "score_call": score_call,

        "score_put": score_put,

        "pullback":
            detalhes_pullback,

        "rejeicao":
            detalhes_confirmacao,

        "tendencia":
            tendencia_5m,

        "tendencia_5m":
            tendencia_5m,

        "tendencia_15m":
            tendencia_15m,

        "lateral":
            "SIM" if lateral else "NAO",

        "mensagem":
            mensagem,
    }


# ============================================================
# ESTATÍSTICAS
# ============================================================

def calcular_estatisticas():

    total = len(
        _historico_resultados
    )

    wins = sum(
        1
        for x in _historico_resultados
        if x["resultado"] == "WIN"
    )

    losses = sum(
        1
        for x in _historico_resultados
        if x["resultado"] == "LOSS"
    )

    dojis = sum(
        1
        for x in _historico_resultados
        if x["resultado"] == "DOJI"
    )

    decididos = (
        wins
        +
        losses
    )

    taxa = (

        wins
        /
        decididos
        *
        100

        if decididos > 0

        else 0
    )

    return {

        "total": total,

        "wins": wins,

        "losses": losses,

        "dojis": dojis,

        "taxa": round(
            taxa,
            2
        ),
    }


# ============================================================
# TELEGRAM
# ============================================================

def telegram_configurado():

    return bool(
        TELEGRAM_BOT_TOKEN
        and
        TELEGRAM_CHAT_ID
    )


def enviar_telegram(texto):

    if not telegram_configurado():

        log(
            "Telegram nao configurado."
        )

        return False

    url = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}"
        "/sendMessage"
    )

    try:

        resposta = requests.post(

            url,

            json={

                "chat_id":
                    TELEGRAM_CHAT_ID,

                "text":
                    texto,
            },

            timeout=15,
        )

        resposta.raise_for_status()

        dados = resposta.json()

        if not dados.get("ok"):

            raise RuntimeError(
                str(dados)
            )

        log(
            "Telegram: mensagem "
            "enviada com sucesso."
        )

        return True

    except Exception as e:

        log(
            f"ERRO ao enviar Telegram: {e}"
        )

        return False


# ============================================================
# ENVIAR SINAL
# ============================================================

def enviar_sinal_telegram(
    symbol,
    resultado
):

    sinal = resultado.get(
        "sinal"
    )

    if sinal not in (
        "CALL",
        "PUT"
    ):

        return

    vela = resultado.get(
        "vela"
    )

    if not isinstance(
        vela,
        datetime
    ):

        return

    chave = (
        f"{symbol}|"
        f"{vela.isoformat()}|"
        f"{sinal}"
    )

    if (
        _ultimos_sinais_telegram.get(
            symbol
        )
        ==
        chave
    ):

        log(
            f"{symbol}: sinal duplicado "
            "ignorado."
        )

        return

    rsi_valor = resultado.get(
        "rsi"
    )

    def fmt(
        valor,
        casas=5
    ):

        if isinstance(
            valor,
            (float, int)
        ):

            return (
                f"{valor:.{casas}f}"
            )

        return "-"

    emoji = (
        "🟢"
        if sinal == "CALL"
        else
        "🔴"
    )

    texto = (

        f"{emoji} SINAL FOREX 5M\n\n"

        f"Ativo: {symbol}\n"

        f"Direcao: {sinal}\n"

        f"Score: "
        f"{resultado.get('score', 0)}\n"

        f"Preco: "
        f"{fmt(resultado.get('preco'))}\n"

        f"Vela analisada: "
        f"{vela.strftime('%Y-%m-%d %H:%M:%S BRT')}\n\n"

        f"Tendencia 5M: "
        f"{resultado.get('tendencia_5m', '-')}\n"

        f"Tendencia 15M: "
        f"{resultado.get('tendencia_15m', '-')}\n"

        f"Pullback: "
        f"{resultado.get('pullback', '-')}\n"

        f"Confirmacao: "
        f"{resultado.get('rejeicao', '-')}\n"

        f"RSI 14: "
        f"{fmt(rsi_valor, 2)}\n"

        f"EMA 5: "
        f"{fmt(resultado.get('ema5'))}\n"

        f"EMA 13: "
        f"{fmt(resultado.get('ema13'))}\n"

        f"EMA 21: "
        f"{fmt(resultado.get('ema21'))}\n"

        f"ATR 14: "
        f"{fmt(resultado.get('atr'), 6)}\n\n"

        f"➡️ ENTRADA: PROXIMA VELA\n"

        f"⏱️ EXPIRACAO: 5 MINUTOS\n\n"

        f"⚠️ Sinal tecnico experimental."
    )

    sucesso = enviar_telegram(
        texto
    )

    if sucesso:

        _ultimos_sinais_telegram[
            symbol
        ] = chave


# ============================================================
# REGISTRAR OPERAÇÃO
# ============================================================

def registrar_operacao(
    symbol,
    resultado,
    candles
):

    sinal = resultado.get(
        "sinal"
    )

    if sinal not in (
        "CALL",
        "PUT"
    ):

        return

    vela_sinal = resultado.get(
        "vela"
    )

    if not isinstance(
        vela_sinal,
        datetime
    ):

        return

    vela_entrada = (
        vela_sinal
        +
        timedelta(minutes=5)
    )

    vela_expiracao = vela_entrada

    chave = (
        f"{symbol}|"
        f"{vela_sinal.isoformat()}"
    )

    if symbol in _operacoes_pendentes:

        log(
            f"{symbol}: ja existe "
            "operacao pendente."
        )

        return

    operacao = {

        "id": chave,

        "symbol": symbol,

        "sinal": sinal,

        "score":
            resultado.get(
                "score",
                0
            ),

        "preco_sinal":
            float(
                resultado["preco"]
            ),

        "vela_sinal":
            vela_sinal,

        "vela_entrada":
            vela_entrada,

        "vela_expiracao":
            vela_expiracao,

        "entrada": None,

        "saida": None,

        "resultado":
            "PENDENTE",
    }

    _operacoes_pendentes[
        symbol
    ] = operacao

    log(
        f"{symbol}: operacao registrada "
        f"{sinal} | "
        f"vela entrada="
        f"{vela_entrada.strftime('%H:%M')}"
    )


# ============================================================
# AVALIAR WIN / LOSS
# ============================================================

def avaliar_operacao(
    symbol,
    candles
):

    operacao = (
        _operacoes_pendentes.get(
            symbol
        )
    )

    if not operacao:

        return

    agora = agora_brt()

    alvo_dt = operacao[
        "vela_expiracao"
    ]

    for candle in candles:

        dt = candle["_dt"]

        if dt != alvo_dt:

            continue

        if (
            dt
            +
            timedelta(minutes=5)
            >
            agora
        ):

            return

        info = candle_info(
            candle
        )

        entrada = info["open"]

        saida = info["close"]

        operacao[
            "entrada"
        ] = entrada

        operacao[
            "saida"
        ] = saida

        if operacao["sinal"] == "CALL":

            if saida > entrada:

                resultado = "WIN"

            elif saida < entrada:

                resultado = "LOSS"

            else:

                resultado = "DOJI"

        else:

            if saida < entrada:

                resultado = "WIN"

            elif saida > entrada:

                resultado = "LOSS"

            else:

                resultado = "DOJI"

        operacao[
            "resultado"
        ] = resultado

        operacao[
            "finalizado_em"
        ] = agora

        _historico_resultados.append(
            operacao.copy()
        )

        del _operacoes_pendentes[
            symbol
        ]

        estatisticas = (
            calcular_estatisticas()
        )

        log(
            f"{symbol}: "
            f"{operacao['sinal']} -> "
            f"{resultado} | "
            f"entrada={entrada:.5f} | "
            f"saida={saida:.5f} | "
            f"taxa="
            f"{estatisticas['taxa']:.2f}%"
        )

        enviar_resultado_telegram(
            operacao,
            estatisticas
        )

        return


# ============================================================
# TELEGRAM - RESULTADO
# ============================================================

def enviar_resultado_telegram(
    operacao,
    estatisticas
):

    resultado = operacao[
        "resultado"
    ]

    if resultado == "WIN":

        emoji = "✅"

    elif resultado == "LOSS":

        emoji = "❌"

    else:

        emoji = "➖"

    def fmt(valor):

        if isinstance(
            valor,
            (float, int)
        ):

            return f"{valor:.5f}"

        return "-"

    texto = (

        f"{emoji} RESULTADO DA OPERACAO\n\n"

        f"Ativo: "
        f"{operacao['symbol']}\n"

        f"Direcao: "
        f"{operacao['sinal']}\n"

        f"Resultado: "
        f"{resultado}\n\n"

        f"Entrada: "
        f"{fmt(operacao.get('entrada'))}\n"

        f"Saida: "
        f"{fmt(operacao.get('saida'))}\n\n"

        f"📊 ESTATISTICAS\n"

        f"Operacoes: "
        f"{estatisticas['total']}\n"

        f"Wins: "
        f"{estatisticas['wins']}\n"

        f"Losses: "
        f"{estatisticas['losses']}\n"

        f"Dojis: "
        f"{estatisticas['dojis']}\n"

        f"Taxa: "
        f"{estatisticas['taxa']:.2f}%"
    )

    enviar_telegram(
        texto
    )


# ============================================================
# ATUALIZAR ESTADO DO ATIVO
# ============================================================

def atualizar_estado_ativo(
    symbol,
    resultado,
    idade
):

    ativo = estado_ativos[
        symbol
    ]

    ativo["ativo"] = symbol

    ativo["sinal"] = (
        resultado["sinal"]
    )

    ativo["score"] = (
        resultado["score"]
    )

    preco = resultado.get(
        "preco"
    )

    ativo["preco"] = (

        f"{preco:.5f}"

        if isinstance(
            preco,
            (float, int)
        )

        else "-"
    )

    vela = resultado.get(
        "vela"
    )

    ativo["vela"] = (

        vela.strftime(
            "%Y-%m-%d %H:%M:%S BRT"
        )

        if isinstance(
            vela,
            datetime
        )

        else "-"
    )

    ativo["atualizado"] = (
        agora_brt().strftime(
            "%H:%M:%S BRT"
        )
    )

    ativo["atualidade_min"] = (
        f"{idade:.1f} min"
    )

    ativo["mensagem"] = (
        resultado.get(
            "mensagem",
            ""
        )
    )

    ativo["detalhes"] = {

        "score_call":
            resultado.get(
                "score_call",
                "-"
            ),

        "score_put":
            resultado.get(
                "score_put",
                "-"
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

        "tendencia_5m":
            resultado.get(
                "tendencia_5m",
                "-"
            ),

        "tendencia_15m":
            resultado.get(
                "tendencia_15m",
                "-"
            ),

        "pullback":
            resultado.get(
                "pullback",
                "-"
            ),

        "confirmacao":
            resultado.get(
                "rejeicao",
                "-"
            ),

        "lateral":
            resultado.get(
                "lateral",
                "-"
            ),

        "atr": (

            f"{resultado['atr']:.6f}"

            if isinstance(
                resultado.get("atr"),
                (float, int)
            )

            else "-"
        ),
    }


def atualizar_estado_erro(
    symbol,
    mensagem
):

    ativo = estado_ativos[
        symbol
    ]

    ativo["ativo"] = symbol

    ativo["sinal"] = "AGUARDAR"

    ativo["score"] = 0

    ativo["preco"] = "-"

    ativo["vela"] = "-"

    ativo["atualizado"] = (
        agora_brt().strftime(
            "%H:%M:%S BRT"
        )
    )

    ativo["atualidade_min"] = "-"

    ativo["mensagem"] = (
        mensagem
    )


# ============================================================
# PROCESSAR ATIVO
# ============================================================

def processar_ativo(
    chave,
    symbol
):

    try:

        log(
            f"Consultando 5M: {symbol}"
        )

        candles_5m = obter_candles(
            symbol,
            TIMEFRAME,
            OUTPUTSIZE
        )

        ultimo_raw, idade = (
            idade_do_ultimo_candle(
                candles_5m
            )
        )

        if ultimo_raw is None:

            raise RuntimeError(
                "Ultimo candle nao encontrado."
            )

        log(
            f"{symbol} | "
            f"ultimo 5M="
            f"{ultimo_raw['_dt'].strftime('%Y-%m-%d %H:%M:%S')} "
            f"BRT | "
            f"atraso="
            f"{idade:.2f} min"
        )

        # ====================================================
        # RESULTADO DA OPERAÇÃO ANTERIOR
        # ====================================================

        avaliar_operacao(
            symbol,
            candles_5m
        )

        # ====================================================
        # DADOS ATRASADOS
        # ====================================================

        if idade > MAX_ATRASO_MINUTOS:

            atualizar_estado_erro(
                symbol,
                f"Dado atrasado "
                f"({idade:.1f} min)."
            )

            ativo = estado_ativos[
                symbol
            ]

            ativo["preco"] = (
                f"{float(ultimo_raw['close']):.5f}"
            )

            ativo["vela"] = (
                ultimo_raw["_dt"].strftime(
                    "%Y-%m-%d %H:%M:%S BRT"
                )
            )

            ativo["atualidade_min"] = (
                f"{idade:.1f} min"
            )

            return

        # ====================================================
        # VELAS FECHADAS
        # ====================================================

        fechadas_5m = (
            somente_velas_fechadas(
                candles_5m,
                5
            )
        )

        if len(fechadas_5m) < 40:

            atualizar_estado_erro(
                symbol,
                "Poucas velas 5M."
            )

            return

        # ====================================================
        # 15M
        # ====================================================

        log(
            f"Consultando 15M: {symbol}"
        )

        candles_15m_raw = (
            obter_candles(
                symbol,
                TIMEFRAME_TREND,
                OUTPUTSIZE_15M
            )
        )

        fechadas_15m = (
            somente_velas_fechadas(
                candles_15m_raw,
                15
            )
        )

        if len(fechadas_15m) < 40:

            atualizar_estado_erro(
                symbol,
                "Poucas velas 15M."
            )

            return

        # ====================================================
        # ANALISAR
        # ====================================================

        resultado = analisar_pullback(

            fechadas_5m,

            fechadas_15m
        )

        # ====================================================
        # ATUALIZAR SOMENTE ESTE ATIVO
        # ====================================================

        atualizar_estado_ativo(
            symbol,
            resultado,
            idade
        )

        # ====================================================
        # LOG
        # ====================================================

        log(

            f"{symbol} -> "
            f"{resultado['sinal']} | "

            f"score="
            f"{resultado['score']} | "

            f"CALL="
            f"{resultado.get('score_call', 0)} | "

            f"PUT="
            f"{resultado.get('score_put', 0)} | "

            f"5M="
            f"{resultado.get('tendencia_5m', '-')} | "

            f"15M="
            f"{resultado.get('tendencia_15m', '-')} | "

            f"pullback="
            f"{resultado.get('pullback', '-')} | "

            f"confirmacao="
            f"{resultado.get('rejeicao', '-')} | "

            f"lateral="
            f"{resultado.get('lateral', '-')} | "

            f"preco="
            f"{estado_ativos[symbol]['preco']}"
        )

        # ====================================================
        # NOVO SINAL
        # ====================================================

        if resultado["sinal"] in (
            "CALL",
            "PUT"
        ):

            enviar_sinal_telegram(
                symbol,
                resultado
            )

            registrar_operacao(
                symbol,
                resultado,
                fechadas_5m
            )

        # ====================================================
        # ESTATÍSTICAS
        # ====================================================

        estado[
            "estatisticas"
        ] = calcular_estatisticas()

    except Exception as e:

        log(
            f"ERRO em {symbol}: {e}"
        )

        atualizar_estado_erro(
            symbol,
            f"Erro: {e}"
        )


# ============================================================
# HORÁRIO
# ============================================================

def dentro_do_horario():

    hora = agora_brt().hour

    return (
        HORA_INICIO
        <=
        hora
        <
        HORA_FIM
    )


# ============================================================
# LEITURA DE TODOS OS ATIVOS
# ============================================================

def executar_leitura():

    log(
        "================================"
    )

    log(
        "INICIANDO LEITURA DOS ATIVOS"
    )

    log(
        "================================"
    )

    if not API_KEY:

        log(
            "ERRO: "
            "TWELVE_DATA_API_KEY "
            "nao configurada."
        )

        for symbol in ATIVOS.values():

            atualizar_estado_erro(
                symbol,
                "Configure "
                "TWELVE_DATA_API_KEY "
                "no Render."
            )

        return

    if not dentro_do_horario():

        agora = agora_brt()

        log(
            "Fora do horario configurado."
        )

        for symbol in ATIVOS.values():

            ativo = estado_ativos[
                symbol
            ]

            ativo["sinal"] = (
                "AGUARDAR"
            )

            ativo["mensagem"] = (
                "Fora do horario configurado."
            )

            ativo["atualizado"] = (
                agora.strftime(
                    "%H:%M:%S BRT"
                )
            )

        return

    # ========================================================
    # TODOS OS ATIVOS
    # ========================================================

    for chave, symbol in ATIVOS.items():

        processar_ativo(
            chave,
            symbol
        )

    estado[
        "estatisticas"
    ] = calcular_estatisticas()

    log(
        "Leitura de todos os ativos concluida."
    )

    log(
        f"Estatisticas: "
        f"WINS="
        f"{estado['estatisticas']['wins']} | "
        f"LOSS="
        f"{estado['estatisticas']['losses']} | "
        f"DOJI="
        f"{estado['estatisticas']['dojis']} | "
        f"TAXA="
        f"{estado['estatisticas']['taxa']:.2f}%"
    )


# ============================================================
# PRÓXIMA LEITURA
# ============================================================

def esperar_ate_proxima_leitura():

    agora = agora_brt()

    proximo_bloco = (
        (agora.minute // 5)
        + 1
    ) * 5

    if proximo_bloco >= 60:

        proxima = (
            agora
            +
            timedelta(hours=1)
        ).replace(

            minute=0,

            second=5,

            microsecond=0,
        )

    else:

        proxima = agora.replace(

            minute=proximo_bloco,

            second=5,

            microsecond=0,
        )

    segundos = max(

        (
            proxima
            -
            agora
        ).total_seconds(),

        1
    )

    log(
        "Proxima leitura: "
        f"{proxima.strftime('%H:%M:%S BRT')}"
    )

    time.sleep(
        segundos
    )


# ============================================================
# LOOP
# ============================================================

def loop_robo():

    log(
        "Loop do robo iniciado."
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


# ============================================================
# INICIAR ROBÔ
# ============================================================

def garantir_robo_iniciado():

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

            name="robo-forex",
        )

        thread.start()

        log(
            "Thread do robo iniciada."
        )


@app.before_request
def iniciar_robo():

    garantir_robo_iniciado()


# ============================================================
# INTERFACE HTML
# ============================================================

HTML = """

<!DOCTYPE html>

<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width,
initial-scale=1.0">

<title>
Robo Forex Pullback PRO
</title>

<style>

* {
    box-sizing: border-box;
}

body {

    font-family: Arial, sans-serif;

    background:
        linear-gradient(
            135deg,
            #0b0b0b,
            #151515
        );

    color: white;

    margin: 0;

    padding: 20px;
}

.container {

    width: 100%;

    max-width: 1400px;

    margin: auto;
}

h1 {

    text-align: center;

    margin-bottom: 5px;

    font-size: 30px;
}

.subtitulo {

    text-align: center;

    color: #aaa;

    margin-bottom: 25px;
}


/* ==========================================================
   GRID DOS ATIVOS
   ========================================================== */

.ativos {

    display: grid;

    grid-template-columns:
        repeat(2, minmax(0, 1fr));

    gap: 18px;

    margin-bottom: 20px;
}


/* ==========================================================
   CARD DO ATIVO
   ========================================================== */

.ativo-card {

    background: #1d1d1d;

    border-radius: 16px;

    padding: 20px;

    box-shadow:
        0 5px 20px
        rgba(0,0,0,.35);

    border:
        1px solid #303030;

    transition:
        transform .2s,
        border-color .2s;
}

.ativo-card:hover {

    transform: translateY(-2px);

    border-color: #555;
}

.ativo-titulo {

    display: flex;

    justify-content:
        space-between;

    align-items: center;

    margin-bottom: 5px;
}

.ativo-nome {

    font-size: 25px;

    font-weight: bold;
}

.status {

    font-size: 12px;

    color: #888;
}


/* ==========================================================
   SINAL
   ========================================================== */

.sinal {

    font-size: 40px;

    font-weight: bold;

    text-align: center;

    padding: 12px;

    margin:
        12px 0 15px;

    border-radius: 12px;

    background: #252525;
}

.call {

    color: #00e676;

    background:
        rgba(0,230,118,.08);
}

.put {

    color: #ff5252;

    background:
        rgba(255,82,82,.08);
}

.aguardar {

    color: #ffc107;

    background:
        rgba(255,193,7,.06);
}


/* ==========================================================
   LINHAS
   ========================================================== */

.linha {

    display: flex;

    justify-content:
        space-between;

    align-items:
        center;

    gap: 12px;

    padding: 8px 0;

    border-bottom:
        1px solid #333;
}

.linha:last-child {

    border-bottom: none;
}

.valor {

    font-weight: bold;

    text-align: right;

    word-break: break-word;
}


/* ==========================================================
   FILTROS
   ========================================================== */

.filtros {

    margin-top: 18px;

    padding-top: 12px;

    border-top:
        1px solid #333;
}

.filtros h3 {

    margin-top: 0;

    color: #ddd;
}


/* ==========================================================
   MENSAGEM
   ========================================================== */

.observacao {

    text-align: center;

    color: #bbb;

    font-size: 13px;

    line-height: 1.5;

    margin-top: 15px;
}


/* ==========================================================
   ESTATÍSTICAS
   ========================================================== */

.card {

    background: #1d1d1d;

    border-radius: 15px;

    padding: 20px;

    margin-bottom: 15px;

    box-shadow:
        0 4px 15px
        rgba(0,0,0,.25);
}

.estatisticas {

    display: grid;

    grid-template-columns:
        repeat(4, 1fr);

    gap: 10px;

    margin-top: 10px;
}

.box {

    background: #292929;

    border-radius: 10px;

    padding: 15px;

    text-align: center;
}

.numero {

    font-size: 25px;

    font-weight: bold;

    margin-top: 5px;
}


/* ==========================================================
   RODAPÉ
   ========================================================== */

.atualizacao {

    text-align: center;

    color: #888;

    font-size: 13px;

    margin-top: 15px;
}


/* ==========================================================
   CELULAR
   ========================================================== */

@media (max-width: 850px) {

    .ativos {

        grid-template-columns: 1fr;
    }

    .estatisticas {

        grid-template-columns:
            repeat(2, 1fr);
    }
}

@media (max-width: 500px) {

    body {

        padding: 10px;
    }

    h1 {

        font-size: 24px;
    }

    .ativo-card {

        padding: 15px;
    }

    .ativo-nome {

        font-size: 22px;
    }

    .sinal {

        font-size: 34px;
    }

    .linha {

        font-size: 13px;
    }
}

</style>

</head>


<body>

<div class="container">


<h1>
🤖 Robo Forex Pullback PRO
</h1>

<div class="subtitulo">

5M + 15M + Pullback +
Confirmação + RSI + ATR

</div>


<!-- =======================================================
     TODOS OS ATIVOS
     ======================================================= -->

<div class="ativos">

{% for symbol, ativo in estado.ativos.items() %}


<div class="ativo-card">


<div class="ativo-titulo">

<div class="ativo-nome">

{{ symbol }}

</div>

<div class="status">

Atualizado:
{{ ativo.atualizado }}

</div>

</div>


<!-- SINAL -->

<div class="sinal
{% if ativo.sinal == 'CALL' %}
call
{% elif ativo.sinal == 'PUT' %}
put
{% else %}
aguardar
{% endif %}
">

{{ ativo.sinal }}

</div>


<!-- DADOS PRINCIPAIS -->

<div class="linha">

<span>
Score
</span>

<span class="valor">
{{ ativo.score }}
</span>

</div>


<div class="linha">

<span>
Preço
</span>

<span class="valor">
{{ ativo.preco }}
</span>

</div>


<div class="linha">

<span>
Vela analisada
</span>

<span class="valor">
{{ ativo.vela }}
</span>

</div>


<div class="linha">

<span>
Idade do dado
</span>

<span class="valor">
{{ ativo.atualidade_min }}
</span>

</div>


<!-- FILTROS -->

<div class="filtros">

<h3>
Filtros da entrada
</h3>


<div class="linha">

<span>
Tendência 5M
</span>

<span class="valor">
{{ ativo.detalhes.tendencia_5m }}
</span>

</div>


<div class="linha">

<span>
Tendência 15M
</span>

<span class="valor">
{{ ativo.detalhes.tendencia_15m }}
</span>

</div>


<div class="linha">

<span>
Pullback
</span>

<span class="valor">
{{ ativo.detalhes.pullback }}
</span>

</div>


<div class="linha">

<span>
Confirmação
</span>

<span class="valor">
{{ ativo.detalhes.confirmacao }}
</span>

</div>


<div class="linha">

<span>
Mercado lateral
</span>

<span class="valor">
{{ ativo.detalhes.lateral }}
</span>

</div>


<div class="linha">

<span>
Score CALL
</span>

<span class="valor">
{{ ativo.detalhes.score_call }}
</span>

</div>


<div class="linha">

<span>
Score PUT
</span>

<span class="valor">
{{ ativo.detalhes.score_put }}
</span>

</div>


<div class="linha">

<span>
RSI 14
</span>

<span class="valor">
{{ ativo.detalhes.rsi }}
</span>

</div>


<div class="linha">

<span>
EMA 5
</span>

<span class="valor">
{{ ativo.detalhes.ema5 }}
</span>

</div>


<div class="linha">

<span>
EMA 13
</span>

<span class="valor">
{{ ativo.detalhes.ema13 }}
</span>

</div>


<div class="linha">

<span>
EMA 21
</span>

<span class="valor">
{{ ativo.detalhes.ema21 }}
</span>

</div>


<div class="linha">

<span>
ATR 14
</span>

<span class="valor">
{{ ativo.detalhes.atr }}
</span>

</div>

</div>


<!-- MENSAGEM -->

<div class="observacao">

{{ ativo.mensagem }}

<br><br>

<strong>
Entrada:
próxima vela de 5 minutos
</strong>

<br>

<strong>
Expiração:
5 minutos
</strong>

</div>


</div>


{% endfor %}

</div>


<!-- =======================================================
     ESTATÍSTICAS GERAIS
     ======================================================= -->

<div class="card">

<h3>
📊 Estatísticas gerais
</h3>


<div class="estatisticas">


<div class="box">

Total

<div class="numero">
{{ estado.estatisticas.total }}
</div>

</div>


<div class="box">

WIN

<div class="numero">
{{ estado.estatisticas.wins }}
</div>

</div>


<div class="box">

LOSS

<div class="numero">
{{ estado.estatisticas.losses }}
</div>

</div>


<div class="box">

DOJI

<div class="numero">
{{ estado.estatisticas.dojis }}
</div>

</div>


</div>


<br>


<div class="linha">

<span>
Taxa de acerto
</span>

<span class="valor">

{{ estado.estatisticas.taxa }}%

</span>

</div>


</div>


<!-- =======================================================
     OBSERVAÇÃO
     ======================================================= -->

<div class="card">

<div class="observacao">

O robô analisa os quatro ativos
independentemente.

<br><br>

Cada ativo possui seu próprio
sinal, score, indicadores e
operação pendente.

<br><br>

Quando houver sinal:

<br>

<strong>
Entrada: próxima vela de 5 minutos
</strong>

<br>

<strong>
Expiração: 5 minutos
</strong>

<br><br>

O resultado será calculado automaticamente
quando a vela de expiração fechar.

<br><br>

<strong>
WIN = direção acertou
</strong>

<br>

<strong>
LOSS = direção errou
</strong>

<br>

<strong>
DOJI = entrada e saída iguais
</strong>

<br><br>

Use primeiro em conta demo
e valide a estratégia com quantidade
suficiente de operações.

</div>

</div>


<div class="atualizacao">

Página atualiza automaticamente
a cada 10 segundos.

</div>


</div>


<script>

setTimeout(function() {

    location.reload();

}, 10000);

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

    estado[
        "estatisticas"
    ] = calcular_estatisticas()

    return render_template_string(
        HTML,
        estado=estado
    )


@app.route("/dados")
def dados():

    garantir_robo_iniciado()

    estado[
        "estatisticas"
    ] = calcular_estatisticas()

    return jsonify(
        estado
    )


@app.route("/health")
def health():

    return jsonify({

        "status": "ok",

        "bot_iniciado":
            _robo_started,

        "horario_brt":
            agora_brt().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "estrategia":
            (
                "5M + 15M + "
                "pullback + "
                "confirmacao "
                "em vela separada"
            ),

        "ativos":
            list(
                ATIVOS.values()
            ),

        "telegram_configurado":
            telegram_configurado(),

        "operacoes_pendentes":
            len(
                _operacoes_pendentes
            ),

        "estatisticas":
            calcular_estatisticas(),
    })


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":

    garantir_robo_iniciado()

    app.run(

        host="0.0.0.0",

        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        ),

        debug=False,
    )
