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
# CONTROLE DE REQUISIÇÕES TWELVE DATA
# ============================================================
# Evita rajadas de chamadas que causam HTTP 429.
# O cache de 15M é reutilizado por vários ciclos de 5M.

TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"

# Intervalo mínimo entre chamadas à Twelve Data.
# 4 ativos x 2 timeframes já geram 8 chamadas por ciclo.
TD_MIN_INTERVAL = float(os.getenv("TWELVE_DATA_MIN_INTERVAL", "2.5"))

# Tempo de cache do 5M. O robô só precisa de dados novos a cada ciclo.
TD_CACHE_5M_SECONDS = int(
    os.getenv("TWELVE_DATA_CACHE_5M_SECONDS", "60")
)

# O 15M não precisa ser consultado a cada ciclo de 5M.
TD_CACHE_15M_SECONDS = int(
    os.getenv("TWELVE_DATA_CACHE_15M_SECONDS", "600")
)

# Após 429, aumenta o intervalo antes da próxima chamada.
TD_429_BACKOFF_SECONDS = int(
    os.getenv("TWELVE_DATA_429_BACKOFF_SECONDS", "30")
)

TD_MAX_429_BACKOFF_SECONDS = int(
    os.getenv("TWELVE_DATA_MAX_429_BACKOFF_SECONDS", "300")
)

# Quantidade de tentativas. Não são feitas em sequência:
# cada tentativa respeita o backoff.
TD_MAX_TENTATIVAS = 3

_td_lock = threading.Lock()
_td_request_session = requests.Session()
_td_ultima_requisicao = 0.0
_td_bloqueado_ate = 0.0
_td_429_backoff = TD_429_BACKOFF_SECONDS

# Cache:
# (symbol, interval) -> {"timestamp": float, "candles": list}
_td_cache = {}

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
# ESTADO
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
        "tendencia_5m": "-",
        "tendencia_15m": "-",
        "pullback": "-",
        "confirmacao": "-",
        "lateral": "-",
        "atr": "-",
    },

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
    print(f"[BOT] {msg}", flush=True)


def agora_brt():
    return datetime.now(TZ)


def parse_datetime_candle(txt):
    if not txt:
        return None

    txt = str(txt).strip()

    try:
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"

        dt = datetime.fromisoformat(txt)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)

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
                ).replace(tzinfo=TZ)
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


def somente_velas_fechadas(candles, minutos):
    candles = ordenar_candles(candles)
    agora = agora_brt()

    return [
        candle
        for candle in candles
        if candle["_dt"] + timedelta(minutes=minutos) <= agora
    ]


def idade_do_ultimo_candle(candles):
    ordenadas = ordenar_candles(candles)

    if not ordenadas:
        return None, None

    ultimo = ordenadas[-1]

    idade = (
        agora_brt() - ultimo["_dt"]
    ).total_seconds() / 60

    return ultimo, idade


# ============================================================
# TWELVE DATA - RATE LIMIT / CACHE
# ============================================================


class TwelveDataRateLimitError(RuntimeError):
    pass


def _td_cache_ttl(interval):
    if interval == TIMEFRAME_TREND:
        return TD_CACHE_15M_SECONDS
    return TD_CACHE_5M_SECONDS


def _td_obter_cache(symbol, interval):
    chave = (symbol, interval)

    item = _td_cache.get(chave)

    if not item:
        return None

    idade = time.monotonic() - item["timestamp"]

    if idade <= _td_cache_ttl(interval):
        return item["candles"]

    return None


def _td_salvar_cache(symbol, interval, candles):
    _td_cache[(symbol, interval)] = {
        "timestamp": time.monotonic(),
        "candles": candles,
    }


def _td_esperar_intervalo():
    global _td_ultima_requisicao

    agora = time.monotonic()

    espera = (
        TD_MIN_INTERVAL
        -
        (agora - _td_ultima_requisicao)
    )

    if espera > 0:
        time.sleep(espera)

    _td_ultima_requisicao = time.monotonic()


def _td_esperar_backoff():
    agora = time.monotonic()

    if agora < _td_bloqueado_ate:
        espera = _td_bloqueado_ate - agora

        log(
            f"Twelve Data em cooldown por "
            f"{espera:.1f}s."
        )

        time.sleep(espera)


def obter_candles(
    symbol,
    interval=TIMEFRAME,
    outputsize=OUTPUTSIZE
):
    global _td_bloqueado_ate
    global _td_429_backoff

    if not API_KEY:
        raise RuntimeError(
            "TWELVE_DATA_API_KEY nao configurada no Render."
        )

    # Cache evita chamadas desnecessárias.
    with _td_lock:
        cached = _td_obter_cache(
            symbol,
            interval
        )

    if cached is not None:
        log(
            f"{symbol} {interval}: usando cache."
        )
        return cached

    ultimo_erro = None

    for tentativa in range(
        1,
        TD_MAX_TENTATIVAS + 1
    ):
        try:
            with _td_lock:
                _td_esperar_backoff()
                _td_esperar_intervalo()

                resposta = _td_request_session.get(
                    TWELVE_DATA_URL,
                    params={
                        "symbol": symbol,
                        "interval": interval,
                        "outputsize": outputsize,
                        "timezone": TIMEZONE,
                        "apikey": API_KEY,
                    },
                    timeout=20,
                )

            if resposta.status_code == 429:
                ultimo_erro = (
                    f"429 Too Many Requests "
                    f"(tentativa {tentativa}/{TD_MAX_TENTATIVAS})"
                )

                # Backoff progressivo.
                with _td_lock:
                    espera = min(
                        _td_429_backoff,
                        TD_MAX_429_BACKOFF_SECONDS
                    )
                    _td_bloqueado_ate = (
                        time.monotonic() + espera
                    )
                    _td_429_backoff = min(
                        max(
                            _td_429_backoff * 2,
                            TD_429_BACKOFF_SECONDS
                        ),
                        TD_MAX_429_BACKOFF_SECONDS
                    )

                log(
                    f"{symbol} {interval}: {ultimo_erro}. "
                    f"Aguardando {espera}s antes de nova tentativa."
                )

                if tentativa < TD_MAX_TENTATIVAS:
                    time.sleep(espera)

                continue

            resposta.raise_for_status()

            dados = resposta.json()

            if dados.get("status") == "error":
                mensagem = dados.get(
                    "message",
                    "Erro retornado pela Twelve Data."
                )

                # Algumas respostas de limite podem vir em HTTP 200.
                texto_erro = str(mensagem).lower()

                if (
                    "rate limit" in texto_erro
                    or "too many" in texto_erro
                    or "credits" in texto_erro
                ):
                    ultimo_erro = (
                        f"Limite Twelve Data: {mensagem}"
                    )

                    with _td_lock:
                        espera = min(
                            _td_429_backoff,
                            TD_MAX_429_BACKOFF_SECONDS
                        )
                        _td_bloqueado_ate = (
                            time.monotonic() + espera
                        )
                        _td_429_backoff = min(
                            max(
                                _td_429_backoff * 2,
                                TD_429_BACKOFF_SECONDS
                            ),
                            TD_MAX_429_BACKOFF_SECONDS
                        )

                    log(
                        f"{symbol} {interval}: {ultimo_erro}. "
                        f"Cooldown {espera}s."
                    )

                    if tentativa < TD_MAX_TENTATIVAS:
                        time.sleep(espera)

                    continue

                raise RuntimeError(mensagem)

            values = dados.get("values")

            if not values:
                raise RuntimeError(
                    f"Nenhuma vela recebida para "
                    f"{symbol}: {dados}"
                )

            candles = ordenar_candles(values)

            if not candles:
                raise RuntimeError(
                    f"Nao foi possivel interpretar "
                    f"as datas de {symbol}."
                )

            # Requisição bem-sucedida:
            # reduz gradualmente o backoff.
            with _td_lock:
                _td_429_backoff = max(
                    TD_429_BACKOFF_SECONDS,
                    _td_429_backoff / 2
                )
                _td_bloqueado_ate = 0.0

                _td_salvar_cache(
                    symbol,
                    interval,
                    candles
                )

            return candles

        except TwelveDataRateLimitError as e:
            ultimo_erro = str(e)

        except requests.RequestException as e:
            ultimo_erro = str(e)

            # Erros transitórios também recebem uma pequena espera,
            # mas não são tratados como limite 429.
            if tentativa < TD_MAX_TENTATIVAS:
                espera = min(
                    5 * tentativa,
                    15
                )

                log(
                    f"{symbol} {interval}: erro HTTP "
                    f"(tentativa {tentativa}/"
                    f"{TD_MAX_TENTATIVAS}): {e}. "
                    f"Aguardando {espera}s."
                )

                time.sleep(espera)

        except Exception:
            raise

    raise RuntimeError(
        ultimo_erro
        or
        f"Falha ao consultar Twelve Data para {symbol}."
    )


# ============================================================
# INDICADORES
# ============================================================


def closes(candles):
    return [
        float(c["close"])
        for c in candles
    ]


def ema(values, period):
    if len(values) < period:
        return None

    k = 2 / (period + 1)

    valor = (
        sum(values[:period])
        / period
    )

    for preco in values[period:]:
        valor = (
            preco * k
            +
            valor * (1 - k)
        )

    return valor


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

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

    for i in range(period, len(ganhos)):
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


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        atual = candles[i]
        anterior = candles[i - 1]

        high = float(atual["high"])
        low = float(atual["low"])
        close_anterior = float(
            anterior["close"]
        )

        trs.append(
            max(
                high - low,
                abs(high - close_anterior),
                abs(low - close_anterior),
            )
        )

    if len(trs) < period:
        return None

    return (
        sum(trs[-period:])
        /
        period
    )


# ============================================================
# INFORMAÇÕES DA VELA
# ============================================================


def candle_info(candle):
    abertura = float(candle["open"])
    fechamento = float(candle["close"])
    maxima = float(candle["high"])
    minima = float(candle["low"])

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
        max(abertura, fechamento)
    )

    pavio_inferior = (
        min(abertura, fechamento)
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


def percentual_distancia(preco, referencia):
    if referencia == 0:
        return 999.0

    return (
        abs(preco - referencia)
        /
        abs(referencia)
    )


# ============================================================
# TENDÊNCIA 15 MIN
# ============================================================


def tendencia_timeframe(candles):
    if len(candles) < 40:
        return "NEUTRA"

    valores = closes(candles)

    ema5 = ema(valores, 5)
    ema13 = ema(valores, 13)
    ema21 = ema(valores, 21)

    if not (ema5 and ema13 and ema21):
        return "NEUTRA"

    if ema5 > ema13 > ema21:
        return "ALTA"

    if ema5 < ema13 < ema21:
        return "BAIXA"

    return "NEUTRA"


# ============================================================
# PULLBACK
# ============================================================


def pullback_call_na_vela(info, ema13, ema21):
    if not ema13 or not ema21:
        return False

    tocou_ema13 = (
        info["low"] <= ema13 <= info["high"]
    )

    tocou_ema21 = (
        info["low"] <= ema21 <= info["high"]
    )

    perto_ema13 = (
        percentual_distancia(
            info["low"],
            ema13
        ) <= 0.0012
    )

    perto_ema21 = (
        percentual_distancia(
            info["low"],
            ema21
        ) <= 0.0012
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


def pullback_put_na_vela(info, ema13, ema21):
    if not ema13 or not ema21:
        return False

    tocou_ema13 = (
        info["low"] <= ema13 <= info["high"]
    )

    tocou_ema21 = (
        info["low"] <= ema21 <= info["high"]
    )

    perto_ema13 = (
        percentual_distancia(
            info["high"],
            ema13
        ) <= 0.0012
    )

    perto_ema21 = (
        percentual_distancia(
            info["high"],
            ema21
        ) <= 0.0012
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
# ANÁLISE DE MERCADO LATERAL
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
        abs(ema5 - ema21)
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


def analisar_pullback(candles_5m, candles_15m):
    if len(candles_5m) < 40:
        return {
            "sinal": "AGUARDAR",
            "score": 0,
            "preco": (
                float(candles_5m[-1]["close"])
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
                float(candles_5m[-1]["close"]),
            "vela":
                candles_5m[-1]["_dt"],
            "mensagem":
                "Poucas velas de 15M.",
            "score_call": 0,
            "score_put": 0,
        }

    c = closes(candles_5m)
    preco = c[-1]

    ema5 = ema(c, 5)
    ema13 = ema(c, 13)
    ema21 = ema(c, 21)

    rsi14 = rsi(c, 14)
    atr14 = atr(candles_5m, 14)

    tendencia_5m = tendencia_timeframe(candles_5m)
    tendencia_15m = tendencia_timeframe(candles_15m)

    confirmacao = candle_info(candles_5m[-1])
    pullback_1 = candle_info(candles_5m[-2])
    pullback_2 = candle_info(candles_5m[-3])

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

    confirmacao_call = False

    if confirmacao["close"] > confirmacao["open"]:
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
            confirmacao["body_ratio"] >= 0.45
            and
            (
                (
                    confirmacao["high"]
                    -
                    confirmacao["close"]
                )
                /
                confirmacao["range"]
            ) <= 0.30
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

    confirmacao_put = False

    if confirmacao["close"] < confirmacao["open"]:
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
            confirmacao["body_ratio"] >= 0.45
            and
            (
                (
                    confirmacao["close"]
                    -
                    confirmacao["low"]
                )
                /
                confirmacao["range"]
            ) <= 0.30
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

    movimento_4 = c[-1] - c[-4]
    movimento_8 = c[-1] - c[-8]

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

    atr_ok = True

    if atr14 is not None and preco != 0:
        atr_ratio = atr14 / preco

        if atr_ratio < 0.00008:
            atr_ok = False

        if atr_ratio > 0.0035:
            atr_ok = False

    lateral = mercado_lateral(
        preco,
        ema5,
        ema13,
        ema21,
        atr14
    )

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
        if confirmacao["close"] > confirmacao["open"]:
            score_call += 1
        elif confirmacao["close"] < confirmacao["open"]:
            score_put += 1

    sinal = "AGUARDAR"

    score = max(
        score_call,
        score_put
    )

    bloqueio = None

    if lateral:
        bloqueio = (
            "Mercado lateral ou tendencia fraca."
        )

    elif not atr_ok:
        bloqueio = (
            "ATR fora da faixa ideal."
        )

    elif rsi_extremo:
        bloqueio = (
            f"RSI extremo ({rsi14:.2f})."
        )

    elif tendencia_5m == "ALTA":
        if tendencia_15m != "ALTA":
            bloqueio = (
                "5M em alta, mas 15M nao confirma."
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
                "5M em baixa, mas 15M nao confirma."
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
        mensagem = f"AGUARDAR | {bloqueio}"

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
        "vela": candles_5m[-1]["_dt"],
        "rsi": rsi14,
        "ema5": ema5,
        "ema13": ema13,
        "ema21": ema21,
        "atr": atr14,
        "score_call": score_call,
        "score_put": score_put,
        "pullback": detalhes_pullback,
        "rejeicao": detalhes_confirmacao,
        "tendencia": tendencia_5m,
        "tendencia_5m": tendencia_5m,
        "tendencia_15m": tendencia_15m,
        "lateral": "SIM" if lateral else "NAO",
        "mensagem": mensagem,
    }


# ============================================================
# ESTATÍSTICAS
# ============================================================


def calcular_estatisticas():
    total = len(_historico_resultados)

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

    decididos = wins + losses

    taxa = (
        wins / decididos * 100
        if decididos > 0
        else 0
    )

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "dojis": dojis,
        "taxa": round(taxa, 2),
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
        log("Telegram nao configurado.")
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
                "chat_id": TELEGRAM_CHAT_ID,
                "text": texto,
            },
            timeout=15,
        )

        resposta.raise_for_status()

        dados = resposta.json()

        if not dados.get("ok"):
            raise RuntimeError(str(dados))

        log(
            "Telegram: mensagem enviada com sucesso."
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


def enviar_sinal_telegram(symbol, resultado):
    sinal = resultado.get("sinal")

    if sinal not in ("CALL", "PUT"):
        return

    vela = resultado.get("vela")

    if not isinstance(vela, datetime):
        return

    chave = (
        f"{symbol}|"
        f"{vela.isoformat()}|"
        f"{sinal}"
    )

    if (
        _ultimos_sinais_telegram.get(symbol)
        ==
        chave
    ):
        log(
            f"{symbol}: sinal duplicado ignorado."
        )
        return

    rsi_valor = resultado.get("rsi")

    def fmt(valor, casas=5):
        if isinstance(valor, (float, int)):
            return f"{valor:.{casas}f}"
        return "-"

    emoji = "🟢" if sinal == "CALL" else "🔴"

    texto = (
        f"{emoji} SINAL FOREX 5M\n\n"
        f"Ativo: {symbol}\n"
        f"Direcao: {sinal}\n"
        f"Score: {resultado.get('score', 0)}\n"
        f"Preco: {fmt(resultado.get('preco'))}\n"
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
        f"RSI 14: {fmt(rsi_valor, 2)}\n"
        f"EMA 5: {fmt(resultado.get('ema5'))}\n"
        f"EMA 13: {fmt(resultado.get('ema13'))}\n"
        f"EMA 21: {fmt(resultado.get('ema21'))}\n"
        f"ATR 14: {fmt(resultado.get('atr'), 6)}\n\n"
        f"➡️ ENTRADA: PROXIMA VELA\n"
        f"⏱️ EXPIRACAO: 5 MINUTOS\n\n"
        f"⚠️ Sinal tecnico experimental."
    )

    sucesso = enviar_telegram(texto)

    if sucesso:
        _ultimos_sinais_telegram[symbol] = chave


# ============================================================
# REGISTRAR OPERAÇÃO
# ============================================================


def registrar_operacao(symbol, resultado, candles):
    sinal = resultado.get("sinal")

    if sinal not in ("CALL", "PUT"):
        return

    vela_sinal = resultado.get("vela")

    if not isinstance(vela_sinal, datetime):
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
            f"{symbol}: ja existe operacao pendente."
        )
        return

    operacao = {
        "id": chave,
        "symbol": symbol,
        "sinal": sinal,
        "score": resultado.get("score", 0),
        "preco_sinal": float(resultado["preco"]),
        "vela_sinal": vela_sinal,
        "vela_entrada": vela_entrada,
        "vela_expiracao": vela_expiracao,
        "entrada": None,
        "saida": None,
        "resultado": "PENDENTE",
    }

    _operacoes_pendentes[symbol] = operacao

    log(
        f"{symbol}: operacao registrada "
        f"{sinal} | "
        f"vela entrada="
        f"{vela_entrada.strftime('%H:%M')}"
    )


# ============================================================
# AVALIAR WIN / LOSS
# ============================================================


def avaliar_operacao(symbol, candles):
    operacao = _operacoes_pendentes.get(symbol)

    if not operacao:
        return

    agora = agora_brt()
    alvo_dt = operacao["vela_expiracao"]

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

        info = candle_info(candle)

        entrada = info["open"]
        saida = info["close"]

        operacao["entrada"] = entrada
        operacao["saida"] = saida

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

        operacao["resultado"] = resultado
        operacao["finalizado_em"] = agora

        _historico_resultados.append(
            operacao.copy()
        )

        del _operacoes_pendentes[symbol]

        estatisticas = calcular_estatisticas()

        log(
            f"{symbol}: "
            f"{operacao['sinal']} -> "
            f"{resultado} | "
            f"entrada={entrada:.5f} | "
            f"saida={saida:.5f} | "
            f"taxa={estatisticas['taxa']:.2f}%"
        )

        enviar_resultado_telegram(
            operacao,
            estatisticas
        )

        return


# ============================================================
# TELEGRAM - RESULTADO
# ============================================================


def enviar_resultado_telegram(operacao, estatisticas):
    resultado = operacao["resultado"]

    if resultado == "WIN":
        emoji = "✅"
    elif resultado == "LOSS":
        emoji = "❌"
    else:
        emoji = "➖"

    def fmt(valor):
        if isinstance(valor, (float, int)):
            return f"{valor:.5f}"
        return "-"

    texto = (
        f"{emoji} RESULTADO DA OPERACAO\n\n"
        f"Ativo: {operacao['symbol']}\n"
        f"Direcao: {operacao['sinal']}\n"
        f"Resultado: {resultado}\n\n"
        f"Entrada: {fmt(operacao.get('entrada'))}\n"
        f"Saida: {fmt(operacao.get('saida'))}\n\n"
        f"📊 ESTATISTICAS\n"
        f"Operacoes: {estatisticas['total']}\n"
        f"Wins: {estatisticas['wins']}\n"
        f"Losses: {estatisticas['losses']}\n"
        f"Dojis: {estatisticas['dojis']}\n"
        f"Taxa: {estatisticas['taxa']:.2f}%"
    )

    enviar_telegram(texto)


# ============================================================
# PROCESSAR ATIVO
# ============================================================


def processar_ativo(chave, symbol):
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
            f"atraso={idade:.2f} min"
        )

        avaliar_operacao(
            symbol,
            candles_5m
        )

        if idade > MAX_ATRASO_MINUTOS:
            estado["ativo"] = symbol
            estado["sinal"] = "AGUARDAR"
            estado["score"] = 0
            estado["preco"] = (
                f"{float(ultimo_raw['close']):.5f}"
            )
            estado["vela"] = (
                ultimo_raw["_dt"].strftime(
                    "%Y-%m-%d %H:%M:%S BRT"
                )
            )
            estado["atualidade_min"] = (
                f"{idade:.1f} min"
            )
            estado["atualizado"] = (
                agora_brt().strftime(
                    "%H:%M:%S BRT"
                )
            )
            estado["mensagem"] = (
                f"Dado atrasado ({idade:.1f} min)."
            )
            return

        fechadas_5m = somente_velas_fechadas(
            candles_5m,
            5
        )

        if len(fechadas_5m) < 40:
            log(
                f"{symbol}: poucas velas 5M."
            )
            return

        log(
            f"Consultando 15M: {symbol}"
        )

        candles_15m_raw = obter_candles(
            symbol,
            TIMEFRAME_TREND,
            OUTPUTSIZE_15M
        )

        fechadas_15m = somente_velas_fechadas(
            candles_15m_raw,
            15
        )

        if len(fechadas_15m) < 40:
            log(
                f"{symbol}: poucas velas 15M."
            )
            return

        resultado = analisar_pullback(
            fechadas_5m,
            fechadas_15m
        )

        estado["ativo"] = symbol
        estado["sinal"] = resultado["sinal"]
        estado["score"] = resultado["score"]

        preco = resultado.get("preco")

        estado["preco"] = (
            f"{preco:.5f}"
            if isinstance(preco, (float, int))
            else "-"
        )

        vela = resultado.get("vela")

        estado["vela"] = (
            vela.strftime(
                "%Y-%m-%d %H:%M:%S BRT"
            )
            if isinstance(vela, datetime)
            else "-"
        )

        estado["atualizado"] = (
            agora_brt().strftime(
                "%H:%M:%S BRT"
            )
        )

        estado["atualidade_min"] = (
            f"{idade:.1f} min"
        )

        estado["mensagem"] = resultado.get(
            "mensagem",
            ""
        )

        estado["detalhes"] = {
            "score_call":
                resultado.get("score_call", "-"),

            "score_put":
                resultado.get("score_put", "-"),

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

        log(
            f"{symbol} -> "
            f"{resultado['sinal']} | "
            f"score={resultado['score']} | "
            f"CALL={resultado.get('score_call', 0)} | "
            f"PUT={resultado.get('score_put', 0)} | "
            f"5M={resultado.get('tendencia_5m', '-')} | "
            f"15M={resultado.get('tendencia_15m', '-')} | "
            f"pullback={resultado.get('pullback', '-')} | "
            f"confirmacao={resultado.get('rejeicao', '-')} | "
            f"lateral={resultado.get('lateral', '-')} | "
            f"preco={estado['preco']}"
        )

        if resultado["sinal"] in ("CALL", "PUT"):
            enviar_sinal_telegram(
                symbol,
                resultado
            )

            registrar_operacao(
                symbol,
                resultado,
                fechadas_5m
            )

        estado["estatisticas"] = calcular_estatisticas()

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
        estado["mensagem"] = f"Erro: {e}"


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
# LEITURA
# ============================================================


def executar_leitura():
    log("================================")
    log("INICIANDO LEITURA")
    log("================================")

    if not API_KEY:
        log(
            "ERRO: TWELVE_DATA_API_KEY nao configurada."
        )

        estado["sinal"] = "AGUARDAR"

        estado["mensagem"] = (
            "Configure TWELVE_DATA_API_KEY no Render."
        )

        return

    if not dentro_do_horario():
        agora = agora_brt()

        log(
            "Fora do horario configurado."
        )

        estado["sinal"] = "AGUARDAR"

        estado["mensagem"] = (
            "Fora do horario configurado."
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

    estado["estatisticas"] = calcular_estatisticas()

    log("Leitura concluida.")

    log(
        f"Estatisticas: "
        f"WINS={estado['estatisticas']['wins']} | "
        f"LOSS={estado['estatisticas']['losses']} | "
        f"DOJI={estado['estatisticas']['dojis']} | "
        f"TAXA={estado['estatisticas']['taxa']:.2f}%"
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

    time.sleep(segundos)


# ============================================================
# LOOP
# ============================================================


def loop_robo():
    log("Loop do robo iniciado.")

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
content="width=device-width, initial-scale=1.0">
<title>Robo Forex Pullback PRO</title>
<style>
body {
    font-family: Arial, sans-serif;
    background: #111;
    color: white;
    margin: 0;
    padding: 20px;
}
.container {
    max-width: 750px;
    margin: auto;
}
h1 {
    text-align: center;
    margin-bottom: 5px;
}
.subtitulo {
    text-align: center;
    color: #aaa;
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
    text-align: right;
}
.estatisticas {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
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
.observacao {
    text-align: center;
    color: #bbb;
    font-size: 14px;
    line-height: 1.5;
}
.atualizacao {
    text-align: center;
    color: #888;
    font-size: 13px;
    margin-top: 15px;
}
</style>
</head>
<body>
<div class="container">
<h1>Robo Forex Pullback PRO</h1>
<div class="subtitulo">
5M + 15M + Pullback + Confirmação + RSI + ATR
</div>

<div class="card">
<div class="linha">
<span>Ativo</span>
<span class="valor">{{ estado.ativo }}</span>
</div>
<div class="sinal">{{ estado.sinal }}</div>
<div class="linha">
<span>Score</span>
<span class="valor">{{ estado.score }}</span>
</div>
<div class="linha">
<span>Preço</span>
<span class="valor">{{ estado.preco }}</span>
</div>
<div class="linha">
<span>Vela analisada</span>
<span class="valor">{{ estado.vela }}</span>
</div>
<div class="linha">
<span>Idade do dado</span>
<span class="valor">{{ estado.atualidade_min }}</span>
</div>
<div class="linha">
<span>Atualizado</span>
<span class="valor">{{ estado.atualizado }}</span>
</div>
</div>

<div class="card">
<h3>Filtros da entrada</h3>
<div class="linha">
<span>Tendência 5M</span>
<span class="valor">{{ estado.detalhes.tendencia_5m }}</span>
</div>
<div class="linha">
<span>Tendência 15M</span>
<span class="valor">{{ estado.detalhes.tendencia_15m }}</span>
</div>
<div class="linha">
<span>Pullback</span>
<span class="valor">{{ estado.detalhes.pullback }}</span>
</div>
<div class="linha">
<span>Confirmação</span>
<span class="valor">{{ estado.detalhes.confirmacao }}</span>
</div>
<div class="linha">
<span>Mercado lateral</span>
<span class="valor">{{ estado.detalhes.lateral }}</span>
</div>
<div class="linha">
<span>Score CALL</span>
<span class="valor">{{ estado.detalhes.score_call }}</span>
</div>
<div class="linha">
<span>Score PUT</span>
<span class="valor">{{ estado.detalhes.score_put }}</span>
</div>
<div class="linha">
<span>RSI 14</span>
<span class="valor">{{ estado.detalhes.rsi }}</span>
</div>
<div class="linha">
<span>EMA 5</span>
<span class="valor">{{ estado.detalhes.ema5 }}</span>
</div>
<div class="linha">
<span>EMA 13</span>
<span class="valor">{{ estado.detalhes.ema13 }}</span>
</div>
<div class="linha">
<span>EMA 21</span>
<span class="valor">{{ estado.detalhes.ema21 }}</span>
</div>
<div class="linha">
<span>ATR 14</span>
<span class="valor">{{ estado.detalhes.atr }}</span>
</div>
</div>

<div class="card">
<h3>Estatísticas</h3>
<div class="estatisticas">
<div class="box">
Total
<div class="numero">{{ estado.estatisticas.total }}</div>
</div>
<div class="box">
WIN
<div class="numero">{{ estado.estatisticas.wins }}</div>
</div>
<div class="box">
LOSS
<div class="numero">{{ estado.estatisticas.losses }}</div>
</div>
<div class="box">
DOJI
<div class="numero">{{ estado.estatisticas.dojis }}</div>
</div>
</div>
<br>
<div class="linha">
<span>Taxa de acerto</span>
<span class="valor">{{ estado.estatisticas.taxa }}%</span>
</div>
</div>

<div class="card">
<div class="observacao">
{{ estado.mensagem }}
<br><br>
Quando houver sinal:
<br>
<strong>Entrada: próxima vela de 5 minutos</strong>
<br>
<strong>Expiração: 5 minutos</strong>
<br><br>
O resultado será calculado automaticamente
quando a vela de expiração fechar.
<br><br>
<strong>WIN = direção acertou</strong>
<br>
<strong>LOSS = direção errou</strong>
<br>
<strong>DOJI = entrada e saída iguais</strong>
<br><br>
Use primeiro em conta demo e valide a estratégia
com quantidade suficiente de operações.
</div>
</div>

<div class="atualizacao">
Página atualiza automaticamente a cada 10 segundos.
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

    estado["estatisticas"] = calcular_estatisticas()

    return render_template_string(
        HTML,
        estado=estado
    )


@app.route("/dados")
def dados():
    garantir_robo_iniciado()

    estado["estatisticas"] = calcular_estatisticas()

    return jsonify(estado)


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "bot_iniciado": _robo_started,
        "horario_brt":
            agora_brt().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        "estrategia": (
            "5M + 15M + "
            "pullback + "
            "confirmacao "
            "em vela separada"
        ),
        "telegram_configurado":
            telegram_configurado(),
        "operacoes_pendentes":
            len(_operacoes_pendentes),
        "cache_tw_data": {
            "itens": len(_td_cache),
            "intervalo_minimo_segundos":
                TD_MIN_INTERVAL,
            "cache_5m_segundos":
                TD_CACHE_5M_SECONDS,
            "cache_15m_segundos":
                TD_CACHE_15M_SECONDS,
            "backoff_429_atual":
                _td_429_backoff,
        },
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
