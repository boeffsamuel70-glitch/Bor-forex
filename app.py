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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TIMEFRAME = "5min"
TIMEFRAME_TREND = "15min"
TIMEZONE = "America/Sao_Paulo"

OUTPUTSIZE = 150
OUTPUTSIZE_15M = 100

HORA_INICIO = 6
HORA_FIM = 22

MAX_ATRASO_MINUTOS = 8

# Controle de chamadas para reduzir 429 da Twelve Data.
API_INTERVALO_MINIMO = 15.0
API_MAX_TENTATIVAS_429 = 3
API_ESPERA_429 = 20

# Cache maior para evitar novas chamadas desnecessárias.
CACHE_5M_SEGUNDOS = 240
CACHE_15M_SEGUNDOS = 900

TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"

_api_lock = threading.Lock()
_ultima_chamada_api = 0.0
_api_bloqueado_ate = 0.0

_cache_candles = {}
_cache_lock = threading.Lock()

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

def novo_estado_ativo():
    return {
        "sinal": "AGUARDANDO",
        "score": 0,
        "preco": None,
        "candle": None,
        "idade_minutos": None,
        "atualizado": None,
        "tendencia_5m": "N/D",
        "tendencia_15m": "N/D",
        "pullback": False,
        "confirmacao": False,
        "rsi": None,
        "ema5": None,
        "ema13": None,
        "ema21": None,
        "atr": None,
        "mensagem": "Aguardando análise.",
        "filtros": {},
        "pontuacoes": {},
        "erro": None,
    }


_estado = {codigo: novo_estado_ativo() for codigo in ATIVOS}
_estado_lock = threading.Lock()

_robo_lock = threading.Lock()
_robo_started = False

_ultimos_sinais_telegram = {}
_operacoes_pendentes = {}
_historico_resultados = []

# ============================================================
# UTILITÁRIOS
# ============================================================

def log(msg):
    agora = agora_brt().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{agora}] {msg}", flush=True)


def agora_brt():
    return datetime.now(ZoneInfo(TIMEZONE))


def atualizar_estado_ativo(codigo, **kwargs):
    with _estado_lock:
        if codigo not in _estado:
            _estado[codigo] = novo_estado_ativo()
        _estado[codigo].update(kwargs)


def obter_estado_para_json():
    with _estado_lock:
        dados = {}
        for codigo, estado in _estado.items():
            dados[codigo] = dict(estado)
        return dados


def parse_datetime_candle(valor):
    if not valor:
        return None

    texto = str(valor).strip()

    formatos = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]

    for formato in formatos:
        try:
            dt = datetime.strptime(texto, formato)
            return dt.replace(tzinfo=ZoneInfo(TIMEZONE))
        except ValueError:
            pass

    try:
        dt = datetime.fromisoformat(texto.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(TIMEZONE))
        return dt.astimezone(ZoneInfo(TIMEZONE))
    except ValueError:
        return None


def ordenar_candles(candles):
    return sorted(
        candles,
        key=lambda c: parse_datetime_candle(c.get("datetime"))
        or datetime.min.replace(tzinfo=ZoneInfo(TIMEZONE))
    )


def somente_velas_fechadas(candles, timeframe_minutes):
    agora = agora_brt()
    resultado = []

    for candle in candles:
        dt = parse_datetime_candle(candle.get("datetime"))
        if dt is None:
            continue

        fechamento = dt + timedelta(minutes=timeframe_minutes)

        if fechamento <= agora:
            resultado.append(candle)

    return ordenar_candles(resultado)


def idade_do_ultimo_candle(candles):
    if not candles:
        return None

    dt = parse_datetime_candle(candles[-1].get("datetime"))
    if dt is None:
        return None

    return max(0.0, (agora_brt() - dt).total_seconds() / 60.0)


def numero(valor, casas=6):
    if valor is None:
        return None
    try:
        return round(float(valor), casas)
    except (TypeError, ValueError):
        return None


# ============================================================
# CONTROLE DA API / CACHE
# ============================================================

def aguardar_intervalo_api():
    global _ultima_chamada_api

    with _api_lock:
        agora = time.monotonic()

        if agora < _api_bloqueado_ate:
            espera = _api_bloqueado_ate - agora
            if espera > 0:
                time.sleep(espera)

        agora = time.monotonic()
        espera = API_INTERVALO_MINIMO - (agora - _ultima_chamada_api)

        if espera > 0:
            time.sleep(espera)

        _ultima_chamada_api = time.monotonic()


def cache_key(symbol, interval):
    return f"{symbol}|{interval}"


def obter_cache(symbol, interval):
    chave = cache_key(symbol, interval)

    ttl = (
        CACHE_5M_SEGUNDOS
        if interval == TIMEFRAME
        else CACHE_15M_SEGUNDOS
    )

    with _cache_lock:
        item = _cache_candles.get(chave)

        if not item:
            return None

        timestamp, candles = item

        if time.monotonic() - timestamp > ttl:
            return None

        return list(candles)


def salvar_cache(symbol, interval, candles):
    chave = cache_key(symbol, interval)

    with _cache_lock:
        _cache_candles[chave] = (
            time.monotonic(),
            list(candles),
        )


def obter_candles(symbol, interval, outputsize):
    global _api_bloqueado_ate

    if not API_KEY:
        raise RuntimeError(
            "TWELVE_DATA_API_KEY não configurada no ambiente."
        )

    cache = obter_cache(symbol, interval)

    if cache is not None:
        return cache

    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "timezone": TIMEZONE,
        "apikey": API_KEY,
    }

    ultimo_erro = None

    for tentativa in range(1, API_MAX_TENTATIVAS_429 + 1):
        try:
            aguardar_intervalo_api()

            resposta = requests.get(
                TWELVE_DATA_URL,
                params=params,
                timeout=20,
            )

            if resposta.status_code == 429:
                retry_after = resposta.headers.get("Retry-After")

                try:
                    espera = float(retry_after)
                except (TypeError, ValueError):
                    espera = API_ESPERA_429 * tentativa

                espera = max(5.0, min(espera, 120.0))

                ultimo_erro = (
                    f"429 Too Many Requests "
                    f"(tentativa {tentativa}/{API_MAX_TENTATIVAS_429})"
                )

                log(
                    f"[API] {ultimo_erro} em {symbol} "
                    f"{interval}. Aguardando {espera:.0f}s."
                )

                if tentativa >= API_MAX_TENTATIVAS_429:
                    with _api_lock:
                        _api_bloqueado_ate = (
                            time.monotonic() + espera
                        )
                    break

                time.sleep(espera)
                continue

            resposta.raise_for_status()

            try:
                dados = resposta.json()
            except ValueError as exc:
                raise RuntimeError(
                    "Resposta inválida da Twelve Data."
                ) from exc

            if isinstance(dados, dict) and dados.get("status") == "error":
                mensagem = str(
                    dados.get("message")
                    or dados.get("code")
                    or "Erro desconhecido da Twelve Data."
                )

                texto_lower = mensagem.lower()

                if (
                    "rate" in texto_lower
                    or "limit" in texto_lower
                    or "credit" in texto_lower
                    or "too many" in texto_lower
                ):
                    ultimo_erro = (
                        f"Limite da Twelve Data: {mensagem}"
                    )

                    espera = API_ESPERA_429 * tentativa

                    log(
                        f"[API] {ultimo_erro} em {symbol} "
                        f"{interval}. Aguardando {espera}s."
                    )

                    if tentativa >= API_MAX_TENTATIVAS_429:
                        with _api_lock:
                            _api_bloqueado_ate = (
                                time.monotonic() + espera
                            )
                        break

                    time.sleep(espera)
                    continue

                raise RuntimeError(mensagem)

            valores = dados.get("values") if isinstance(dados, dict) else None

            if not valores:
                raise RuntimeError(
                    f"Nenhuma vela retornada para {symbol} {interval}."
                )

            candles = []

            for item in valores:
                try:
                    candles.append(
                        {
                            "datetime": item["datetime"],
                            "open": float(item["open"]),
                            "high": float(item["high"]),
                            "low": float(item["low"]),
                            "close": float(item["close"]),
                        }
                    )
                except (KeyError, TypeError, ValueError):
                    continue

            candles = ordenar_candles(candles)

            if not candles:
                raise RuntimeError(
                    f"Dados de candles inválidos para {symbol} {interval}."
                )

            salvar_cache(symbol, interval, candles)
            return candles

        except requests.RequestException as exc:
            ultimo_erro = str(exc)

            log(
                f"[API] Erro HTTP em {symbol} {interval}: "
                f"{exc}"
            )

            if tentativa < API_MAX_TENTATIVAS_429:
                time.sleep(5 * tentativa)

        except RuntimeError:
            raise

        except Exception as exc:
            ultimo_erro = str(exc)

            log(
                f"[API] Erro inesperado em {symbol} {interval}: "
                f"{exc}"
            )

            if tentativa < API_MAX_TENTATIVAS_429:
                time.sleep(5 * tentativa)

    raise RuntimeError(
        ultimo_erro
        or f"Falha ao obter dados de {symbol} {interval}."
    )


# ============================================================
# INDICADORES
# ============================================================

def closes(candles):
    return [float(c["close"]) for c in candles]


def ema(values, period):
    if len(values) < period:
        return None

    k = 2.0 / (period + 1.0)
    resultado = sum(values[:period]) / period

    for valor in values[period:]:
        resultado = (
            valor * k
            + resultado * (1.0 - k)
        )

    return resultado


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    ganhos = []
    perdas = []

    for i in range(1, period + 1):
        diferenca = values[i] - values[i - 1]

        if diferenca >= 0:
            ganhos.append(diferenca)
            perdas.append(0.0)
        else:
            ganhos.append(0.0)
            perdas.append(abs(diferenca))

    ganho_medio = sum(ganhos) / period
    perda_media = sum(perdas) / period

    if perda_media == 0:
        return 100.0

    rs = ganho_medio / perda_media
    resultado = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period + 1, len(values)):
        diferenca = values[i] - values[i - 1]

        ganho = max(diferenca, 0.0)
        perda = max(-diferenca, 0.0)

        ganho_medio = (
            (ganho_medio * (period - 1)) + ganho
        ) / period

        perda_media = (
            (perda_media * (period - 1)) + perda
        ) / period

        if perda_media == 0:
            resultado = 100.0
        else:
            rs = ganho_medio / perda_media
            resultado = 100.0 - (100.0 / (1.0 + rs))

    return resultado


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        atual = candles[i]
        anterior = candles[i - 1]

        high = float(atual["high"])
        low = float(atual["low"])
        fechamento_anterior = float(anterior["close"])

        tr = max(
            high - low,
            abs(high - fechamento_anterior),
            abs(low - fechamento_anterior),
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    resultado = sum(trs[:period]) / period

    for tr in trs[period:]:
        resultado = (
            (resultado * (period - 1)) + tr
        ) / period

    return resultado


# ============================================================
# CANDLE
# ============================================================

def informacoes_candle(candle):
    abertura = float(candle["open"])
    fechamento = float(candle["close"])
    maxima = float(candle["high"])
    minima = float(candle["low"])

    faixa = max(maxima - minima, 1e-12)
    corpo = abs(fechamento - abertura)

    pavio_superior = maxima - max(abertura, fechamento)
    pavio_inferior = min(abertura, fechamento) - minima

    return {
        "open": abertura,
        "close": fechamento,
        "high": maxima,
        "low": minima,
        "range": faixa,
        "body": corpo,
        "upper_wick": max(0.0, pavio_superior),
        "lower_wick": max(0.0, pavio_inferior),
        "body_ratio": corpo / faixa,
        "bullish": fechamento > abertura,
        "bearish": fechamento < abertura,
    }


def percentual_distancia(preco, referencia):
    if preco is None or referencia in (None, 0):
        return None

    return abs(preco - referencia) / abs(referencia)


# ============================================================
# TENDÊNCIA
# ============================================================

def tendencia_timeframe(candles):
    if len(candles) < 40:
        return "NEUTRA"

    valores = closes(candles)

    e5 = ema(valores, 5)
    e13 = ema(valores, 13)
    e21 = ema(valores, 21)

    if e5 is None or e13 is None or e21 is None:
        return "NEUTRA"

    if e5 > e13 > e21:
        return "ALTA"

    if e5 < e13 < e21:
        return "BAIXA"

    return "NEUTRA"


# ============================================================
# PULLBACK
# ============================================================

def pullback_call_na_vela(candle, ema13_valor, ema21_valor):
    info = informacoes_candle(candle)

    for referencia in (ema13_valor, ema21_valor):
        if referencia is None:
            continue

        tocou = (
            info["low"] <= referencia <= info["high"]
        )

        distancia = percentual_distancia(
            info["close"],
            referencia,
        )

        if tocou or (
            distancia is not None
            and distancia <= 0.0012
        ):
            return True

    return False


def pullback_put_na_vela(candle, ema13_valor, ema21_valor):
    info = informacoes_candle(candle)

    for referencia in (ema13_valor, ema21_valor):
        if referencia is None:
            continue

        tocou = (
            info["low"] <= referencia <= info["high"]
        )

        distancia = percentual_distancia(
            info["close"],
            referencia,
        )

        if tocou or (
            distancia is not None
            and distancia <= 0.0012
        ):
            return True

    return False


# ============================================================
# MERCADO LATERAL
# ============================================================

def mercado_lateral(candles, e5, e21, atr_valor):
    if not candles or e5 is None or e21 is None:
        return False

    preco = float(candles[-1]["close"])

    distancia_emas = (
        abs(e5 - e21) / max(abs(preco), 1e-12)
    )

    atr_ratio = (
        atr_valor / max(abs(preco), 1e-12)
        if atr_valor is not None
        else 0.0
    )

    return (
        distancia_emas < 0.00025
        or atr_ratio < 0.00008
    )


# ============================================================
# ESTRATÉGIA
# ============================================================

def analisar_pullback(candles_5m, candles_15m):
    fechadas_5m = somente_velas_fechadas(candles_5m, 5)
    fechadas_15m = somente_velas_fechadas(candles_15m, 15)

    if len(fechadas_5m) < 40:
        return {
            "sinal": "AGUARDANDO",
            "score": 0,
            "mensagem": "Poucas velas fechadas no 5m.",
        }

    if len(fechadas_15m) < 40:
        return {
            "sinal": "AGUARDANDO",
            "score": 0,
            "mensagem": "Poucas velas fechadas no 15m.",
        }

    valores_5m = closes(fechadas_5m)
    valores_15m = closes(fechadas_15m)

    ema5_5m = ema(valores_5m, 5)
    ema13_5m = ema(valores_5m, 13)
    ema21_5m = ema(valores_5m, 21)

    ema5_15m = ema(valores_15m, 5)
    ema13_15m = ema(valores_15m, 13)
    ema21_15m = ema(valores_15m, 21)

    rsi_valor = rsi(valores_5m, 14)
    atr_valor = atr(fechadas_5m, 14)

    tendencia_5m = tendencia_timeframe(fechadas_5m)
    tendencia_15m = tendencia_timeframe(fechadas_15m)

    confirmacao = fechadas_5m[-1]
    pullback_1 = fechadas_5m[-2]
    pullback_2 = fechadas_5m[-3]

    info_confirmacao = informacoes_candle(confirmacao)
    info_pullback_1 = informacoes_candle(pullback_1)
    info_pullback_2 = informacoes_candle(pullback_2)

    pullback_call = (
        pullback_call_na_vela(
            pullback_1,
            ema13_5m,
            ema21_5m,
        )
        or
        pullback_call_na_vela(
            pullback_2,
            ema13_5m,
            ema21_5m,
        )
    )

    pullback_put = (
        pullback_put_na_vela(
            pullback_1,
            ema13_5m,
            ema21_5m,
        )
        or
        pullback_put_na_vela(
            pullback_2,
            ema13_5m,
            ema21_5m,
        )
    )

    rejeicao_call = (
        info_confirmacao["bullish"]
        and
        info_confirmacao["lower_wick"]
        >= info_confirmacao["body"] * 0.50
    )

    fechamento_forte_call = (
        info_confirmacao["bullish"]
        and info_confirmacao["body_ratio"] >= 0.55
        and info_confirmacao["close"]
        >= (
            info_confirmacao["low"]
            + info_confirmacao["range"] * 0.70
        )
    )

    rejeicao_put = (
        info_confirmacao["bearish"]
        and
        info_confirmacao["upper_wick"]
        >= info_confirmacao["body"] * 0.50
    )

    fechamento_forte_put = (
        info_confirmacao["bearish"]
        and info_confirmacao["body_ratio"] >= 0.55
        and info_confirmacao["close"]
        <= (
            info_confirmacao["low"]
            + info_confirmacao["range"] * 0.30
        )
    )

    confirmacao_call = (
        (
            rejeicao_call
            or fechamento_forte_call
        )
        and
        info_confirmacao["close"]
        > info_pullback_1["high"]
    )

    confirmacao_put = (
        (
            rejeicao_put
            or fechamento_forte_put
        )
        and
        info_confirmacao["close"]
        < info_pullback_1["low"]
    )

    movimento_4 = (
        valores_5m[-1] - valores_5m[-5]
        if len(valores_5m) >= 5
        else 0.0
    )

    movimento_8 = (
        valores_5m[-1] - valores_5m[-9]
        if len(valores_5m) >= 9
        else 0.0
    )

    contexto_call = (
        movimento_4 > 0
        and movimento_8 > 0
    )

    contexto_put = (
        movimento_4 < 0
        and movimento_8 < 0
    )

    rsi_call = (
        rsi_valor is not None
        and 52 <= rsi_valor <= 68
    )

    rsi_put = (
        rsi_valor is not None
        and 32 <= rsi_valor <= 48
    )

    extremo = (
        rsi_valor is not None
        and (
            rsi_valor >= 72
            or rsi_valor <= 28
        )
    )

    preco = float(confirmacao["close"])

    atr_ratio = (
        atr_valor / max(abs(preco), 1e-12)
        if atr_valor is not None
        else 0.0
    )

    volatilidade_ok = (
        0.00008 <= atr_ratio <= 0.0035
    )

    lateral = mercado_lateral(
        fechadas_5m,
        ema5_5m,
        ema21_5m,
        atr_valor,
    )

    scores_call = {}
    scores_put = {}

    # Tendência 5m
    scores_call["tendencia_5m"] = (
        3 if tendencia_5m == "ALTA" else 0
    )

    scores_put["tendencia_5m"] = (
        3 if tendencia_5m == "BAIXA" else 0
    )

    # Tendência 15m
    scores_call["tendencia_15m"] = (
        2 if tendencia_15m == "ALTA" else 0
    )

    scores_put["tendencia_15m"] = (
        2 if tendencia_15m == "BAIXA" else 0
    )

    # Pullback
    scores_call["pullback"] = 2 if pullback_call else 0
    scores_put["pullback"] = 2 if pullback_put else 0

    # Confirmação
    scores_call["confirmacao"] = 2 if confirmacao_call else 0
    scores_put["confirmacao"] = 2 if confirmacao_put else 0

    # RSI
    scores_call["rsi"] = 1 if rsi_call else 0
    scores_put["rsi"] = 1 if rsi_put else 0

    # Contexto
    scores_call["contexto"] = 1 if contexto_call else 0
    scores_put["contexto"] = 1 if contexto_put else 0

    # Corpo
    scores_call["corpo"] = (
        1 if info_confirmacao["body_ratio"] >= 0.45
        else 0
    )

    scores_put["corpo"] = (
        1 if info_confirmacao["body_ratio"] >= 0.45
        else 0
    )

    score_call = sum(scores_call.values())
    score_put = sum(scores_put.values())

    sinal = "AGUARDANDO"
    score = max(score_call, score_put)
    scores_escolhidos = scores_call

    if (
        not extremo
        and volatilidade_ok
        and not lateral
        and tendencia_5m == "ALTA"
        and tendencia_15m == "ALTA"
        and pullback_call
        and confirmacao_call
        and rsi_call
        and contexto_call
        and score_call >= 9
    ):
        sinal = "CALL"
        score = score_call
        scores_escolhidos = scores_call

    elif (
        not extremo
        and volatilidade_ok
        and not lateral
        and tendencia_5m == "BAIXA"
        and tendencia_15m == "BAIXA"
        and pullback_put
        and confirmacao_put
        and rsi_put
        and contexto_put
        and score_put >= 9
    ):
        sinal = "PUT"
        score = score_put
        scores_escolhidos = scores_put

    if sinal == "AGUARDANDO":
        motivos = []

        if extremo:
            motivos.append("RSI extremo")

        if not volatilidade_ok:
            motivos.append("ATR fora da faixa")

        if lateral:
            motivos.append("mercado lateral")

        if tendencia_5m == "NEUTRA":
            motivos.append("tendência 5m neutra")

        if tendencia_15m == "NEUTRA":
            motivos.append("tendência 15m neutra")

        if not (pullback_call or pullback_put):
            motivos.append("sem pullback")

        if not (confirmacao_call or confirmacao_put):
            motivos.append("sem confirmação")

        if not (rsi_call or rsi_put):
            motivos.append("RSI fora da faixa")

        if not (contexto_call or contexto_put):
            motivos.append("sem contexto")

        mensagem = (
            "Sem sinal."
            if not motivos
            else "Sem sinal: " + ", ".join(motivos) + "."
        )
    else:
        mensagem = (
            f"{sinal} confirmado com score {score}."
        )

    return {
        "sinal": sinal,
        "score": int(score),
        "preco": preco,
        "candle": confirmacao.get("datetime"),
        "idade_minutos": idade_do_ultimo_candle(fechadas_5m),
        "tendencia_5m": tendencia_5m,
        "tendencia_15m": tendencia_15m,
        "pullback": (
            pullback_call
            if sinal == "CALL"
            else pullback_put
            if sinal == "PUT"
            else (pullback_call or pullback_put)
        ),
        "confirmacao": (
            confirmacao_call
            if sinal == "CALL"
            else confirmacao_put
            if sinal == "PUT"
            else (confirmacao_call or confirmacao_put)
        ),
        "rsi": numero(rsi_valor, 2),
        "ema5": numero(ema5_5m, 6),
        "ema13": numero(ema13_5m, 6),
        "ema21": numero(ema21_5m, 6),
        "atr": numero(atr_valor, 6),
        "mensagem": mensagem,
        "filtros": {
            "extremo_rsi": extremo,
            "volatilidade_ok": volatilidade_ok,
            "mercado_lateral": lateral,
            "contexto_call": contexto_call,
            "contexto_put": contexto_put,
            "rsi_call": rsi_call,
            "rsi_put": rsi_put,
        },
        "pontuacoes": scores_escolhidos,
        "erro": None,
        "atr_ratio": atr_ratio,
        "pullback_1": info_pullback_1,
        "pullback_2": info_pullback_2,
        "confirmacao_info": info_confirmacao,
    }


# ============================================================
# ESTATÍSTICAS
# ============================================================

def calcular_estatisticas():
    with _estado_lock:
        resultados = list(_historico_resultados)

    wins = sum(
        1 for item in resultados
        if item.get("resultado") == "WIN"
    )

    losses = sum(
        1 for item in resultados
        if item.get("resultado") == "LOSS"
    )

    dojis = sum(
        1 for item in resultados
        if item.get("resultado") == "DOJI"
    )

    total = wins + losses + dojis

    taxa = (
        (wins / (wins + losses) * 100.0)
        if (wins + losses) > 0
        else 0.0
    )

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "dojis": dojis,
        "taxa_acerto": round(taxa, 2),
    }


def atualizar_estatisticas_todos():
    return calcular_estatisticas()


# ============================================================
# TELEGRAM
# ============================================================

def telegram_configurado():
    return bool(
        TELEGRAM_BOT_TOKEN
        and TELEGRAM_CHAT_ID
    )


def enviar_telegram(texto):
    if not telegram_configurado():
        log("[TELEGRAM] Não configurado.")
        return False

    url = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": texto,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        resposta = requests.post(
            url,
            json=payload,
            timeout=15,
        )

        if not resposta.ok:
            log(
                "[TELEGRAM] Erro "
                f"{resposta.status_code}: "
                f"{resposta.text[:500]}"
            )
            return False

        return True

    except requests.RequestException as exc:
        log(f"[TELEGRAM] Erro ao enviar: {exc}")
        return False


def enviar_sinal_telegram(codigo, sinal):
    candle = sinal.get("candle")

    chave = (
        f"{codigo}|{candle}|{sinal.get('sinal')}"
    )

    if _ultimos_sinais_telegram.get(codigo) == chave:
        log(
            f"[TELEGRAM] Sinal duplicado ignorado: "
            f"{codigo} {sinal.get('sinal')}"
        )
        return False

    direcao = sinal.get("sinal")

    emoji = "🟢" if direcao == "CALL" else "🔴"

    preco = sinal.get("preco")
    rsi_valor = sinal.get("rsi")

    texto = (
        f"{emoji} <b>SINAL FOREX</b>\n\n"
        f"<b>Ativo:</b> {ATIVOS.get(codigo, codigo)}\n"
        f"<b>Direção:</b> {direcao}\n"
        f"<b>Score:</b> {sinal.get('score')}/12\n"
        f"<b>Preço:</b> {preco}\n"
        f"<b>Candle:</b> {candle}\n\n"
        f"<b>Tendência 5m:</b> {sinal.get('tendencia_5m')}\n"
        f"<b>Tendência 15m:</b> {sinal.get('tendencia_15m')}\n"
        f"<b>Pullback:</b> {'SIM' if sinal.get('pullback') else 'NÃO'}\n"
        f"<b>Confirmação:</b> {'SIM' if sinal.get('confirmacao') else 'NÃO'}\n"
        f"<b>RSI:</b> {rsi_valor}\n"
        f"<b>EMA 5:</b> {sinal.get('ema5')}\n"
        f"<b>EMA 13:</b> {sinal.get('ema13')}\n"
        f"<b>EMA 21:</b> {sinal.get('ema21')}\n"
        f"<b>ATR:</b> {sinal.get('atr')}\n\n"
        f"➡️ <b>Entrada:</b> próxima vela de 5 minutos\n"
        f"⏱️ <b>Expiração:</b> 5 minutos"
    )

    enviado = enviar_telegram(texto)

    if enviado:
        _ultimos_sinais_telegram[codigo] = chave

    return enviado


# ============================================================
# OPERAÇÕES
# ============================================================

def registrar_operacao(codigo, sinal):
    if sinal.get("sinal") not in ("CALL", "PUT"):
        return

    candle = sinal.get("candle")

    if not candle:
        return

    entrada_dt = parse_datetime_candle(candle)

    if entrada_dt is None:
        return

    expiracao = entrada_dt + timedelta(minutes=10)

    operacao = {
        "codigo": codigo,
        "ativo": ATIVOS.get(codigo, codigo),
        "sinal": sinal.get("sinal"),
        "preco_entrada": sinal.get("preco"),
        "candle_sinal": candle,
        "entrada": entrada_dt.isoformat(),
        "expiracao": expiracao.isoformat(),
        "score": sinal.get("score"),
        "status": "PENDENTE",
    }

    _operacoes_pendentes[codigo] = operacao

    log(
        f"[OPERAÇÃO] Registrada {codigo} "
        f"{operacao['sinal']} "
        f"para expiração {expiracao}"
    )


def avaliar_operacao(codigo, candles_5m):
    operacao = _operacoes_pendentes.get(codigo)

    if not operacao:
        return None

    agora = agora_brt()
    expiracao = parse_datetime_candle(
        operacao.get("expiracao")
    )

    if expiracao is None:
        _operacoes_pendentes.pop(codigo, None)
        return None

    if agora < expiracao:
        return None

    fechadas = somente_velas_fechadas(candles_5m, 5)

    if not fechadas:
        return None

    alvo = None

    for candle in fechadas:
        dt = parse_datetime_candle(candle.get("datetime"))

        if dt is None:
            continue

        fechamento_candle = dt + timedelta(minutes=5)

        if (
            dt <= expiracao
            and fechamento_candle >= expiracao
        ):
            alvo = candle
            break

    if alvo is None:
        # Usa a última vela fechada somente se já passou
        # da expiração e não foi possível localizar a vela exata.
        alvo = fechadas[-1]

    entrada = float(operacao["preco_entrada"])
    saida = float(alvo["close"])

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

    operacao["preco_saida"] = saida
    operacao["resultado"] = resultado
    operacao["status"] = "FINALIZADA"
    operacao["candle_saida"] = alvo.get("datetime")

    _historico_resultados.append(dict(operacao))

    # Mantém o histórico enxuto.
    if len(_historico_resultados) > 500:
        del _historico_resultados[:-500]

    _operacoes_pendentes.pop(codigo, None)

    log(
        f"[RESULTADO] {codigo} "
        f"{operacao['sinal']} = {resultado}"
    )

    enviar_resultado_telegram(operacao)

    return operacao


def enviar_resultado_telegram(operacao):
    resultado = operacao.get("resultado")

    emoji = {
        "WIN": "✅",
        "LOSS": "❌",
        "DOJI": "⚪",
    }.get(resultado, "📊")

    texto = (
        f"{emoji} <b>RESULTADO</b>\n\n"
        f"<b>Ativo:</b> {operacao.get('ativo')}\n"
        f"<b>Direção:</b> {operacao.get('sinal')}\n"
        f"<b>Entrada:</b> {operacao.get('preco_entrada')}\n"
        f"<b>Saída:</b> {operacao.get('preco_saida')}\n"
        f"<b>Resultado:</b> {resultado}\n"
        f"<b>Score:</b> {operacao.get('score')}\n\n"
        f"<b>Placar:</b> "
        f"{calcular_estatisticas()}"
    )

    enviar_telegram(texto)


# ============================================================
# PROCESSAMENTO
# ============================================================

def processar_ativo(codigo):
    simbolo = ATIVOS[codigo]

    try:
        atualizar_estado_ativo(
            codigo,
            erro=None,
            atualizado=agora_brt().isoformat(),
        )

        candles_5m = obter_candles(
            simbolo,
            TIMEFRAME,
            OUTPUTSIZE,
        )

        fechadas_5m = somente_velas_fechadas(
            candles_5m,
            5,
        )

        idade = idade_do_ultimo_candle(fechadas_5m)

        log(
            f"[BOT] {codigo}: "
            f"{len(fechadas_5m)} velas 5m, "
            f"idade={idade}"
        )

        avaliar_operacao(
            codigo,
            candles_5m,
        )

        if not fechadas_5m:
            atualizar_estado_ativo(
                codigo,
                sinal="AGUARDANDO",
                score=0,
                mensagem="Nenhuma vela 5m fechada.",
                idade_minutos=None,
                atualizado=agora_brt().isoformat(),
            )
            return

        if (
            idade is not None
            and idade > MAX_ATRASO_MINUTOS
        ):
            atualizar_estado_ativo(
                codigo,
                sinal="AGUARDANDO",
                score=0,
                preco=fechadas_5m[-1]["close"],
                candle=fechadas_5m[-1]["datetime"],
                idade_minutos=round(idade, 2),
                atualizado=agora_brt().isoformat(),
                mensagem=(
                    "Dados atrasados: "
                    f"{idade:.1f} minutos."
                ),
                erro=None,
            )
            return

        candles_15m = obter_candles(
            simbolo,
            TIMEFRAME_TREND,
            OUTPUTSIZE_15M,
        )

        fechadas_15m = somente_velas_fechadas(
            candles_15m,
            15,
        )

        if len(fechadas_15m) < 40:
            atualizar_estado_ativo(
                codigo,
                sinal="AGUARDANDO",
                score=0,
                preco=fechadas_5m[-1]["close"],
                candle=fechadas_5m[-1]["datetime"],
                idade_minutos=round(idade, 2) if idade is not None else None,
                atualizado=agora_brt().isoformat(),
                mensagem=(
                    "Aguardando 40 velas fechadas "
                    "no 15m."
                ),
                erro=None,
            )
            return

        analise = analisar_pullback(
            fechadas_5m,
            fechadas_15m,
        )

        analise["atualizado"] = agora_brt().isoformat()

        atualizar_estado_ativo(
            codigo,
            **analise,
        )

        if analise.get("sinal") in ("CALL", "PUT"):
            enviado = enviar_sinal_telegram(
                codigo,
                analise,
            )

            if enviado:
                registrar_operacao(
                    codigo,
                    analise,
                )
            else:
                # Mesmo que o Telegram não esteja configurado,
                # não registra uma operação duplicada.
                if telegram_configurado():
                    log(
                        f"[BOT] Sinal não enviado ao Telegram "
                        f"para {codigo}; operação não registrada."
                    )
                else:
                    # Em ambiente sem Telegram configurado,
                    # permite acompanhar a estratégia pela interface.
                    registrar_operacao(
                        codigo,
                        analise,
                    )

        log(
            f"[BOT] {codigo}: "
            f"{analise.get('sinal')} "
            f"score={analise.get('score')}"
        )

    except Exception as exc:
        log(
            f"[BOT] ERRO em {codigo}: {exc}"
        )

        atualizar_estado_ativo(
            codigo,
            sinal="ERRO",
            erro=str(exc),
            mensagem=f"Erro: {exc}",
            atualizado=agora_brt().isoformat(),
        )


# ============================================================
# HORÁRIO
# ============================================================

def dentro_do_horario():
    hora = agora_brt().hour
    return HORA_INICIO <= hora < HORA_FIM


def executar_leitura():
    if not dentro_do_horario():
        log(
            f"[BOT] Fora do horário operacional "
            f"({HORA_INICIO}:00-{HORA_FIM}:00)."
        )
        return

    log("[BOT] Iniciando leitura dos ativos.")

    for codigo in ATIVOS:
        processar_ativo(codigo)

    log("[BOT] Leitura dos ativos concluída.")


def esperar_ate_proxima_leitura():
    agora = agora_brt()

    minuto_atual = agora.minute

    proximo_bloco = (
        ((minuto_atual // 5) + 1) * 5
    )

    proximo = agora.replace(
        second=0,
        microsecond=0,
    )

    if proximo_bloco >= 60:
        proximo = (
            proximo.replace(
                minute=0
            )
            + timedelta(hours=1)
        )
    else:
        proximo = proximo.replace(
            minute=proximo_bloco
        )

    segundos = (
        proximo - agora
    ).total_seconds()

    if segundos < 1:
        segundos = 1

    time.sleep(segundos)


def loop_robo():
    log("[BOT] Loop do robô iniciado.")

    # Pequena espera para permitir que o servidor web suba.
    time.sleep(2)

    while True:
        try:
            executar_leitura()
            esperar_ate_proxima_leitura()

        except Exception as exc:
            log(f"[BOT] Erro no loop principal: {exc}")
            time.sleep(10)


# ============================================================
# INICIALIZAÇÃO SEGURA
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

        log("[BOT] Thread do robô iniciada.")


@app.before_request
def iniciar_robo():
    garantir_robo_iniciado()


# ============================================================
# HTML
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport"
          content="width=device-width, initial-scale=1.0">

    <title>Robô Forex</title>

    <style>
        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            padding: 20px;
            background: #0b0f14;
            color: #e8edf2;
            font-family: Arial, Helvetica, sans-serif;
        }

        .container {
            max-width: 1400px;
            margin: auto;
        }

        h1 {
            margin: 0 0 6px;
            font-size: 28px;
        }

        .sub {
            color: #8f9baa;
            margin-bottom: 20px;
        }

        .stats {
            display: grid;
            grid-template-columns:
                repeat(auto-fit, minmax(150px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }

        .stat,
        .card {
            background: #121820;
            border: 1px solid #202a35;
            border-radius: 14px;
            padding: 16px;
        }

        .stat-title {
            color: #8f9baa;
            font-size: 13px;
        }

        .stat-value {
            margin-top: 6px;
            font-size: 24px;
            font-weight: bold;
        }

        .grid {
            display: grid;
            grid-template-columns:
                repeat(auto-fit, minmax(300px, 1fr));
            gap: 15px;
        }

        .asset-title {
            font-size: 20px;
            font-weight: bold;
            margin-bottom: 8px;
        }

        .signal {
            font-size: 26px;
            font-weight: bold;
            margin: 10px 0;
        }

        .info {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            font-size: 13px;
        }

        .info div {
            background: #0d131a;
            border-radius: 8px;
            padding: 8px;
        }

        .label {
            color: #7f8b98;
            display: block;
            margin-bottom: 3px;
        }

        .message {
            margin-top: 12px;
            color: #b9c2cc;
            font-size: 13px;
            line-height: 1.4;
        }

        .error {
            margin-top: 10px;
            color: #ff8f8f;
            font-size: 12px;
            word-break: break-word;
        }

        .footer {
            margin-top: 20px;
            color: #687482;
            font-size: 12px;
            text-align: center;
        }
    </style>
</head>

<body>
<div class="container">

    <h1>Robô Forex — Pullback</h1>

    <div class="sub">
        Estratégia 5m + confirmação de tendência 15m
    </div>

    <div class="stats">
        <div class="stat">
            <div class="stat-title">Total</div>
            <div class="stat-value" id="total">0</div>
        </div>

        <div class="stat">
            <div class="stat-title">Wins</div>
            <div class="stat-value" id="wins">0</div>
        </div>

        <div class="stat">
            <div class="stat-title">Losses</div>
            <div class="stat-value" id="losses">0</div>
        </div>

        <div class="stat">
            <div class="stat-title">Doji</div>
            <div class="stat-value" id="dojis">0</div>
        </div>

        <div class="stat">
            <div class="stat-title">Acerto</div>
            <div class="stat-value" id="taxa">0%</div>
        </div>
    </div>

    <div class="grid" id="ativos"></div>

    <div class="footer">
        Atualização automática a cada 10 segundos.
    </div>
</div>

<script>
function valor(v) {
    if (v === null || v === undefined) {
        return "N/D";
    }

    return v;
}

function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text ?? "";
    return div.innerHTML;
}

function card(codigo, d) {
    const sinal = d.sinal || "AGUARDANDO";

    return `
        <div class="card">
            <div class="asset-title">
                ${escapeHtml(codigo)}
            </div>

            <div class="signal">
                ${escapeHtml(sinal)}
            </div>

            <div class="info">

                <div>
                    <span class="label">Score</span>
                    ${valor(d.score)}
                </div>

                <div>
                    <span class="label">Preço</span>
                    ${valor(d.preco)}
                </div>

                <div>
                    <span class="label">Candle</span>
                    ${escapeHtml(valor(d.candle))}
                </div>

                <div>
                    <span class="label">Idade</span>
                    ${valor(d.idade_minutos)} min
                </div>

                <div>
                    <span class="label">Tendência 5m</span>
                    ${escapeHtml(valor(d.tendencia_5m))}
                </div>

                <div>
                    <span class="label">Tendência 15m</span>
                    ${escapeHtml(valor(d.tendencia_15m))}
                </div>

                <div>
                    <span class="label">Pullback</span>
                    ${d.pullback ? "SIM" : "NÃO"}
                </div>

                <div>
                    <span class="label">Confirmação</span>
                    ${d.confirmacao ? "SIM" : "NÃO"}
                </div>

                <div>
                    <span class="label">RSI</span>
                    ${valor(d.rsi)}
                </div>

                <div>
                    <span class="label">EMA 5</span>
                    ${valor(d.ema5)}
                </div>

                <div>
                    <span class="label">EMA 13</span>
                    ${valor(d.ema13)}
                </div>

                <div>
                    <span class="label">EMA 21</span>
                    ${valor(d.ema21)}
                </div>

                <div>
                    <span class="label">ATR</span>
                    ${valor(d.atr)}
                </div>
            </div>

            <div class="message">
                ${escapeHtml(valor(d.mensagem))}
            </div>

            ${
                d.erro
                ? `<div class="error">
                    ${escapeHtml(d.erro)}
                   </div>`
                : ""
            }
        </div>
    `;
}

async function atualizar() {
    try {
        const resposta = await fetch(
            "/dados",
            { cache: "no-store" }
        );

        const json = await resposta.json();

        const stats = json.estatisticas || {};

        document.getElementById("total").textContent =
            stats.total ?? 0;

        document.getElementById("wins").textContent =
            stats.wins ?? 0;

        document.getElementById("losses").textContent =
            stats.losses ?? 0;

        document.getElementById("dojis").textContent =
            stats.dojis ?? 0;

        document.getElementById("taxa").textContent =
            (stats.taxa_acerto ?? 0) + "%";

        const container =
            document.getElementById("ativos");

        container.innerHTML = "";

        const ativos = json.ativos || {};

        for (const codigo of Object.keys(ativos)) {
            container.innerHTML +=
                card(codigo, ativos[codigo]);
        }

    } catch (erro) {
        console.error(erro);
    }
}

atualizar();
setInterval(atualizar, 10000);
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
    return render_template_string(HTML)


@app.route("/dados")
def dados():
    garantir_robo_iniciado()

    return jsonify(
        {
            "ativos": obter_estado_para_json(),
            "estatisticas": calcular_estatisticas(),
            "telegram_configurado": telegram_configurado(),
            "horario": dentro_do_horario(),
        }
    )


@app.route("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "telegram_configurado": telegram_configurado(),
            "api_configurada": bool(API_KEY),
            "ativos": list(ATIVOS.keys()),
            "horario_operacional": dentro_do_horario(),
            "estatisticas": calcular_estatisticas(),
            "api_intervalo_minimo": API_INTERVALO_MINIMO,
            "cache_5m_segundos": CACHE_5M_SEGUNDOS,
            "cache_15m_segundos": CACHE_15M_SEGUNDOS,
            "robo_iniciado": _robo_started,
        }
    )


# ============================================================
# EXECUÇÃO LOCAL
# ============================================================

if __name__ == "__main__":
    garantir_robo_iniciado()

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        debug=False,
    )
