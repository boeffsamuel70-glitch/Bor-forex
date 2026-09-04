import os
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import json
import requests
import websocket

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ============================================================
# CONFIGURAÇÃO
# ============================================================

# ============================================================
# BULLEX WEBSOCKET - LEITURA DIRETA DO FEED
# ============================================================
# IMPORTANTE:
# Este arquivo SOMENTE lê candles. Não envia ordens para a Bullex.
#
# A autenticação do WebSocket usa a sessão da Traderoom. Para não
# colocar segredo dentro do código, informe a sessão por variável
# de ambiente.
#
# BULLEX_SSID:
#   valor da sessão "ssid" obtido na autenticação da Traderoom.
#
# Se a sua captura usar outro corpo de autenticação, você pode
# informar o JSON completo em BULLEX_AUTH_BODY_JSON.
#
# Exemplo:
# BULLEX_AUTH_BODY_JSON={"ssid":"SEU_VALOR_DE_SESSAO"}
#
# Nunca coloque esses valores no GitHub.
BULLEX_WS_URL = os.getenv(
    "BULLEX_WS_URL",
    "wss://ws.trade.bull-ex.com/echo/websocket"
).strip()

BULLEX_ORIGIN = os.getenv(
    "BULLEX_ORIGIN",
    "https://trade.bull-ex.com"
).strip()

BULLEX_SSID = os.getenv("BULLEX_SSID", "").strip()
BULLEX_AUTH_BODY_JSON = os.getenv(
    "BULLEX_AUTH_BODY_JSON", ""
).strip()

# Mapa confirmado na Traderoom para os ativos OTC.
# 5M = 300 segundos; 15M = 900 segundos.
ATIVO_BULLEX = {
    "EURUSD": {"symbol": "EUR/USD", "active_id": 76, "ticker": "EURUSD-OTC"},
    "GBPUSD": {"symbol": "GBP/USD", "active_id": 81, "ticker": "GBPUSD-OTC"},
    "USDJPY": {"symbol": "USD/JPY", "active_id": 85, "ticker": "USDJPY-OTC"},
    "GBPJPY": {"symbol": "GBP/JPY", "active_id": 84, "ticker": "GBPJPY-OTC"},
}

_BULLEX_CANDLE_SIZES = {"5min": 300, "15min": 900}
_bullex_ws = None
_bullex_ws_lock = threading.RLock()
_bullex_request_lock = threading.Lock()
_bullex_request_counter = 1000
_bullex_connected = False
_bullex_last_error = None
_bullex_candles = {}
_bullex_cv = threading.Condition(_bullex_ws_lock)

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

# Evita mandar duas vezes o mesmo sinal.
_ultimos_sinais_telegram = {}

# Guarda operações aguardando resultado.
#
# Exemplo:
# {
#   "EUR/USD": {
#       "id": "...",
#       "sinal": "CALL",
#       "entrada": 1.16000,
#       "vela_sinal": datetime,
#       "vela_expiracao": datetime
#   }
# }
#
# Pode existir no máximo uma operação pendente por ativo.
_operacoes_pendentes = {}

# Impede registrar novamente a mesma vela de sinal depois que ela foi finalizada.
_ultimas_operacoes_registradas = {}

# Resultados da sessão.
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
            txt = txt[:-1] + "+00:00"

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
        if candle["_dt"]
        + timedelta(minutes=minutos)
        <= agora
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
# BULLEX - FONTE DE DADOS (WEBSOCKET DIRETO)
# ============================================================

def _next_request_id():
    global _bullex_request_counter
    with _bullex_request_lock:
        _bullex_request_counter += 1
        return str(_bullex_request_counter)


def _auth_body():
    if BULLEX_AUTH_BODY_JSON:
        try:
            body = json.loads(BULLEX_AUTH_BODY_JSON)
            if not isinstance(body, dict):
                raise ValueError("BULLEX_AUTH_BODY_JSON precisa ser um objeto JSON.")
            return body
        except Exception as e:
            raise RuntimeError(
                f"BULLEX_AUTH_BODY_JSON invalido: {e}"
            )

    if not BULLEX_SSID:
        raise RuntimeError(
            "Configure BULLEX_SSID ou BULLEX_AUTH_BODY_JSON no Render. "
            "Nao coloque o segredo no codigo/GitHub."
        )

    return {"ssid": BULLEX_SSID}


def _montar_send_message(nome, version, body=None):
    payload = {
        "name": "sendMessage",
        "request_id": _next_request_id(),
        "local_time": int(time.time() * 1000) % 1_000_000,
        "msg": {
            "name": nome,
            "version": version,
        },
    }

    if body is not None:
        payload["msg"]["body"] = body

    return payload


def _normalizar_candle_ws(item):
    if not isinstance(item, dict):
        return None

    timestamp = item.get("from")
    if timestamp is None:
        timestamp = item.get("timestamp")
    if timestamp is None:
        timestamp = item.get("time")
    if timestamp is None:
        return None

    try:
        timestamp = float(timestamp)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0

        dt = datetime.fromtimestamp(timestamp, tz=TZ)

        return {
            "id": item.get("id"),
            "datetime": dt.isoformat(),
            "open": float(item["open"]),
            "high": float(item.get("max", item.get("high"))),
            "low": float(item.get("min", item.get("low"))),
            "close": float(item["close"]),
            "volume": float(item.get("volume", 0) or 0),
        }
    except (TypeError, ValueError, OverflowError, KeyError):
        return None


def _armazenar_candle_ws(active_id, size, item):
    candle = _normalizar_candle_ws(item)
    if candle is None:
        return

    chave = (int(active_id), int(size), candle.get("id"))
    if candle.get("id") is None:
        chave = (
            int(active_id),
            int(size),
            candle["datetime"],
        )

    with _bullex_cv:
        bucket = _bullex_candles.setdefault(
            (int(active_id), int(size)),
            {}
        )
        bucket[chave] = candle
        _bullex_cv.notify_all()


def _extrair_candles_da_resposta(msg):
    nome = msg.get("name")
    conteudo = msg.get("msg")

    if nome == "candles":
        if isinstance(conteudo, list):
            return conteudo
        if isinstance(conteudo, dict):
            for chave in ("candles", "data", "values"):
                if isinstance(conteudo.get(chave), list):
                    return conteudo[chave]

    if nome == "first-candles" and isinstance(conteudo, dict):
        por_tamanho = conteudo.get("candles_by_size", {})
        if isinstance(por_tamanho, dict):
            encontrados = []
            for valor in por_tamanho.values():
                if isinstance(valor, dict):
                    encontrados.append(valor)
                elif isinstance(valor, list):
                    encontrados.extend(valor)
            return encontrados

    return []


def _on_bullex_message(ws, raw_message):
    global _bullex_last_error

    try:
        data = json.loads(raw_message)
    except Exception:
        return

    # Eventos em tempo real confirmados na Traderoom.
    if data.get("name") == "candle-generated":
        msg = data.get("msg", {})
        if isinstance(msg, dict):
            active_id = msg.get("active_id")
            size = msg.get("size")
            if active_id is not None and size is not None:
                _armazenar_candle_ws(active_id, size, msg)
        return

    # Algumas mensagens vêm embrulhadas em data.
    if isinstance(data.get("data"), str):
        try:
            inner = json.loads(data["data"])
        except Exception:
            inner = None
        if isinstance(inner, dict):
            _on_bullex_message(ws, json.dumps(inner))
            return

    if isinstance(data.get("data"), dict):
        inner = data["data"]
        if inner.get("name") == "candle-generated":
            _on_bullex_message(ws, json.dumps(inner))
            return

    request_id = data.get("request_id")
    if request_id is not None:
        # Guarda a resposta para o solicitante.
        with _bullex_cv:
            _bullex_response_store[str(request_id)] = data
            _bullex_cv.notify_all()


def _on_bullex_error(ws, error):
    global _bullex_last_error
    _bullex_last_error = str(error)
    log(f"Bullex WebSocket erro: {error}")


def _on_bullex_close(ws, code, reason):
    global _bullex_connected
    _bullex_connected = False
    log(
        f"Bullex WebSocket fechado: "
        f"code={code} reason={reason}"
    )


def _on_bullex_open(ws):
    global _bullex_connected, _bullex_last_error
    _bullex_last_error = None
    _bullex_connected = True
    log("Bullex WebSocket conectado.")

    try:
        auth = _montar_send_message(
            "authenticate",
            "1.0",
            _auth_body()
        )
        ws.send(json.dumps(auth, separators=(",", ":")))
        log("Autenticacao WebSocket enviada.")
    except Exception as e:
        _bullex_last_error = str(e)
        log(f"Erro ao enviar autenticacao Bullex: {e}")


_bullex_response_store = {}


def _thread_bullex_ws():
    global _bullex_ws, _bullex_connected

    while True:
        try:
            cookie = os.getenv("BULLEX_COOKIE", "").strip()

            ws = websocket.WebSocketApp(
                BULLEX_WS_URL,
                cookie=cookie or None,
                on_open=_on_bullex_open,
                on_message=_on_bullex_message,
                on_error=_on_bullex_error,
                on_close=_on_bullex_close,
            )

            with _bullex_ws_lock:
                _bullex_ws = ws

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                origin=BULLEX_ORIGIN,
            )

        except Exception as e:
            _bullex_connected = False
            log(f"Falha no WebSocket Bullex: {e}")

        time.sleep(5)


def conectar_bullex():
    global _bullex_ws

    with _bullex_ws_lock:
        if _bullex_ws is not None and _bullex_connected:
            return _bullex_ws

        if not _auth_body():
            raise RuntimeError("Autenticacao Bullex nao configurada.")

        # Inicia apenas uma thread de WebSocket.
        if not getattr(conectar_bullex, "_thread_started", False):
            conectar_bullex._thread_started = True
            thread = threading.Thread(
                target=_thread_bullex_ws,
                daemon=True,
                name="bullex-websocket",
            )
            thread.start()

    limite = time.time() + 20
    while time.time() < limite:
        with _bullex_ws_lock:
            if _bullex_ws is not None and _bullex_connected:
                return _bullex_ws
        time.sleep(0.2)

    raise RuntimeError(
        "WebSocket Bullex nao conectou em 20 segundos. "
        "Verifique BULLEX_SSID/BULLEX_AUTH_BODY_JSON e os logs."
    )


def _enviar_e_aguardar(nome, version, body=None, timeout=15):
    ws = conectar_bullex()

    payload = _montar_send_message(
        nome,
        version,
        body
    )

    request_id = str(payload["request_id"])

    with _bullex_cv:
        _bullex_response_store.pop(request_id, None)

    ws.send(json.dumps(payload, separators=(",", ":")))

    limite = time.time() + timeout

    with _bullex_cv:
        while time.time() < limite:
            resposta = _bullex_response_store.pop(
                request_id,
                None
            )
            if resposta is not None:
                return resposta

            restante = limite - time.time()
            if restante <= 0:
                break

            _bullex_cv.wait(timeout=min(0.5, restante))

    raise RuntimeError(
        f"Timeout aguardando resposta Bullex: "
        f"{nome} request_id={request_id}"
    )


def _obter_ultimo_id(active_id, size):
    # Primeiro-candles é uma chamada confirmada pela Traderoom.
    resposta = _enviar_e_aguardar(
        "get-first-candles",
        "1.0",
        {
            "active_id": int(active_id),
            "split_normalization": True,
        },
        timeout=15,
    )

    msg = resposta.get("msg", {})
    if not isinstance(msg, dict):
        raise RuntimeError(
            "Resposta invalida em get-first-candles."
        )

    por_tamanho = msg.get("candles_by_size", {})
    valor = por_tamanho.get(str(size))

    if valor is None:
        # Alguns retornos podem usar chave numerica.
        valor = por_tamanho.get(size)

    if isinstance(valor, list):
        itens = valor
    elif isinstance(valor, dict):
        itens = [valor]
    else:
        itens = []

    if not itens:
        raise RuntimeError(
            f"get-first-candles nao retornou candle "
            f"para active_id={active_id}, size={size}."
        )

    ultimo = max(
        itens,
        key=lambda x: int(x.get("id", 0))
    )

    return int(ultimo["id"])


def obter_candles(
    symbol,
    interval=TIMEFRAME,
    outputsize=OUTPUTSIZE
):
    """Busca OHLC diretamente do WebSocket da Bullex."""
    codigo = None

    for chave, nome in ATIVOS.items():
        if nome == symbol:
            codigo = chave
            break

    if codigo is None:
        raise RuntimeError(
            f"Ativo nao mapeado para Bullex: {symbol}"
        )

    config = ATIVO_BULLEX[codigo]
    active_id = config["active_id"]

    size = _BULLEX_CANDLE_SIZES.get(interval)
    if size is None:
        raise RuntimeError(
            f"Timeframe nao suportado: {interval}"
        )

    ultimo_id = _obter_ultimo_id(
        active_id,
        size
    )

    # O protocolo confirmado usa from_id/to_id.
    from_id = max(
        1,
        ultimo_id - int(outputsize) + 1
    )

    resposta = _enviar_e_aguardar(
        "get-candles",
        "2.0",
        {
            "active_id": int(active_id),
            "size": int(size),
            "from_id": int(from_id),
            "to_id": int(ultimo_id),
            "split_normalization": True,
            "only_closed": True,
        },
        timeout=20,
    )

    dados = _extrair_candles_da_resposta(
        resposta
    )

    candles = []

    for item in dados:
        normalizado = _normalizar_candle_ws(item)
        if normalizado is not None:
            _armazenar_candle_ws(
                active_id,
                size,
                item
            )
            candles.append(normalizado)

    # Acrescenta eventos em tempo real já recebidos.
    with _bullex_cv:
        bucket = _bullex_candles.get(
            (int(active_id), int(size)),
            {}
        )
        candles.extend(bucket.values())

    # Remove duplicados pelo id/datetime.
    unicos = {}
    for candle in candles:
        chave = (
            candle.get("id"),
            candle.get("datetime")
        )
        unicos[chave] = candle

    candles = ordenar_candles(
        list(unicos.values())
    )

    if not candles:
        raise RuntimeError(
            f"Nenhum candle recebido da Bullex "
            f"para {symbol} ({interval})."
        )

    return candles[-int(outputsize):]


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
        / period
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
        / period
    )

    avg_loss = (
        sum(
            perdas[:period]
        )
        / period
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
# TENDÊNCIA 15 MIN
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
        and ema13
        and ema21
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
        abs(
            ema5
            -
            ema21
        )
        /
        preco
    )

    # Se as EMAs estiverem muito próximas,
    # a tendência é considerada fraca.
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

    # ========================================================
    # VALORES
    # ========================================================

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

    # ========================================================
    # VELAS
    # ========================================================

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
    #
    # SOMENTE -2 E -3.
    #
    # A vela -1 NÃO pode ser pullback.
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
    # MERCADO LATERAL
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

    # Tendência 5M
    if tendencia_5m == "ALTA":
        score_call += 3

    if tendencia_5m == "BAIXA":
        score_put += 3

    # Tendência 15M
    if tendencia_15m == "ALTA":
        score_call += 2

    if tendencia_15m == "BAIXA":
        score_put += 2

    # Pullback
    if pullback_call:
        score_call += 2

    if pullback_put:
        score_put += 2

    # Confirmação
    if confirmacao_call:
        score_call += 2

    if confirmacao_put:
        score_put += 2

    # RSI
    if rsi_call_ok:
        score_call += 1

    if rsi_put_ok:
        score_put += 1

    # Contexto
    if contexto_call:
        score_call += 1

    if contexto_put:
        score_put += 1

    # Corpo da confirmação
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

        # Exige também confirmação
        # do 15M.
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

        "vela": candles_5m[-1]["_dt"],

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

    # A entrada é na abertura
    # da próxima vela de 5 minutos.
    #
    # Como a vela de sinal acabou
    # de fechar, a próxima vela começa
    # imediatamente depois dela.

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

    if _ultimas_operacoes_registradas.get(symbol) == chave:
        log(
            f"{symbol}: operacao duplicada para a mesma vela ignorada."
        )
        return

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

        "entrada":
            None,

        "saida":
            None,

        "resultado":
            "PENDENTE",
    }

    _operacoes_pendentes[
        symbol
    ] = operacao
    _ultimas_operacoes_registradas[symbol] = chave

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

    # A operação só pode ser avaliada
    # depois do fechamento da vela de entrada.
    #
    # Exemplo:
    #
    # 10:55 vela de sinal fecha
    # 11:00 começa entrada
    # 11:05 fecha entrada
    #
    # Comparação:
    # preço de abertura da vela 11:00
    # contra fechamento da vela 11:00.

    alvo_dt = operacao[
        "vela_expiracao"
    ]

    for candle in candles:

        dt = candle["_dt"]

        if dt != alvo_dt:
            continue

        # Garantir que a vela já fechou.
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
        f"{fmt(operacao.get('saida'))}\n"
        f"Fonte da vela: Bullex\n\n"

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
        # PRIMEIRO:
        # VERIFICAR RESULTADO DE OPERAÇÃO ANTERIOR
        # ====================================================

        avaliar_operacao(
            symbol,
            candles_5m
        )

        # ====================================================
        # DADOS ATRASADOS
        # ====================================================

        if idade > MAX_ATRASO_MINUTOS:

            estado["ativo"] = symbol

            estado["sinal"] = (
                "AGUARDAR"
            )

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
                f"Dado atrasado "
                f"({idade:.1f} min)."
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

            log(
                f"{symbol}: poucas velas 5M."
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

            log(
                f"{symbol}: poucas velas 15M."
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
        # ATUALIZAR INTERFACE
        # ====================================================

        estado["ativo"] = symbol

        estado["sinal"] = (
            resultado["sinal"]
        )

        estado["score"] = (
            resultado["score"]
        )

        preco = resultado.get(
            "preco"
        )

        estado["preco"] = (

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

        estado["vela"] = (

            vela.strftime(
                "%Y-%m-%d %H:%M:%S BRT"
            )

            if isinstance(
                vela,
                datetime
            )

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

        estado["mensagem"] = (
            resultado.get(
                "mensagem",
                ""
            )
        )

        estado["detalhes"] = {

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
            f"{estado['preco']}"
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

        estado["ativo"] = symbol

        estado["sinal"] = (
            "AGUARDAR"
        )

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

    log(
        "================================"
    )

    log(
        "INICIANDO LEITURA"
    )

    log(
        "================================"
    )

    # ========================================================
    # AUTENTICAÇÃO BULLEX
    # ========================================================
    # A versão atual usa BULLEX_SSID ou BULLEX_AUTH_BODY_JSON.
    # Não usa mais BULLEX_EMAIL/BULLEX_SENHA.

    try:

        _auth_body()

    except Exception as e:

        log(
            f"ERRO: autenticacao Bullex nao configurada: {e}"
        )

        estado["sinal"] = (
            "AGUARDAR"
        )

        estado["score"] = 0

        estado["mensagem"] = (
            "Configure BULLEX_SSID ou "
            "BULLEX_AUTH_BODY_JSON no Render."
        )

        estado["atualizado"] = (
            agora_brt().strftime(
                "%H:%M:%S BRT"
            )
        )

        return

    if not dentro_do_horario():

        agora = agora_brt()

        log(
            "Fora do horario configurado."
        )

        estado["sinal"] = (
            "AGUARDAR"
        )

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

    estado[
        "estatisticas"
    ] = calcular_estatisticas()

    log(
        "Leitura concluida."
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

    box-shadow:
    0 4px 15px
    rgba(0,0,0,.25);
}

.sinal {

    font-size: 42px;

    font-weight: bold;

    text-align: center;

    margin: 15px 0;
}

.linha {

    display: flex;

    justify-content:
    space-between;

    gap: 10px;

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
}

.estatisticas {

    display: grid;

    grid-template-columns:
    repeat(2, 1fr);

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

<h1>
Robo Forex Pullback PRO
</h1>

<div class="subtitulo">

5M + 15M + Pullback +
Confirmação + RSI + ATR

</div>


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

<h3>
Filtros da entrada
</h3>

<div class="linha">

<span>Tendência 5M</span>

<span class="valor">
{{ estado.detalhes.tendencia_5m }}
</span>

</div>

<div class="linha">

<span>Tendência 15M</span>

<span class="valor">
{{ estado.detalhes.tendencia_15m }}
</span>

</div>

<div class="linha">

<span>Pullback</span>

<span class="valor">
{{ estado.detalhes.pullback }}
</span>

</div>

<div class="linha">

<span>Confirmação</span>

<span class="valor">
{{ estado.detalhes.confirmacao }}
</span>

</div>

<div class="linha">

<span>Mercado lateral</span>

<span class="valor">
{{ estado.detalhes.lateral }}
</span>

</div>

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

<div class="linha">

<span>ATR 14</span>

<span class="valor">
{{ estado.detalhes.atr }}
</span>

</div>

</div>


<div class="card">

<h3>
Estatísticas
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


<div class="card">

<div class="observacao">

{{ estado.mensagem }}

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
com base na vela de expiração recebida da Bullex.

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
        "fonte_candles": "Bullex",
        "execucao_automatica": False,

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