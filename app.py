```python
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

BULLEX_WS_URL = os.getenv(
    "BULLEX_WS_URL",
    "wss://ws.trade.bull-ex.com/echo/websocket"
).strip()

BULLEX_ORIGIN = os.getenv(
    "BULLEX_ORIGIN",
    "https://bull-ex.com"
).strip()

BULLEX_SSID = os.getenv("BULLEX_SSID", "").strip()
BULLEX_COOKIE = os.getenv("BULLEX_COOKIE", "").strip()

BULLEX_PROTOCOL = int(
    os.getenv("BULLEX_PROTOCOL", "3") or "3"
)

BULLEX_USER_AGENT = os.getenv(
    "BULLEX_USER_AGENT",
    "Mozilla/5.0 (Linux; Android 10; K) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/152.0.0.0 Mobile Safari/537.36"
).strip()

try:
    BULLEX_LOCAL_TIME = int(
        os.getenv("BULLEX_LOCAL_TIME", "9087")
    )
except Exception:
    BULLEX_LOCAL_TIME = 9087


# ============================================================
# EXECUÇÃO AUTOMÁTICA DEMO
# ============================================================

BULLEX_AUTO_TRADE = (
    os.getenv(
        "BULLEX_AUTO_TRADE",
        "true"
    ).strip().lower()
    in ("1", "true", "yes", "sim", "on")
)

BULLEX_USER_BALANCE_ID = os.getenv(
    "BULLEX_USER_BALANCE_ID",
    ""
).strip()

# Progressão:
# R$5 -> R$10,50 -> R$23
VALORES_ENTRADA = [5.00, 10.50, 23.00]

EXPIRACAO_MINUTOS = 5

# Uma operação por vez globalmente.
UMA_OPERACAO_GLOBAL = True


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
).strip()

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
).strip()


# ============================================================
# ATIVOS BULLEX
# ============================================================

ATIVO_BULLEX = {
    "EURUSD": {
        "symbol": "EUR/USD",
        "active_id": 76,
        "ticker": "EURUSD-OTC",
    },
    "EURJPY": {
        "symbol": "EUR/JPY",
        "active_id": 79,
        "ticker": "EURJPY-OTC",
    },
    "GBPUSD": {
        "symbol": "GBP/USD",
        "active_id": 81,
        "ticker": "GBPUSD-OTC",
    },
    "USDJPY": {
        "symbol": "USD/JPY",
        "active_id": 85,
        "ticker": "USDJPY-OTC",
    },
    "GBPJPY": {
        "symbol": "GBP/JPY",
        "active_id": 84,
        "ticker": "GBPJPY-OTC",
    },
}

ATIVOS = {
    chave: valor["symbol"]
    for chave, valor in ATIVO_BULLEX.items()
}


# ============================================================
# TIMEFRAMES
# ============================================================

TIMEFRAME = "5min"
TIMEFRAME_TREND = "15min"

BULLEX_CANDLE_SIZES = {
    "5min": 300,
    "15min": 900,
}

TIMEZONE = "America/Sao_Paulo"
TZ = ZoneInfo(TIMEZONE)

OUTPUTSIZE = 150
OUTPUTSIZE_15M = 100

HORA_INICIO = 6
HORA_FIM = 22

MAX_ATRASO_MINUTOS = 8


# ============================================================
# ESTADO DO WEBSOCKET
# ============================================================

_bullex_ws = None
_bullex_ws_lock = threading.RLock()
_bullex_request_lock = threading.Lock()
_bullex_cv = threading.Condition(_bullex_ws_lock)

_bullex_request_counter = 1000

_bullex_connected = False
_bullex_authenticated = False
_bullex_last_error = None

_bullex_ws_thread_started = False

_bullex_auth_event = threading.Event()
_bullex_auth_request_id = None
_bullex_client_session_id = None

_bullex_response_store = {}
_bullex_candles = {}

_bullex_balance_id = None
_bullex_instrument_cache = {}


# ============================================================
# DIAGNÓSTICO
# ============================================================

BULLEX_DIAGNOSTIC_VERSION = (
    "OTC-AUTO-DEMO-5M-20260906-02"
)

_bullex_diag = {
    "messages": 0,
    "generated": 0,
    "responses": 0,
    "stored": 0,
    "orders_sent": 0,
    "orders_confirmed": 0,
    "orders_errors": 0,
    "last_name": None,
    "last_request_id": None,
    "last_active_id": None,
    "last_size": None,
}

_bullex_diag_lock = threading.Lock()


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
        "tendencia_5m": "-",
        "tendencia_15m": "-",
        "pullback": "-",
        "confirmacao": "-",
        "lateral": "-",
        "atr": "-",
        "bloqueio": "-",
    },

    "estatisticas": {
        "total": 0,
        "wins": 0,
        "losses": 0,
        "dojis": 0,
        "taxa": 0.0,
    },

    "execucao": {
        "automatica": BULLEX_AUTO_TRADE,
        "modo": "DEMO",
        "valor_atual": VALORES_ENTRADA[0],
        "nivel_progressao": 0,
        "operacao_ativa": False,
        "ultima_ordem": None,
        "ultimo_erro": None,
    },
}


_robo_lock = threading.Lock()
_robo_started = False

_ultimos_sinais_telegram = {}

_operacoes_pendentes = {}
_ultimas_operacoes_registradas = {}

_historico_resultados = []

_execucao_lock = threading.RLock()
_operacao_global_ativa = None

_nivel_progressao = 0


# ============================================================
# UTILITÁRIOS
# ============================================================

def log(msg):
    agora = datetime.now(TZ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    print(
        f"[BOT {agora}] {msg}",
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
            dt = dt.replace(tzinfo=TZ)

        return dt.astimezone(TZ)

    except Exception:
        pass

    formatos = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    )

    for fmt in formatos:
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
        try:
            item = dict(candle)

            dt = parse_datetime_candle(
                item.get("datetime")
            )

            if dt is None:
                continue

            item["_dt"] = dt
            resultado.append(item)

        except Exception:
            continue

    resultado.sort(
        key=lambda x: x["_dt"]
    )

    return resultado


def somente_velas_fechadas(
    candles,
    minutos
):
    candles = ordenar_candles(candles)

    agora = agora_brt()

    return [
        candle
        for candle in candles
        if (
            candle["_dt"]
            + timedelta(minutes=minutos)
            <= agora
        )
    ]


def idade_do_ultimo_candle(candles):
    ordenadas = ordenar_candles(candles)

    if not ordenadas:
        return None, None

    ultimo = ordenadas[-1]

    idade = (
        agora_brt()
        - ultimo["_dt"]
    ).total_seconds() / 60

    return ultimo, idade


# ============================================================
# BULLEX - AUTENTICAÇÃO
# ============================================================

def _auth_body():
    if not BULLEX_SSID:
        raise RuntimeError(
            "Configure BULLEX_SSID no Render."
        )

    return {
        "ssid": BULLEX_SSID,
        "protocol": BULLEX_PROTOCOL,
        "session_id": "",
        "client_session_id": "",
    }


def _next_request_id():
    global _bullex_request_counter

    with _bullex_request_lock:
        _bullex_request_counter += 1
        contador = _bullex_request_counter

    return (
        f"{int(time.time())}_{contador}"
    )


def _montar_auth_message():
    return {
        "name": "authenticate",
        "request_id": _next_request_id(),
        "local_time": BULLEX_LOCAL_TIME,
        "msg": {
            "ssid": BULLEX_SSID,
            "protocol": BULLEX_PROTOCOL,
            "session_id": "",
            "client_session_id": "",
        },
    }


def _montar_send_message(
    nome,
    version,
    body=None
):
    payload = {
        "name": "sendMessage",
        "request_id": _next_request_id(),
        "local_time": (
            int(time.time() * 1000)
            % 1_000_000
        ),
        "msg": {
            "name": nome,
            "version": version,
        },
    }

    if body is not None:
        payload["msg"]["body"] = body

    return payload


# ============================================================
# BULLEX - CANDLE NORMALIZATION
# ============================================================

def _normalizar_candle_ws(item):
    if not isinstance(item, dict):
        return None

    timestamp = (
        item.get("from")
        or item.get("timestamp")
        or item.get("time")
    )

    if timestamp is None:
        return None

    try:
        timestamp = float(timestamp)

        if timestamp > 10_000_000_000_000:
            timestamp /= 1_000_000_000

        elif timestamp > 10_000_000_000:
            timestamp /= 1_000

        dt = datetime.fromtimestamp(
            timestamp,
            tz=TZ
        )

        high = item.get(
            "max",
            item.get("high")
        )

        low = item.get(
            "min",
            item.get("low")
        )

        return {
            "id": item.get("id"),
            "datetime": dt.isoformat(),
            "open": float(item["open"]),
            "high": float(high),
            "low": float(low),
            "close": float(item["close"]),
            "volume": float(
                item.get("volume", 0) or 0
            ),
            "phase": item.get("phase"),
        }

    except Exception:
        return None


def _armazenar_candle_ws(
    active_id,
    size,
    item
):
    candle = _normalizar_candle_ws(item)

    if candle is None:
        return

    try:
        active_id = int(active_id)
        size = int(size)

    except Exception:
        return

    candle_id = candle.get("id")

    chave = (
        str(candle_id)
        if candle_id is not None
        else candle["datetime"]
    )

    with _bullex_cv:

        bucket = _bullex_candles.setdefault(
            (active_id, size),
            {}
        )

        bucket[chave] = candle

        _bullex_cv.notify_all()


def _candidatos_candles_cache(
    active_id,
    size
):
    with _bullex_cv:

        bucket = _bullex_candles.get(
            (int(active_id), int(size)),
            {}
        )

        return list(
            bucket.values()
        )


# ============================================================
# BULLEX - EXTRAÇÃO DE CANDLES
# ============================================================

def _extrair_candles_da_resposta(data):
    if not isinstance(data, dict):
        return []

    msg = data.get("msg")

    if isinstance(msg, list):
        return [
            x
            for x in msg
            if isinstance(x, dict)
        ]

    if not isinstance(msg, dict):
        msg = data

    encontrados = []

    for chave in (
        "candles",
        "data",
        "values"
    ):

        valor = msg.get(chave)

        if isinstance(valor, list):

            encontrados.extend(
                x
                for x in valor
                if isinstance(x, dict)
            )

        elif (
            isinstance(valor, dict)
            and "open" in valor
            and "close" in valor
        ):

            encontrados.append(
                valor
            )

    por_tamanho = msg.get(
        "candles_by_size"
    )

    if isinstance(
        por_tamanho,
        dict
    ):

        for valor in por_tamanho.values():

            if isinstance(
                valor,
                list
            ):

                encontrados.extend(
                    x
                    for x in valor
                    if isinstance(x, dict)
                )

            elif isinstance(
                valor,
                dict
            ):

                encontrados.append(
                    valor
                )

    if (
        not encontrados
        and "open" in msg
        and "close" in msg
    ):

        encontrados.append(msg)

    return encontrados


# ============================================================
# BULLEX - EXTRAÇÃO DO BALANCE ID
# ============================================================

def _extrair_balance_id(data):
    global _bullex_balance_id

    if not isinstance(data, dict):
        return

    candidatos = []

    def procurar(obj):

        if isinstance(obj, dict):

            for chave in (
                "user_balance_id",
                "balance_id",
                "balanceId",
                "userBalanceId",
            ):

                if obj.get(chave) is not None:

                    candidatos.append(
                        obj.get(chave)
                    )

            for valor in obj.values():
                procurar(valor)

        elif isinstance(obj, list):

            for valor in obj:
                procurar(valor)

    procurar(data)

    if candidatos:

        valor = candidatos[0]

        if valor is not None:

            _bullex_balance_id = str(
                valor
            )

            log(
                "Balance ID recebido da Bullex: "
                f"{_bullex_balance_id}"
            )


def _obter_balance_id():

    if BULLEX_USER_BALANCE_ID:
        return BULLEX_USER_BALANCE_ID

    if _bullex_balance_id:
        return str(
            _bullex_balance_id
        )

    return None


# ============================================================
# BULLEX - RESPOSTA AUTH
# ============================================================

def _mensagem_indica_auth_sucesso(data):

    global _bullex_client_session_id

    if not isinstance(data, dict):
        return False

    if data.get("name") != "authenticated":
        return False

    if data.get("msg") is not True:
        return False

    _bullex_client_session_id = (
        data.get(
            "client_session_id"
        )
    )

    return True


def _mensagem_indica_auth_erro(data):

    texto = json.dumps(
        data,
        ensure_ascii=False
    ).lower()

    palavras = (
        "unauthorized",
        "authentication failed",
        "auth failed",
        "invalid ssid",
        "invalid session",
        "not authenticated",
        "authentication error",
    )

    return any(
        p in texto
        for p in palavras
    )


# ============================================================
# BULLEX - ARMAZENAR RESPOSTA DE CANDLES
# ============================================================

def _armazenar_candles_resposta(
    active_id,
    msg
):

    if not isinstance(msg, dict):
        return

    por_tamanho = msg.get(
        "candles_by_size"
    )

    if isinstance(
        por_tamanho,
        dict
    ):

        for size_key, valores in (
            por_tamanho.items()
        ):

            try:
                size = int(size_key)

            except Exception:
                continue

            if isinstance(
                valores,
                dict
            ):
                valores = [valores]

            if not isinstance(
                valores,
                list
            ):
                continue

            for item in valores:

                if isinstance(
                    item,
                    dict
                ):

                    _armazenar_candle_ws(
                        active_id,
                        size,
                        item
                    )

    size = msg.get("size")
    dados = msg.get("candles")

    if (
        size is not None
        and isinstance(dados, list)
    ):

        for item in dados:

            if isinstance(
                item,
                dict
            ):

                _armazenar_candle_ws(
                    active_id,
                    size,
                    item
                )


# ============================================================
# BULLEX - MESSAGE HANDLER
# ============================================================

def _on_bullex_message(
    ws,
    raw_message
):

    global _bullex_last_error
    global _bullex_authenticated

    try:

        data = json.loads(
            raw_message
        )

    except Exception:

        return

    if not isinstance(data, dict):
        return

    # Algumas respostas podem vir encapsuladas.
    if isinstance(
        data.get("data"),
        str
    ):

        try:

            inner = json.loads(
                data["data"]
            )

            if isinstance(
                inner,
                dict
            ):

                _on_bullex_message(
                    ws,
                    json.dumps(inner)
                )

                return

        except Exception:
            pass

    if isinstance(
        data.get("data"),
        dict
    ):

        inner = data["data"]

        if isinstance(
            inner,
            dict
        ):

            _on_bullex_message(
                ws,
                json.dumps(inner)
            )

            return

    _extrair_balance_id(data)

    nome = data.get("name")

    request_id = data.get(
        "request_id"
    )

    msg = data.get("msg")

    active_id = None
    size = None

    if isinstance(msg, dict):

        active_id = msg.get(
            "active_id"
        )

        size = msg.get(
            "size"
        )

    with _bullex_diag_lock:

        _bullex_diag[
            "messages"
        ] += 1

        _bullex_diag[
            "last_name"
        ] = nome

        _bullex_diag[
            "last_request_id"
        ] = request_id

        _bullex_diag[
            "last_active_id"
        ] = active_id

        _bullex_diag[
            "last_size"
        ] = size

    # --------------------------------------------------------
    # AUTH
    # --------------------------------------------------------

    if _mensagem_indica_auth_sucesso(
        data
    ):

        _bullex_authenticated = True

        _bullex_auth_event.set()

        log(
            "Autenticacao Bullex confirmada."
        )

        threading.Thread(
            target=_assinar_candles_otc,
            daemon=True,
            name="bullex-subscriptions",
        ).start()

        return

    if _mensagem_indica_auth_erro(
        data
    ):

        _bullex_authenticated = False

        _bullex_last_error = (
            "Bullex recusou a autenticacao."
        )

        _bullex_auth_event.set()

        log(
            "Bullex recusou a autenticacao."
        )

        return

    # --------------------------------------------------------
    # CANDLE GENERATED
    # --------------------------------------------------------

    if nome == "candle-generated":

        with _bullex_diag_lock:

            _bullex_diag[
                "generated"
            ] += 1

        if isinstance(msg, dict):

            active_id = msg.get(
                "active_id"
            )

            size = msg.get(
                "size"
            )

            if (
                active_id is not None
                and size is not None
            ):

                _armazenar_candle_ws(
                    active_id,
                    size,
                    msg
                )

                with _bullex_diag_lock:

                    _bullex_diag[
                        "stored"
                    ] += 1

        return

    # --------------------------------------------------------
    # RESPOSTA DE CANDLES
    # --------------------------------------------------------

    dados = _extrair_candles_da_resposta(
        data
    )

    is_candle_response = (
        nome in (
            "candles",
            "first-candles",
            "get-candles",
        )
        or bool(dados)
    )

    if is_candle_response:

        with _bullex_diag_lock:

            _bullex_diag[
                "responses"
            ] += 1

        if request_id is not None:

            with _bullex_cv:

                _bullex_response_store[
                    str(request_id)
                ] = data

        if isinstance(msg, dict):

            response_active_id = (
                msg.get("active_id")
            )

            if response_active_id is not None:

                _armazenar_candles_resposta(
                    response_active_id,
                    msg
                )

                for item in dados:

                    if not isinstance(
                        item,
                        dict
                    ):
                        continue

                    item_size = (
                        item.get("size")
                        or
                        msg.get("size")
                    )

                    if item_size is not None:

                        _armazenar_candle_ws(
                            response_active_id,
                            item_size,
                            item
                        )

        with _bullex_cv:
            _bullex_cv.notify_all()

        return

    # --------------------------------------------------------
    # QUALQUER RESPOSTA COM REQUEST_ID
    # --------------------------------------------------------

    if request_id is not None:

        with _bullex_cv:

            _bullex_response_store[
                str(request_id)
            ] = data

            _bullex_cv.notify_all()

    # --------------------------------------------------------
    # LOG DE ORDENS
    # --------------------------------------------------------

    if nome in (
        "digital-option-placed",
        "position-changed",
        "order-changed",
    ):

        log(
            f"[ORDEM WS] {nome} "
            f"request_id={request_id}"
        )

        with _bullex_diag_lock:

            if nome == "digital-option-placed":

                _bullex_diag[
                    "orders_confirmed"
                ] += 1


# ============================================================
# WEBSOCKET ERROR / CLOSE
# ============================================================

def _on_bullex_error(
    ws,
    error
):

    global _bullex_last_error

    _bullex_last_error = str(error)

    log(
        f"Bullex WebSocket erro: {error}"
    )

    with _bullex_cv:
        _bullex_cv.notify_all()


def _on_bullex_close(
    ws,
    code,
    reason
):

    global _bullex_connected
    global _bullex_authenticated
    global _bullex_client_session_id

    _bullex_connected = False
    _bullex_authenticated = False
    _bullex_client_session_id = None

    _bullex_auth_event.clear()

    with _bullex_cv:
        _bullex_cv.notify_all()

    log(
        f"Bullex WebSocket fechado: "
        f"code={code} reason={reason}"
    )


# ============================================================
# WEBSOCKET OPEN
# ============================================================

def _on_bullex_open(ws):

    global _bullex_connected
    global _bullex_last_error
    global _bullex_authenticated
    global _bullex_auth_request_id
    global _bullex_client_session_id

    _bullex_last_error = None
    _bullex_connected = True
    _bullex_authenticated = False
    _bullex_client_session_id = None

    _bullex_auth_request_id = None

    _bullex_auth_event.clear()

    log(
        "Bullex WebSocket conectado."
    )

    try:

        auth = _montar_auth_message()

        _bullex_auth_request_id = str(
            auth["request_id"]
        )

        ws.send(
            json.dumps(
                auth,
                separators=(",", ":")
            )
        )

        log(
            "Autenticacao WebSocket enviada."
        )

    except Exception as e:

        _bullex_last_error = str(e)

        log(
            f"Erro ao enviar autenticacao: {e}"
        )

        _bullex_auth_event.set()


# ============================================================
# THREAD PERSISTENTE DO WEBSOCKET
# ============================================================

def _thread_bullex_ws():

    global _bullex_ws
    global _bullex_connected
    global _bullex_authenticated

    while True:

        try:

            headers = [
                f"User-Agent: {BULLEX_USER_AGENT}"
            ]

            ws = websocket.WebSocketApp(
                BULLEX_WS_URL,
                cookie=BULLEX_COOKIE or None,
                header=headers,
                on_open=_on_bullex_open,
                on_message=_on_bullex_message,
                on_error=_on_bullex_error,
                on_close=_on_bullex_close,
            )

            with _bullex_ws_lock:
                _bullex_ws = ws

            log(
                "Iniciando conexão WebSocket Bullex."
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                origin=BULLEX_ORIGIN,
            )

        except Exception as e:

            _bullex_connected = False
            _bullex_authenticated = False
            _bullex_auth_event.clear()

            log(
                f"Falha no WebSocket Bullex: {e}"
            )

            with _bullex_cv:
                _bullex_cv.notify_all()

        finally:

            with _bullex_ws_lock:

                _bullex_connected = False
                _bullex_authenticated = False
                _bullex_ws = None

            with _bullex_cv:
                _bullex_cv.notify_all()

        time.sleep(5)


def conectar_bullex():

    global _bullex_ws_thread_started

    _auth_body()

    with _bullex_ws_lock:

        if (
            _bullex_ws is not None
            and _bullex_connected
        ):

            return _bullex_ws

        if not _bullex_ws_thread_started:

            _bullex_ws_thread_started = True

            thread = threading.Thread(
                target=_thread_bullex_ws,
                daemon=True,
                name="bullex-websocket",
            )

            thread.start()

    limite = time.time() + 20

    while time.time() < limite:

        with _bullex_ws_lock:

            if (
                _bullex_ws is not None
                and _bullex_connected
            ):

                return _bullex_ws

        time.sleep(0.2)

    raise RuntimeError(
        "WebSocket Bullex nao conectou em 20 segundos."
    )


def _aguardar_autenticacao(timeout=15):

    limite = time.time() + timeout

    while time.time() < limite:

        if _bullex_authenticated:
            return True

        if (
            not _bullex_connected
            and _bullex_last_error
        ):

            raise RuntimeError(
                "Conexao fechou antes da autenticacao: "
                f"{_bullex_last_error}"
            )

        if not _bullex_connected:

            raise RuntimeError(
                "Conexao fechou antes da autenticacao."
            )

        restante = (
            limite - time.time()
        )

        if restante <= 0:
            break

        _bullex_auth_event.wait(
            timeout=min(
                0.5,
                restante
            )
        )

    if _bullex_authenticated:
        return True

    raise RuntimeError(
        "Timeout aguardando autenticacao Bullex."
    )


# ============================================================
# SUBSCRIÇÃO DOS CANDLES
# ============================================================

def _montar_subscribe_candle(
    active_id,
    size,
    request_id=None
):

    return {
        "name": "subscribeMessage",

        "request_id": str(
            request_id
            or
            _next_request_id()
        ),

        "local_time": (
            int(time.time() * 1000)
            % 1_000_000
        ),

        "msg": {
            "name": "candle-generated",

            "params": {
                "routingFilters": {
                    "active_id": int(active_id),
                    "size": int(size),
                }
            },
        },
    }


def _assinar_candle(
    active_id,
    size
):

    ws = conectar_bullex()

    _aguardar_autenticacao(
        timeout=15
    )

    payload = _montar_subscribe_candle(
        active_id,
        size
    )

    try:

        ws.send(
            json.dumps(
                payload,
                separators=(",", ":")
            )
        )

        log(
            f"Assinatura candle enviada: "
            f"active_id={active_id} "
            f"size={size}"
        )

    except Exception as e:

        raise RuntimeError(
            f"Falha ao assinar candle "
            f"{active_id}/{size}: {e}"
        )


def _assinar_candles_otc():

    for config in ATIVO_BULLEX.values():

        active_id = int(
            config["active_id"]
        )

        for size in (
            300,
            900
        ):

            try:

                _assinar_candle(
                    active_id,
                    size
                )

            except Exception as e:

                log(
                    f"Nao foi possivel assinar "
                    f"active_id={active_id} "
                    f"size={size}: {e}"
                )


# ============================================================
# ENVIO DE REQUEST E ESPERA DE RESPOSTA
# ============================================================

def _enviar_e_aguardar(
    nome,
    version,
    body=None,
    timeout=15
):

    ws = conectar_bullex()

    _aguardar_autenticacao(
        timeout=15
    )

    payload = _montar_send_message(
        nome,
        version,
        body
    )

    request_id = str(
        payload["request_id"]
    )

    with _bullex_cv:

        _bullex_response_store.pop(
            request_id,
            None
        )

    try:

        ws.send(
            json.dumps(
                payload,
                separators=(",", ":")
            )
        )

    except Exception as e:

        raise RuntimeError(
            f"Falha ao enviar {nome}: {e}"
        )

    limite = time.time() + timeout

    with _bullex_cv:

        while time.time() < limite:

            resposta = (
                _bullex_response_store.pop(
                    request_id,
                    None
                )
            )

            if resposta is not None:
                return resposta

            if (
                not _bullex_connected
                and _bullex_last_error
            ):

                raise RuntimeError(
                    f"Bullex fechou durante "
                    f"{nome}: "
                    f"{_bullex_last_error}"
                )

            if not _bullex_connected:

                raise RuntimeError(
                    f"Bullex fechou durante {nome}."
                )

            restante = (
                limite - time.time()
            )

            if restante <= 0:
                break

            _bullex_cv.wait(
                timeout=min(
                    0.5,
                    restante
                )
            )

    raise RuntimeError(
        f"Timeout aguardando resposta: "
        f"{nome} request_id={request_id}"
    )


# ============================================================
# OBTENÇÃO DO ÚLTIMO ID DE CANDLE
# ============================================================

def _obter_ultimo_id(
    active_id,
    size,
    timeout=15
):

    active_id = int(active_id)
    size = int(size)

    def ultimo_cache():

        candles = (
            _candidatos_candles_cache(
                active_id,
                size
            )
        )

        ids = []

        for candle in candles:

            try:

                cid = int(
                    candle.get("id")
                )

                ids.append(
                    (cid, candle)
                )

            except Exception:
                pass

        if not ids:
            return None

        return max(
            ids,
            key=lambda x: x[0]
        )

    limite = time.time() + timeout

    while time.time() < limite:

        encontrado = ultimo_cache()

        if encontrado:

            ultimo_id, candle = encontrado

            return int(
                ultimo_id
            )

        with _bullex_cv:

            restante = (
                limite - time.time()
            )

            if restante <= 0:
                break

            _bullex_cv.wait(
                timeout=min(
                    0.5,
                    restante
                )
            )

    resposta = _enviar_e_aguardar(
        "get-first-candles",
        "1.0",
        {
            "active_id": active_id,
            "split_normalization": True,
        },
        timeout=15,
    )

    msg = resposta.get(
        "msg",
        {}
    )

    if not isinstance(
        msg,
        dict
    ):

        raise RuntimeError(
            "Resposta invalida em get-first-candles."
        )

    ids = []

    por_tamanho = msg.get(
        "candles_by_size",
        {}
    )

    if isinstance(
        por_tamanho,
        dict
    ):

        valores = (
            por_tamanho.get(
                str(size)
            )
        )

        if valores is None:

            valores = (
                por_tamanho.get(size)
            )

        if isinstance(
            valores,
            dict
        ):

            valores = [valores]

        if isinstance(
            valores,
            list
        ):

            for item in valores:

                try:

                    ids.append(
                        int(item["id"])
                    )

                except Exception:
                    pass

    if not ids:

        for item in (
            _extrair_candles_da_resposta(
                resposta
            )
        ):

            try:

                ids.append(
                    int(item["id"])
                )

            except Exception:
                pass

    if ids:

        return min(ids)

    raise RuntimeError(
        f"Nao foi possivel descobrir "
        f"ID do candle "
        f"{active_id}/{size}."
    )


# ============================================================
# OBTENÇÃO DOS CANDLES
# ============================================================

def obter_candles(
    symbol,
    interval=TIMEFRAME,
    outputsize=OUTPUTSIZE
):

    codigo = None

    for chave, nome in ATIVOS.items():

        if nome == symbol:

            codigo = chave
            break

    if codigo is None:

        raise RuntimeError(
            f"Ativo nao mapeado: {symbol}"
        )

    active_id = int(
        ATIVO_BULLEX[codigo]["active_id"]
    )

    size = BULLEX_CANDLE_SIZES.get(
        interval
    )

    if size is None:

        raise RuntimeError(
            f"Timeframe nao suportado: {interval}"
        )

    cache = ordenar_candles(
        _candidatos_candles_cache(
            active_id,
            size
        )
    )

    if len(cache) >= outputsize:

        return cache[
            -int(outputsize):
        ]

    ultimo_id = _obter_ultimo_id(
        active_id,
        size,
        timeout=15
    )

    to_id = int(
        ultimo_id
    )

    for candle in cache:

        try:

            if int(
                candle.get("id")
            ) == int(ultimo_id):

                if candle.get(
                    "phase"
                ) == "T":

                    to_id = max(
                        1,
                        int(ultimo_id) - 1
                    )

                break

        except Exception:
            pass

    from_id = max(
        1,
        to_id - int(outputsize) + 1
    )

    resposta = _enviar_e_aguardar(
        "get-candles",
        "2.0",
        {
            "active_id": active_id,
            "size": size,
            "from_id": from_id,
            "to_id": to_id,
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

        candle = _normalizar_candle_ws(
            item
        )

        if candle is None:
            continue

        _armazenar_candle_ws(
            active_id,
            size,
            item
        )

        candles.append(
            candle
        )

    candles.extend(
        _candidatos_candles_cache(
            active_id,
            size
        )
    )

    unicos = {}

    for candle in candles:

        chave = (
            candle.get("id"),
            candle.get("datetime")
        )

        unicos[chave] = candle

    candles = ordenar_candles(
        list(
            unicos.values()
        )
    )

    if not candles:

        raise RuntimeError(
            f"Nenhum candle recebido "
            f"para {symbol}/{interval}."
        )

    return candles[
        -int(outputsize):
    ]


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


def ema_series(
    values,
    period
):

    if len(values) < period:

        return [
            None
        ] * len(values)

    k = 2 / (
        period + 1
    )

    resultado = [
        None
    ] * (period - 1)

    valor = (
        sum(values[:period])
        / period
    )

    resultado.append(
        valor
    )

    for preco in values[period:]:

        valor = (
            preco * k
            +
            valor * (1 - k)
        )

        resultado.append(
            valor
        )

    return resultado


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
            - values[i - 1]
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
                avg_gain
                * (period - 1)
            )
            +
            ganhos[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            +
            perdas[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = (
        avg_gain
        / avg_loss
    )

    return (
        100
        -
        (
            100
            / (1 + rs)
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
                    - close_anterior
                ),
                abs(
                    low
                    - close_anterior
                ),
            )
        )

    if len(trs) < period:
        return None

    return (
        sum(trs[-period:])
        / period
    )


# ============================================================
# INFORMAÇÃO DA VELA
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
        fechamento
        - abertura
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
        "body_ratio": (
            corpo
            / range_vela
        ),
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
            - referencia
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

def pullback_na_vela(
    info,
    ema13,
    ema21,
    direcao
):

    if (
        not info
        or ema13 is None
        or ema21 is None
    ):

        return False

    referencias = (
        ema13,
        ema21
    )

    toque = any(
        info["low"]
        <= ref
        <= info["high"]
        for ref in referencias
    )

    if direcao == "CALL":

        proximidade = (
            min(
                percentual_distancia(
                    info["low"],
                    ref
                )
                for ref in referencias
            )
            <= 0.0007
        )

        vela_retracao = (
            info["close"]
            <= info["open"]
            or
            info["body_ratio"]
            <= 0.55
        )

        return (
            toque
            or proximidade
        ) and vela_retracao

    if direcao == "PUT":

        proximidade = (
            min(
                percentual_distancia(
                    info["high"],
                    ref
                )
                for ref in referencias
            )
            <= 0.0007
        )

        vela_retracao = (
            info["close"]
            >= info["open"]
            or
            info["body_ratio"]
            <= 0.55
        )

        return (
            toque
            or proximidade
        ) and vela_retracao

    return False


def pullback_call_na_vela(
    info,
    ema13,
    ema21
):

    return pullback_na_vela(
        info,
        ema13,
        ema21,
        "CALL"
    )


def pullback_put_na_vela(
    info,
    ema13,
    ema21
):

    return pullback_na_vela(
        info,
        ema13,
        ema21,
        "PUT"
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
        and ema5
        and ema13
        and ema21
    ):

        return True

    distancia_5_21 = (
        abs(
            ema5
            - ema21
        )
        / preco
    )

    if distancia_5_21 < 0.00025:
        return True

    if atr14:

        atr_ratio = (
            atr14
            / preco
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
            "preco": float(
                candles_5m[-1]["close"]
            ),
            "vela": candles_5m[-1]["_dt"],
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

    ema13_series = ema_series(
        c,
        13
    )

    ema21_series = ema_series(
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

    tendencia_5m = tendencia_timeframe(
        candles_5m
    )

    tendencia_15m = tendencia_timeframe(
        candles_15m
    )

    confirmacao = candle_info(
        candles_5m[-1]
    )

    pullback_1 = candle_info(
        candles_5m[-2]
    )

    pb1_ema13 = ema13_series[-2]
    pb1_ema21 = ema21_series[-2]

    pullback_call = pullback_na_vela(
        pullback_1,
        pb1_ema13,
        pb1_ema21,
        "CALL"
    )

    pullback_put = pullback_na_vela(
        pullback_1,
        pb1_ema13,
        pb1_ema21,
        "PUT"
    )

    # --------------------------------------------------------
    # CONFIRMAÇÃO CALL
    # --------------------------------------------------------

    confirmacao_call = False

    if (
        confirmacao["close"]
        >
        confirmacao["open"]
        and pullback_call
    ):

        rejeicao_inferior = (
            confirmacao["lower_wick"]
            >= confirmacao["body"] * 0.35
            and
            confirmacao["lower_wick"]
            >
            confirmacao["upper_wick"]
        )

        fechamento_forte = (
            confirmacao["body_ratio"]
            >= 0.40
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
            <= 0.30
        )

        rompeu_pullback = (
            confirmacao["close"]
            >
            pullback_1["high"]
        )

        confirmacao_call = (
            (
                rejeicao_inferior
                or fechamento_forte
            )
            and
            rompeu_pullback
        )

    # --------------------------------------------------------
    # CONFIRMAÇÃO PUT
    # --------------------------------------------------------

    confirmacao_put = False

    if (
        confirmacao["close"]
        <
        confirmacao["open"]
        and pullback_put
    ):

        rejeicao_superior = (
            confirmacao["upper_wick"]
            >= confirmacao["body"] * 0.35
            and
            confirmacao["upper_wick"]
            >
            confirmacao["lower_wick"]
        )

        fechamento_forte = (
            confirmacao["body_ratio"]
            >= 0.40
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
            <= 0.30
        )

        rompeu_pullback = (
            confirmacao["close"]
            <
            pullback_1["low"]
        )

        confirmacao_put = (
            (
                rejeicao_superior
                or fechamento_forte
            )
            and
            rompeu_pullback
        )

    # --------------------------------------------------------
    # CONTEXTO
    # --------------------------------------------------------

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
        and movimento_8 > 0
    )

    contexto_put = (
        movimento_4 < 0
        and movimento_8 < 0
    )

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # ATR
    # --------------------------------------------------------

    atr_ok = True

    if (
        atr14 is not None
        and preco != 0
    ):

        atr_ratio = (
            atr14
            /
            preco
        )

        if (
            atr_ratio < 0.00008
            or
            atr_ratio > 0.0035
        ):

            atr_ok = False

    # --------------------------------------------------------
    # LATERAL
    # --------------------------------------------------------

    lateral = mercado_lateral(
        preco,
        ema5,
        ema13,
        ema21,
        atr14
    )

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    score_call = 0
    score_put = 0

    if tendencia_5m == "ALTA":
        score_call += 3

    elif tendencia_5m == "BAIXA":
        score_put += 3

    if tendencia_15m == "ALTA":
        score_call += 2

    elif tendencia_15m == "BAIXA":
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

    # --------------------------------------------------------
    # DECISÃO
    # --------------------------------------------------------

    sinal = "AGUARDAR"

    score = max(
        score_call,
        score_put
    )

    bloqueio = None

    if lateral:

        bloqueio = (
            "Mercado lateral ou tendência fraca."
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
                "5M em alta, mas 15M não confirma."
            )

        elif not pullback_call:

            bloqueio = (
                "Alta alinhada, mas sem pullback válido."
            )

        elif not confirmacao_call:

            bloqueio = (
                "Pullback encontrado, "
                "mas sem confirmação separada."
            )

        elif not rsi_call_ok:

            bloqueio = (
                f"RSI não confirma CALL ({rsi14:.2f})."
            )

        elif not contexto_call:

            bloqueio = (
                "Contexto de movimento não confirma CALL."
            )

        elif score_call < 10:

            bloqueio = (
                "Setup de alta abaixo do score mínimo de 10."
            )

        else:

            sinal = "CALL"

    elif tendencia_5m == "BAIXA":

        if tendencia_15m != "BAIXA":

            bloqueio = (
                "5M em baixa, mas 15M não confirma."
            )

        elif not pullback_put:

            bloqueio = (
                "Baixa alinhada, mas sem pullback válido."
            )

        elif not confirmacao_put:

            bloqueio = (
                "Pullback encontrado, "
                "mas sem confirmação separada."
            )

        elif not rsi_put_ok:

            bloqueio = (
                f"RSI não confirma PUT ({rsi14:.2f})."
            )

        elif not contexto_put:

            bloqueio = (
                "Contexto de movimento não confirma PUT."
            )

        elif score_put < 10:

            bloqueio = (
                "Setup de baixa abaixo do score mínimo de 10."
            )

        else:

            sinal = "PUT"

    else:

        bloqueio = (
            "5M sem tendência clara."
        )

    detalhes_pullback = (
        "CONFIRMADO EM VELA ANTERIOR"
        if (
            (
                pullback_call
                and tendencia_5m == "ALTA"
            )
            or
            (
                pullback_put
                and tendencia_5m == "BAIXA"
            )
        )
        else "NÃO"
    )

    detalhes_confirmacao = (
        "CONFIRMADA"
        if (
            (
                confirmacao_call
                and tendencia_5m == "ALTA"
            )
            or
            (
                confirmacao_put
                and tendencia_5m == "BAIXA"
            )
        )
        else "NÃO"
    )

    if sinal == "CALL":

        mensagem = (
            "CALL FORTE | "
            "5M ALTA + 15M ALTA | "
            "Pullback real | "
            "Confirmação em vela separada | "
            f"Score={score_call}/12 | "
            f"RSI={rsi14:.2f}"
        )

    elif sinal == "PUT":

        mensagem = (
            "PUT FORTE | "
            "5M BAIXA + 15M BAIXA | "
            "Pullback real | "
            "Confirmação em vela separada | "
            f"Score={score_put}/12 | "
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
            f"Confirmação={detalhes_confirmacao} | "
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
        "lateral": (
            "SIM"
            if lateral
            else "NÃO"
        ),
        "rsi_call_ok": rsi_call_ok,
        "rsi_put_ok": rsi_put_ok,
        "contexto_call": contexto_call,
        "contexto_put": contexto_put,
        "confirmacao_call": confirmacao_call,
        "confirmacao_put": confirmacao_put,
        "bloqueio": (
            bloqueio
            or
            "SINAL"
        ),
        "mensagem": mensagem,
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
        wins + losses
    )

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
        and TELEGRAM_CHAT_ID
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
                "chat_id": TELEGRAM_CHAT_ID,
                "text": texto,
            },
            timeout=15,
        )

        resposta.raise_for_status()

        dados = resposta.json()

        if not dados.get("ok"):

            raise RuntimeError(
                str(dados)
            )

        return True

    except Exception as e:

        log(
            f"ERRO Telegram: {e}"
        )

        return False


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
        == chave
    ):

        return

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
        else "🔴"
    )

    texto = (
        f"{emoji} SINAL FOREX 5M\n\n"
        f"Ativo: {symbol}\n"
        f"Direcao: {sinal}\n"
        f"Score: {resultado.get('score', 0)}\n"
        f"Preco: {fmt(resultado.get('preco'))}\n"
        f"Vela: "
        f"{vela.strftime('%Y-%m-%d %H:%M:%S BRT')}\n\n"
        f"5M: "
        f"{resultado.get('tendencia_5m', '-')}\n"
        f"15M: "
        f"{resultado.get('tendencia_15m', '-')}\n"
        f"Pullback: "
        f"{resultado.get('pullback', '-')}\n"
        f"Confirmacao: "
        f"{resultado.get('rejeicao', '-')}\n"
        f"RSI: "
        f"{fmt(resultado.get('rsi'), 2)}\n"
        f"EMA5: "
        f"{fmt(resultado.get('ema5'))}\n"
        f"EMA13: "
        f"{fmt(resultado.get('ema13'))}\n"
        f"EMA21: "
        f"{fmt(resultado.get('ema21'))}\n\n"
        f"➡️ ENTRADA: PRÓXIMA VELA\n"
        f"⏱️ EXPIRAÇÃO: 5 MINUTOS\n"
        f"💰 EXECUÇÃO DEMO: "
        f"{'ATIVA' if BULLEX_AUTO_TRADE else 'DESATIVADA'}"
    )

    if enviar_telegram(texto):

        _ultimos_sinais_telegram[
            symbol
        ] = chave
```
# ============================================================
# EXECUÇÃO AUTOMÁTICA
# ============================================================

def _valor_entrada_atual():
    global _nivel_progressao

    if _nivel_progressao < 0:
        _nivel_progressao = 0

    if _nivel_progressao >= len(
        VALORES_ENTRADA
    ):
        _nivel_progressao = (
            len(VALORES_ENTRADA) - 1
        )

    return float(
        VALORES_ENTRADA[
            _nivel_progressao
        ]
    )


def _atualizar_estado_execucao():

    with _execucao_lock:

        estado[
            "execucao"
        ]["automatica"] = (
            BULLEX_AUTO_TRADE
        )

        estado[
            "execucao"
        ]["modo"] = "DEMO"

        estado[
            "execucao"
        ]["valor_atual"] = (
            _valor_entrada_atual()
        )

        estado[
            "execucao"
        ]["nivel_progressao"] = (
            _nivel_progressao
        )

        estado[
            "execucao"
        ]["operacao_ativa"] = (
            _operacao_global_ativa
            is not None
        )


def _instrument_time():

    agora = agora_brt()

    minuto = (
        agora.minute
        // 5
    ) * 5

    return agora.replace(
        minute=minuto,
        second=0,
        microsecond=0
    )


def _montar_instrument_id(
    active_id,
    dt=None
):

    if dt is None:
        dt = _instrument_time()

    return (
        f"do{int(active_id)}"
        f"{dt.strftime('%Y%m%d')}"
        f"D{dt.strftime('%H%M')}"
        f"T5MPSPT"
    )


def _direcao_bullex(sinal):

    if sinal == "CALL":
        return "call"

    if sinal == "PUT":
        return "put"

    raise ValueError(
        f"Direcao invalida: {sinal}"
    )


# ============================================================
# DESCOBERTA DE INSTRUMENTO
# ============================================================

def _extrair_instrumentos_recursivo(
    obj,
    active_id,
    encontrados=None
):

    if encontrados is None:
        encontrados = []

    if isinstance(obj, dict):

        candidate_id = (
            obj.get("instrument_id")
            or obj.get("id")
        )

        candidate_asset = (
            obj.get("asset_id")
            or obj.get("active_id")
            or obj.get("underlying_id")
        )

        candidate_index = (
            obj.get("instrument_index")
            if obj.get("instrument_index") is not None
            else obj.get("index")
        )

        if (
            candidate_id is not None
            and candidate_index is not None
        ):

            corresponde_ativo = False

            if candidate_asset is not None:

                try:

                    corresponde_ativo = (
                        int(candidate_asset)
                        ==
                        int(active_id)
                    )

                except Exception:

                    corresponde_ativo = (
                        str(candidate_asset)
                        ==
                        str(active_id)
                    )

            if not corresponde_ativo:

                corresponde_ativo = (
                    str(active_id)
                    in str(candidate_id)
                )

            if corresponde_ativo:

                item = dict(obj)

                item["instrument_id"] = str(
                    candidate_id
                )

                try:

                    item["instrument_index"] = int(
                        candidate_index
                    )

                except Exception:
                    pass

                item["asset_id"] = int(
                    active_id
                )

                encontrados.append(
                    item
                )

        for valor in obj.values():

            _extrair_instrumentos_recursivo(
                valor,
                active_id,
                encontrados
            )

    elif isinstance(obj, list):

        for valor in obj:

            _extrair_instrumentos_recursivo(
                valor,
                active_id,
                encontrados
            )

    return encontrados


def _instrumento_eh_5m(
    instrumento,
    instrument_id_esperado
):

    if not isinstance(
        instrumento,
        dict
    ):

        return False

    iid = str(
        instrumento.get(
            "instrument_id",
            instrumento.get(
                "id",
                ""
            )
        )
    )

    if not iid:
        return False

    esperado = str(
        instrument_id_esperado
    )

    if iid == esperado:
        return True

    iid_upper = iid.upper()

    if "T5M" not in iid_upper:
        return False

    return True


def _buscar_instrumento(
    active_id,
    dt_inicio
):

    active_id = int(
        active_id
    )

    instrument_id_esperado = (
        _montar_instrument_id(
            active_id,
            dt_inicio
        )
    )

    cache = _bullex_instrument_cache.get(
        instrument_id_esperado
    )

    if isinstance(
        cache,
        dict
    ):

        indice = cache.get(
            "instrument_index"
        )

        if indice is not None:

            try:

                resultado = {

                    "instrument_id":
                        str(
                            cache.get(
                                "instrument_id",
                                instrument_id_esperado
                            )
                        ),

                    "instrument_index":
                        int(indice),

                    "asset_id":
                        active_id,
                }

                log(
                    "Instrumento encontrado no cache: "
                    f"{resultado}"
                )

                return resultado

            except Exception:
                pass

    consultas = [

        (
            "digital-options.get-instruments",
            "3.0",
            {
                "asset_id": active_id,
                "instrument_type": "digital",
            }
        ),

        (
            "digital-options.get-instruments",
            "2.0",
            {
                "asset_id": active_id,
            }
        ),

    ]

    for nome, versao, body in consultas:

        try:

            log(
                "Consultando instrumentos Bullex: "
                f"{nome} v{versao} "
                f"asset_id={active_id}"
            )

            resposta = _enviar_e_aguardar(
                nome,
                versao,
                body,
                timeout=10
            )

            candidatos = (
                _extrair_instrumentos_recursivo(
                    resposta,
                    active_id
                )
            )

            if not candidatos:

                log(
                    "Bullex não retornou "
                    "instrumentos utilizáveis."
                )

                continue

            # ------------------------------------------------
            # PRIMEIRO: procurar instrumento EXATO
            # ------------------------------------------------

            for item in candidatos:

                iid = str(
                    item.get(
                        "instrument_id",
                        ""
                    )
                )

                if iid != (
                    instrument_id_esperado
                ):
                    continue

                indice = item.get(
                    "instrument_index"
                )

                if indice is None:
                    continue

                try:

                    resultado = {

                        "instrument_id":
                            iid,

                        "instrument_index":
                            int(indice),

                        "asset_id":
                            active_id,
                    }

                    _bullex_instrument_cache[
                        instrument_id_esperado
                    ] = resultado

                    log(
                        "INSTRUMENTO EXATO ENCONTRADO: "
                        f"{resultado}"
                    )

                    return resultado

                except Exception:
                    continue

            # ------------------------------------------------
            # SEGUNDO: procurar qualquer instrumento 5M
            # ------------------------------------------------

            for item in candidatos:

                if not _instrumento_eh_5m(
                    item,
                    instrument_id_esperado
                ):

                    continue

                iid = str(
                    item.get(
                        "instrument_id",
                        item.get(
                            "id",
                            ""
                        )
                    )
                )

                indice = item.get(
                    "instrument_index"
                )

                if indice is None:
                    continue

                try:

                    indice = int(
                        indice
                    )

                except Exception:

                    continue

                resultado = {

                    "instrument_id":
                        iid,

                    "instrument_index":
                        indice,

                    "asset_id":
                        active_id,
                }

                _bullex_instrument_cache[
                    instrument_id_esperado
                ] = resultado

                log(
                    "INSTRUMENTO 5M ENCONTRADO: "
                    f"{resultado}"
                )

                return resultado

        except Exception as e:

            log(
                "Falha ao consultar instrumentos: "
                f"{nome} v{versao}: {e}"
            )

    log(
        "================================"
    )

    log(
        "INSTRUMENTO NÃO ENCONTRADO"
    )

    log(
        f"Ativo={active_id}"
    )

    log(
        f"Instrument esperado="
        f"{instrument_id_esperado}"
    )

    log(
        "A ordem será BLOQUEADA."
    )

    log(
        "================================"
    )

    return None


# ============================================================
# EXECUTAR ORDEM
# ============================================================

def executar_ordem_demo(
    symbol,
    sinal,
    resultado
):

    global _operacao_global_ativa

    if not BULLEX_AUTO_TRADE:

        return {
            "success": False,
            "status": "AUTO_TRADE_DESATIVADO",
        }

    with _execucao_lock:

        if UMA_OPERACAO_GLOBAL:

            if _operacao_global_ativa is not None:

                log(
                    "Ordem bloqueada: "
                    "já existe operação global ativa."
                )

                return {
                    "success": False,
                    "status": "OPERACAO_GLOBAL_ATIVA",
                }

        # ----------------------------------------------------
        # BALANCE ID
        # ----------------------------------------------------

        balance_id = _obter_balance_id()

        if not balance_id:

            estado[
                "execucao"
            ]["ultimo_erro"] = (
                "user_balance_id não encontrado."
            )

            log(
                "ORDEM NAO ENVIADA: "
                "user_balance_id nao encontrado."
            )

            return {
                "success": False,
                "status": "SEM_BALANCE_ID",
            }

        # ----------------------------------------------------
        # ATIVO
        # ----------------------------------------------------

        config = None

        for chave, nome in ATIVOS.items():

            if nome == symbol:

                config = ATIVO_BULLEX[
                    chave
                ]

                break

        if not config:

            return {
                "success": False,
                "status": "ATIVO_NAO_MAPEADO",
            }

        active_id = int(
            config["active_id"]
        )

        # ----------------------------------------------------
        # VALOR
        # ----------------------------------------------------

        valor = _valor_entrada_atual()

        agora = agora_brt()

        inicio = _instrument_time()

        instrument_id = (
            _montar_instrument_id(
                active_id,
                inicio
            )
        )

        direcao = _direcao_bullex(
            sinal
        )

        # ----------------------------------------------------
        # DESCOBRIR INSTRUMENTO REAL
        # ----------------------------------------------------

        instrumento = _buscar_instrumento(
            active_id,
            inicio
        )

        if instrumento is None:

            estado[
                "execucao"
            ]["ultimo_erro"] = (
                "Instrument_index não encontrado "
                f"para {instrument_id}."
            )

            log(
                "ORDEM NAO ENVIADA: "
                f"instrumento não encontrado "
                f"{instrument_id}"
            )

            return {
                "success": False,
                "status": "SEM_INSTRUMENTO",
                "instrument_id":
                    instrument_id,
            }

        instrument_index = int(
            instrumento[
                "instrument_index"
            ]
        )

        # ----------------------------------------------------
        # IMPORTANTE:
        # usar o instrument_id REAL retornado pela Bullex
        # ----------------------------------------------------

        instrument_id_real = str(
            instrumento.get(
                "instrument_id",
                instrument_id
            )
        )

        # ----------------------------------------------------
        # BODY DA ORDEM
        # ----------------------------------------------------

        body = {

            "user_balance_id": str(
                balance_id
            ),

            "instrument_id":
                instrument_id_real,

            "amount": str(
                valor
            ),

            "instrument_index":
                instrument_index,

            "asset_id":
                int(active_id),

            "instrument_dir":
                direcao,
        }

        log(
            "================================"
        )

        log(
            "ENVIANDO ORDEM DEMO"
        )

        log(
            f"Ativo={symbol} | "
            f"Direcao={sinal} | "
            f"Valor=R${valor:.2f} | "
            f"Instrument={instrument_id_real} | "
            f"Index={instrument_index} | "
            f"Asset={active_id}"
        )

        log(
            f"Balance ID={balance_id}"
        )

        try:

            resposta = _enviar_e_aguardar(
                "digital-options.place-digital-option",
                "3.0",
                body,
                timeout=15
            )

            with _bullex_diag_lock:

                _bullex_diag[
                    "orders_sent"
                ] += 1

            # ------------------------------------------------
            # CONFIRMAÇÃO
            # ------------------------------------------------

            sucesso = False

            if isinstance(
                resposta,
                dict
            ):

                msg = resposta.get(
                    "msg"
                )

                if (
                    isinstance(
                        msg,
                        dict
                    )
                    and
                    msg.get(
                        "success"
                    )
                    is True
                ):

                    sucesso = True

                if (
                    resposta.get(
                        "success"
                    )
                    is True
                ):

                    sucesso = True

                texto = json.dumps(
                    resposta,
                    ensure_ascii=False
                ).lower()

                if (
                    "digital-option-placed"
                    in texto
                    or
                    (
                        "success"
                        in texto
                        and
                        "true"
                        in texto
                    )
                ):

                    sucesso = True

            if not sucesso:

                estado[
                    "execucao"
                ]["ultimo_erro"] = (
                    "Resposta sem confirmação: "
                    f"{str(resposta)[:800]}"
                )

                with _bullex_diag_lock:

                    _bullex_diag[
                        "orders_errors"
                    ] += 1

                log(
                    "ORDEM SEM CONFIRMACAO: "
                    f"{resposta}"
                )

                return {
                    "success": False,
                    "status": "SEM_CONFIRMACAO",
                    "response": resposta,
                }

            # ------------------------------------------------
            # OPERAÇÃO GLOBAL ATIVA
            # ------------------------------------------------

            _operacao_global_ativa = {

                "symbol":
                    symbol,

                "sinal":
                    sinal,

                "valor":
                    valor,

                "asset_id":
                    active_id,

                "instrument_id":
                    instrument_id_real,

                "instrument_index":
                    instrument_index,

                "balance_id":
                    str(balance_id),

                "enviada_em":
                    agora,

                "resultado":
                    "PENDENTE",

                "response":
                    resposta,
            }

            # ------------------------------------------------
            # ÚLTIMA ORDEM
            # ------------------------------------------------

            estado[
                "execucao"
            ]["ultima_ordem"] = {

                "symbol":
                    symbol,

                "sinal":
                    sinal,

                "valor":
                    valor,

                "instrument_id":
                    instrument_id_real,

                "instrument_index":
                    instrument_index,

                "enviada_em":
                    agora.strftime(
                        "%Y-%m-%d %H:%M:%S BRT"
                    ),
            }

            estado[
                "execucao"
            ]["ultimo_erro"] = None

            _atualizar_estado_execucao()

            log(
                "ORDEM DEMO CONFIRMADA."
            )

            log(
                f"Instrument ID real: "
                f"{instrument_id_real}"
            )

            log(
                f"Instrument index: "
                f"{instrument_index}"
            )

            return {

                "success":
                    True,

                "status":
                    "CONFIRMADA",

                "instrument_id":
                    instrument_id_real,

                "instrument_index":
                    instrument_index,

                "response":
                    resposta,
            }

        except Exception as e:

            with _bullex_diag_lock:

                _bullex_diag[
                    "orders_errors"
                ] += 1

            estado[
                "execucao"
            ]["ultimo_erro"] = str(e)

            log(
                f"ERRO AO ENVIAR ORDEM DEMO: {e}"
            )

            return {

                "success":
                    False,

                "status":
                    "ERRO",

                "error":
                    str(e),
            }


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

        return None

    vela_sinal = resultado.get(
        "vela"
    )

    if not isinstance(
        vela_sinal,
        datetime
    ):

        return None

    vela_entrada = (
        vela_sinal
        +
        timedelta(minutes=5)
    )

    vela_expiracao = (
        vela_entrada
    )

    chave = (
        f"{symbol}|"
        f"{vela_sinal.isoformat()}"
    )

    if (
        _ultimas_operacoes_registradas.get(
            symbol
        )
        ==
        chave
    ):

        return None

    if (
        UMA_OPERACAO_GLOBAL
        and
        _operacao_global_ativa is not None
    ):

        log(
            f"{symbol}: operação local bloqueada "
            "por operação global ativa."
        )

        return None

    if symbol in _operacoes_pendentes:
        return None

    operacao = {

        "id":
            chave,

        "symbol":
            symbol,

        "sinal":
            sinal,

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

        "ordem_automatica":
            False,

        "valor":
            _valor_entrada_atual(),

        "instrument_id":
            None,

        "instrument_index":
            None,
    }

    _operacoes_pendentes[
        symbol
    ] = operacao

    _ultimas_operacoes_registradas[
        symbol
    ] = chave

    if BULLEX_AUTO_TRADE:

        ordem = executar_ordem_demo(
            symbol,
            sinal,
            resultado
        )

        if ordem.get("success"):

            operacao[
                "ordem_automatica"
            ] = True

            operacao[
                "instrument_id"
            ] = ordem.get(
                "instrument_id"
            )

            operacao[
                "instrument_index"
            ] = ordem.get(
                "instrument_index"
            )

            log(
                f"{symbol}: "
                f"ordem automática confirmada "
                f"{sinal}."
            )

        else:

            del _operacoes_pendentes[
                symbol
            ]

            log(
                f"{symbol}: "
                "operação removida porque "
                "a ordem não foi confirmada."
            )

            return None

    else:

        log(
            f"{symbol}: operação registrada "
            f"{sinal} | modo automático OFF."
        )

    return operacao


# ============================================================
# RESULTADO / PROGRESSÃO
# ============================================================

def atualizar_progressao(
    resultado
):

    global _nivel_progressao

    with _execucao_lock:

        if resultado == "WIN":

            _nivel_progressao = 0

            log(
                "PROGRESSÃO: WIN -> "
                "retornando para R$5,00."
            )

        elif resultado == "LOSS":

            if (
                _nivel_progressao
                <
                len(
                    VALORES_ENTRADA
                ) - 1
            ):

                _nivel_progressao += 1

            else:

                _nivel_progressao = 0

            log(
                "PROGRESSÃO: LOSS -> "
                f"próxima entrada "
                f"R${_valor_entrada_atual():.2f}."
            )

        _atualizar_estado_execucao()


# ============================================================
# AVALIAR WIN / LOSS
# ============================================================

def avaliar_operacao(
    symbol,
    candles
):

    global _operacao_global_ativa

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

        with _execucao_lock:

            if (
                _operacao_global_ativa
                is not None
            ):

                ativo_global = (
                    _operacao_global_ativa.get(
                        "symbol"
                    )
                )

                if ativo_global == symbol:

                    _operacao_global_ativa = None

        if operacao.get(
            "ordem_automatica"
        ):

            atualizar_progressao(
                resultado
            )

        _atualizar_estado_execucao()

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
        f"{emoji} RESULTADO DA OPERAÇÃO\n\n"
        f"Ativo: {operacao['symbol']}\n"
        f"Direção: {operacao['sinal']}\n"
        f"Resultado: {resultado}\n"
        f"Valor: R${operacao.get('valor', 0):.2f}\n\n"
        f"Entrada: {fmt(operacao.get('entrada'))}\n"
        f"Saída: {fmt(operacao.get('saida'))}\n"
        f"Fonte: Bullex\n\n"
        f"📊 ESTATÍSTICAS\n"
        f"Operações: {estatisticas['total']}\n"
        f"Wins: {estatisticas['wins']}\n"
        f"Losses: {estatisticas['losses']}\n"
        f"Dojis: {estatisticas['dojis']}\n"
        f"Taxa: {estatisticas['taxa']:.2f}%\n\n"
        f"💰 PRÓXIMO VALOR: "
        f"R${_valor_entrada_atual():.2f}"
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

        log(
            f"Consultando 15M: {symbol}"
        )

        candles_15m = obter_candles(
            symbol,
            TIMEFRAME_TREND,
            OUTPUTSIZE_15M
        )

        fechadas_15m = (
            somente_velas_fechadas(
                candles_15m,
                15
            )
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

            "bloqueio":
                resultado.get(
                    "bloqueio",
                    "-"
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
            f"preco={estado['preco']} | "
            f"bloqueio={resultado.get('bloqueio', '-')}"
        )

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

        estado[
            "estatisticas"
        ] = calcular_estatisticas()

        _atualizar_estado_execucao()

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


# ============================================================
# HORÁRIO
# ============================================================

def dentro_do_horario():

    hora = agora_brt().hour

    return (
        HORA_INICIO
        <= hora
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

    try:

        _auth_body()

    except Exception as e:

        log(
            f"ERRO: Bullex nao configurada: {e}"
        )

        estado["sinal"] = "AGUARDAR"
        estado["score"] = 0

        estado["mensagem"] = (
            "Configure BULLEX_SSID no Render."
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

    estado[
        "estatisticas"
    ] = calcular_estatisticas()

    _atualizar_estado_execucao()

    log(
        "Leitura concluida."
    )

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
            proxima - agora
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
# LOOP PRINCIPAL
# ============================================================

def loop_robo():

    log(
        "Loop do robo iniciado."
    )

    try:

        _auth_body()

        conectar_bullex()

        log(
            "Thread persistente do WebSocket "
            "Bullex iniciada."
        )

    except Exception as e:

        log(
            f"WebSocket sera iniciado "
            f"sob demanda: {e}"
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
    max-width: 800px;
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

.status-auto {
    text-align: center;
    font-size: 20px;
    font-weight: bold;
    margin: 10px 0 20px;
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

<h3>
Execução automática
</h3>

<div class="linha">
<span>Modo</span>
<span class="valor">
{{ estado.execucao.modo }}
</span>
</div>

<div class="linha">
<span>Automática</span>
<span class="valor">
{{ "ATIVA" if estado.execucao.automatica else "DESATIVADA" }}
</span>
</div>

<div class="linha">
<span>Valor atual</span>
<span class="valor">
R$ {{ "%.2f"|format(estado.execucao.valor_atual) }}
</span>
</div>

<div class="linha">
<span>Nível da progressão</span>
<span class="valor">
{{ estado.execucao.nivel_progressao }}
</span>
</div>

<div class="linha">
<span>Operação ativa</span>
<span class="valor">
{{ "SIM" if estado.execucao.operacao_ativa else "NÃO" }}
</span>
</div>

{% if estado.execucao.ultima_ordem %}

<div class="linha">
<span>Última ordem</span>
<span class="valor">
{{ estado.execucao.ultima_ordem.symbol }}
-
{{ estado.execucao.ultima_ordem.sinal }}
-
R$ {{ "%.2f"|format(estado.execucao.ultima_ordem.valor) }}
</span>
</div>

{% endif %}

{% if estado.execucao.ultimo_erro %}

<div class="linha">
<span>Último erro</span>
<span class="valor">
{{ estado.execucao.ultimo_erro }}
</span>
</div>

{% endif %}

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

<div class="linha">
<span>Bloqueio</span>
<span class="valor">
{{ estado.detalhes.bloqueio }}
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

<strong>
Entrada: próxima vela de 5 minutos
</strong>

<br>

<strong>
Expiração: 5 minutos
</strong>

<br><br>

A execução automática, quando habilitada,
é enviada para a conta configurada no
{{ "DEMO" if estado.execucao.modo == "DEMO" else "modo configurado" }}.

<br><br>

Progressão:

<br>

R$ 5,00 → R$ 10,50 → R$ 23,00

<br><br>

Após WIN:

<br>

R$ 5,00

</div>

</div>

<div class="atualizacao">

Página atualiza automaticamente
a cada 10 segundos.

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

    garantir_robo_iniciado()

    estado[
        "estatisticas"
    ] = calcular_estatisticas()

    _atualizar_estado_execucao()

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

    _atualizar_estado_execucao()

    return jsonify(
        estado
    )


@app.route("/health")
def health():

    _atualizar_estado_execucao()

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
                "em vela separada + "
                "RSI + ATR"
            ),

        "fonte_candles":
            "Bullex",

        "execucao_automatica":
            BULLEX_AUTO_TRADE,

        "modo_execucao":
            "DEMO",

        "valor_entrada_atual":
            _valor_entrada_atual(),

        "nivel_progressao":
            _nivel_progressao,

        "progressao":
            VALORES_ENTRADA,

        "expiracao_minutos":
            EXPIRACAO_MINUTOS,

        "websocket_conectado":
            _bullex_connected,

        "websocket_autenticado":
            _bullex_authenticated,

        "websocket_client_session_id":
            _bullex_client_session_id,

        "websocket_auth_request_id":
            _bullex_auth_request_id,

        "websocket_origin":
            BULLEX_ORIGIN,

        "websocket_ultimo_erro":
            _bullex_last_error,

        "balance_id_disponivel":
            bool(
                _obter_balance_id()
            ),

        "balance_id_origem":
            (
                "ENV"
                if BULLEX_USER_BALANCE_ID
                else
                (
                    "WEBSOCKET"
                    if _bullex_balance_id
                    else "NENHUM"
                )
            ),

        "telegram_configurado":
            telegram_configurado(),

        "operacoes_pendentes":
            len(
                _operacoes_pendentes
            ),

        "operacao_global_ativa":
            _operacao_global_ativa
            is not None,

        "estatisticas":
            calcular_estatisticas(),

        "diagnostico":
            _bullex_diag,

        "versao":
            BULLEX_DIAGNOSTIC_VERSION,
    })


# ============================================================
# EXECUÇÃO
# ============================================================

log(
    "============================================"
)

log(
    f"VERSAO DO APP: "
    f"{BULLEX_DIAGNOSTIC_VERSION}"
)

log(
    f"ATIVOS OTC: "
    f"{list(ATIVO_BULLEX.keys())}"
)

log(
    f"WS: {BULLEX_WS_URL}"
)

log(
    f"AUTO TRADE DEMO: "
    f"{BULLEX_AUTO_TRADE}"
)

log(
    f"PROGRESSAO: "
    f"{VALORES_ENTRADA}"
)

log(
    "============================================"
)


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
