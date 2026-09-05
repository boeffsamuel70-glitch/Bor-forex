import os
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import re
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

# O HAR mostrou Origin: https://bull-ex.com.
# Pode ser sobrescrito pelo Render se necessário.
BULLEX_ORIGIN = os.getenv(
    "BULLEX_ORIGIN",
    "https://bull-ex.com"
).strip()

BULLEX_SSID = os.getenv("BULLEX_SSID", "").strip()

# Mantido por compatibilidade com versões anteriores.
# A autenticação agora é montada no formato real observado no HAR.
BULLEX_AUTH_BODY_JSON = os.getenv(
    "BULLEX_AUTH_BODY_JSON", ""
).strip()

BULLEX_COOKIE = os.getenv("BULLEX_COOKIE", "").strip()

BULLEX_PROTOCOL = int(
    os.getenv("BULLEX_PROTOCOL", "3").strip() or "3"
)

# O HAR fornecido mostrou local_time=9087.
# Deixamos configurável para não prender o valor ao código.
try:
    BULLEX_LOCAL_TIME = int(
        os.getenv("BULLEX_LOCAL_TIME", "9087").strip()
    )
except ValueError:
    BULLEX_LOCAL_TIME = 9087

BULLEX_USER_AGENT = os.getenv(
    "BULLEX_USER_AGENT",
    "Mozilla/5.0 (Linux; Android 10; K) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/152.0.0.0 Mobile Safari/537.36"
).strip()

# ============================================================
# ATIVOS BULLEX
# ============================================================

# Fallback inicial. Depois da autenticação o robô consulta
# automaticamente as listas de instrumentos da Traderoom e substitui
# esta tabela pelos OTC realmente disponíveis na conta.
ATIVO_BULLEX = {
    "EURUSD": {"symbol": "EUR/USD", "active_id": 76, "ticker": "EURUSD-OTC"},
    "EURJPY": {"symbol": "EUR/JPY", "active_id": 79, "ticker": "EURJPY-OTC"},
    "GBPUSD": {"symbol": "GBP/USD", "active_id": 81, "ticker": "GBPUSD-OTC"},
    "USDJPY": {"symbol": "USD/JPY", "active_id": 85, "ticker": "USDJPY-OTC"},
    "GBPJPY": {"symbol": "GBP/JPY", "active_id": 84, "ticker": "GBPJPY-OTC"},
}

_bullex_assets_lock = threading.RLock()
_bullex_assets_detected = False
_bullex_assets_last_error = None
_bullex_assets_updated_at = None
_bullex_assets_source = None

# Mantém a estratégia funcionando imediatamente enquanto a lista automática
# ainda está sendo carregada.
ATIVOS = {
    "EURUSD": "EUR/USD",
    "EURJPY": "EUR/JPY",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "GBPJPY": "GBP/JPY",
}

_BULLEX_CANDLE_SIZES = {"5min": 300, "15min": 900}

_bullex_ws = None
_bullex_ws_lock = threading.RLock()
_bullex_request_lock = threading.Lock()
_bullex_request_counter = 1000

_bullex_connected = False
_bullex_authenticated = False
_bullex_last_error = None

_bullex_candles = {}
_bullex_response_store = {}
_bullex_cv = threading.Condition(_bullex_ws_lock)

_bullex_ws_thread_started = False
_bullex_auth_event = threading.Event()
_bullex_auth_request_id = None
_bullex_client_session_id = None

# ============================================================
# DIAGNOSTICO DA VERSAO DEPLOYADA
# ============================================================
BULLEX_DIAGNOSTIC_VERSION = "OTC-AUTO-FIX-FUNCOES-20260905-03"

_bullex_diag = {
    "messages": 0,
    "generated": 0,
    "responses": 0,
    "stored": 0,
    "last_name": None,
    "last_request_id": None,
    "last_active_id": None,
    "last_size": None,
    "last_keys": [],
}
_bullex_diag_lock = threading.Lock()

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

# Informações expostas no dashboard sobre a descoberta automática.
estado["ativos_info"] = {
    "quantidade": len(ATIVO_BULLEX),
    "status": "FALLBACK / aguardando descoberta",
    "lista": ", ".join(
        f"{cfg['ticker']} (id {cfg['active_id']})"
        for cfg in ATIVO_BULLEX.values()
    ) or "-",
}

_robo_lock = threading.Lock()
_robo_started = False

_ultimos_sinais_telegram = {}
_operacoes_pendentes = {}
_ultimas_operacoes_registradas = {}
_historico_resultados = []

# ============================================================
# UTILITÁRIOS
# ============================================================

def log(msg):
    agora = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
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
                return datetime.strptime(txt, fmt).replace(tzinfo=TZ)
            except Exception:
                pass

    return None


def ordenar_candles(candles):
    resultado = []

    for candle in candles:
        item = dict(candle)
        dt = parse_datetime_candle(item.get("datetime"))

        if dt is not None:
            item["_dt"] = dt
            resultado.append(item)

    resultado.sort(key=lambda x: x["_dt"])
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
# BULLEX - AUTENTICAÇÃO
# ============================================================

def _auth_body():
    """
    Valida a configuração de autenticação.

    O protocolo real observado no HAR NÃO usa este objeto como
    {"name":"sendMessage", ...}. A função é mantida apenas para
    compatibilidade/configuração e retorna a msg interna da autenticação.
    """

    if not BULLEX_SSID:
        raise RuntimeError(
            "Configure BULLEX_SSID no Render. "
            "Nao coloque o segredo no codigo/GitHub."
        )

    return {
        "ssid": BULLEX_SSID,
        "protocol": BULLEX_PROTOCOL,
        "session_id": "",
        "client_session_id": "",
    }


def _next_request_id():
    """
    Gera request_id no formato observado no HAR:
    <unix_seconds>_<numero>.
    """

    global _bullex_request_counter

    with _bullex_request_lock:
        _bullex_request_counter += 1
        contador = _bullex_request_counter

    return f"{int(time.time())}_{contador}"


def _montar_auth_message():
    """
    Monta EXATAMENTE a estrutura principal observada no HAR:

    {
      "name": "authenticate",
      "request_id": "...",
      "local_time": 9087,
      "msg": {
        "ssid": "...",
        "protocol": 3,
        "session_id": "",
        "client_session_id": ""
      }
    }
    """

    if not BULLEX_SSID:
        raise RuntimeError(
            "Configure BULLEX_SSID no Render. "
            "Nao coloque o segredo no codigo/GitHub."
        )

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


def _montar_send_message(nome, version, body=None):
    """
    Mantém o formato anterior para comandos posteriores à autenticação.

    IMPORTANTE:
    authenticate NÃO passa por esta função.
    """

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


# ============================================================
# BULLEX - CANDLES
# ============================================================

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

        # Aceita timestamp em segundos, milissegundos ou nanossegundos.
        if timestamp > 10_000_000_000_000:
            timestamp /= 1_000_000_000
        elif timestamp > 10_000_000_000:
            timestamp /= 1_000

        dt = datetime.fromtimestamp(timestamp, tz=TZ)

        high = item.get("max", item.get("high"))
        low = item.get("min", item.get("low"))

        return {
            "id": item.get("id"),
            "datetime": dt.isoformat(),
            "open": float(item["open"]),
            "high": float(high),
            "low": float(low),
            "close": float(item["close"]),
            "volume": float(item.get("volume", 0) or 0),
        }

    except (
        TypeError,
        ValueError,
        OverflowError,
        KeyError,
    ):
        return None


def _armazenar_candle_ws(active_id, size, item):
    candle = _normalizar_candle_ws(item)

    if candle is None:
        return

    try:
        active_id_int = int(active_id)
        size_int = int(size)
    except (TypeError, ValueError):
        return

    if size_int <= 0:
        return

    candle_id = candle.get("id")

    if candle_id is not None:
        chave = (
            active_id_int,
            size_int,
            str(candle_id),
        )
    else:
        chave = (
            active_id_int,
            size_int,
            candle["datetime"],
        )

    with _bullex_cv:
        bucket = _bullex_candles.setdefault(
            (active_id_int, size_int),
            {}
        )

        bucket[chave] = candle
        _bullex_cv.notify_all()


def _extrair_candles_da_resposta(msg):
    """Extrai candles sem depender exclusivamente do campo name."""
    if not isinstance(msg, dict):
        return []

    conteudo = msg.get("msg")

    # Respostas normais: {name, msg:{...}}.
    if isinstance(conteudo, list):
        return [x for x in conteudo if isinstance(x, dict)]

    if not isinstance(conteudo, dict):
        # Alguns envelopes podem trazer candles diretamente.
        conteudo = msg

    encontrados = []

    for chave in ("candles", "data", "values"):
        valor = conteudo.get(chave)
        if isinstance(valor, list):
            encontrados.extend(
                x for x in valor if isinstance(x, dict)
            )
        elif isinstance(valor, dict) and all(
            k in valor for k in ("open", "close")
        ):
            encontrados.append(valor)

    por_tamanho = conteudo.get("candles_by_size")
    if isinstance(por_tamanho, dict):
        for valor in por_tamanho.values():
            if isinstance(valor, list):
                encontrados.extend(
                    x for x in valor if isinstance(x, dict)
                )
            elif isinstance(valor, dict):
                encontrados.append(valor)

    # Fallback para uma única vela direta no msg.
    if not encontrados and all(
        k in conteudo for k in ("open", "close")
    ):
        encontrados.append(conteudo)

    return encontrados


# ============================================================
# BULLEX - RESPOSTAS
# ============================================================

def _mensagem_indica_auth_sucesso(data):
    """
    O sucesso agora é reconhecido somente no formato real
    observado no HAR:

        name == "authenticated"
        msg is True

    Se houver request_id na resposta, ele precisa corresponder
    ao request_id enviado para authenticate.
    """

    global _bullex_client_session_id

    if not isinstance(data, dict):
        return False

    if data.get("name") != "authenticated":
        return False

    if data.get("msg") is not True:
        return False

    request_id_recebido = data.get("request_id")

    if (
        _bullex_auth_request_id
        and request_id_recebido is not None
        and str(request_id_recebido)
        != str(_bullex_auth_request_id)
    ):
        log(
            "Resposta authenticated recebida, "
            "mas request_id nao corresponde ao authenticate enviado."
        )
        return False

    _bullex_client_session_id = (
        data.get("client_session_id")
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
        palavra in texto
        for palavra in palavras
    )


def _armazenar_candles_resposta(active_id, msg):
    """Armazena respostas candles/first-candles preservando o size."""
    if not isinstance(msg, dict):
        return

    por_tamanho = msg.get("candles_by_size")
    if isinstance(por_tamanho, dict):
        for size_key, valores in por_tamanho.items():
            try:
                size = int(size_key)
            except (TypeError, ValueError):
                continue

            if isinstance(valores, dict):
                valores = [valores]
            if not isinstance(valores, list):
                continue

            for item in valores:
                if isinstance(item, dict):
                    _armazenar_candle_ws(active_id, size, item)

    size = msg.get("size")
    dados = msg.get("candles")
    if size is not None and isinstance(dados, list):
        for item in dados:
            if isinstance(item, dict):
                _armazenar_candle_ws(active_id, size, item)


def _on_bullex_message(ws, raw_message):
    global _bullex_last_error
    global _bullex_authenticated

    try:
        data = json.loads(raw_message)
    except Exception:
        return

    if not isinstance(data, dict):
        return

    # Alguns ambientes entregam JSON encapsulado em "data".
    if isinstance(data.get("data"), str):
        try:
            inner = json.loads(data["data"])
            if isinstance(inner, dict):
                _on_bullex_message(ws, json.dumps(inner))
                return
        except Exception:
            pass

    if isinstance(data.get("data"), dict):
        inner = data["data"]
        if isinstance(inner, dict):
            _on_bullex_message(ws, json.dumps(inner))
            return

    nome = data.get("name")
    request_id = data.get("request_id")
    msg = data.get("msg")

    active_id = None
    size = None
    if isinstance(msg, dict):
        active_id = msg.get("active_id")
        size = msg.get("size")

    with _bullex_diag_lock:
        _bullex_diag["messages"] += 1
        _bullex_diag["last_name"] = nome
        _bullex_diag["last_request_id"] = request_id
        _bullex_diag["last_active_id"] = active_id
        _bullex_diag["last_size"] = size
        _bullex_diag["last_keys"] = list(data.keys())[:25]

    # Log somente mensagens relevantes para não inundar o Render.
    if nome in (
        "authenticated",
        "get-first-candles",
        "first-candles",
        "get-candles",
        "candles",
        "candle-generated",
    ):
        log(
            f"[DIAG WS] name={nome} request_id={request_id} "
            f"active_id={active_id} size={size} "
            f"msg_type={type(msg).__name__} keys={list(data.keys())[:12]}"
        )

    # ========================================================
    # AUTENTICAÇÃO
    # ========================================================

    if _mensagem_indica_auth_sucesso(data):
        _bullex_authenticated = True
        _bullex_auth_event.set()

        session = _bullex_client_session_id
        if session:
            log(
                "Autenticacao Bullex confirmada. "
                f"client_session_id={session}"
            )
        else:
            log("Autenticacao Bullex confirmada.")

        threading.Thread(
            target=_inicializar_ativos_otc,
            daemon=True,
            name="bullex-candle-subscriptions",
        ).start()
        return

    if _mensagem_indica_auth_erro(data):
        _bullex_authenticated = False
        _bullex_last_error = "Bullex recusou a autenticacao."
        _bullex_auth_event.set()
        log("Bullex recusou a autenticacao.")
        return

    # ========================================================
    # CANDLE-GENERATED
    # ========================================================

    if nome == "candle-generated":
        with _bullex_diag_lock:
            _bullex_diag["generated"] += 1

        if isinstance(msg, dict):
            active_id = msg.get("active_id")
            size = msg.get("size")
            if active_id is not None and size is not None:
                _armazenar_candle_ws(active_id, size, msg)
                with _bullex_diag_lock:
                    _bullex_diag["stored"] += 1
        return

    # ========================================================
    # RESPOSTAS / EVENTOS DE CANDLES
    # ========================================================

    dados = _extrair_candles_da_resposta(data)
    is_candle_response = nome in (
        "candles",
        "first-candles",
        "get-candles",
    ) or bool(dados)

    if is_candle_response:
        with _bullex_diag_lock:
            _bullex_diag["responses"] += 1

        qtd = len(dados)
        log(
            f"[DIAG CANDLE] resposta name={nome} "
            f"request_id={request_id} active_id={active_id} "
            f"size={size} qtd={qtd}"
        )

        if isinstance(msg, dict):
            log(
                f"[DIAG CANDLE] chaves_msg={list(msg.keys())[:30]}"
            )

            if isinstance(msg.get("candles_by_size"), dict):
                log(
                    "[DIAG CANDLE] candles_by_size="
                    f"{[(str(k), len(v) if isinstance(v, list) else 1) for k, v in msg['candles_by_size'].items()]}"
                )

        # Guarda a resposta para quem estiver esperando request_id.
        if request_id is not None:
            with _bullex_cv:
                _bullex_response_store[str(request_id)] = data

        # Armazena independentemente de existir request_id.
        if isinstance(msg, dict):
            response_active_id = msg.get("active_id")
            if response_active_id is not None:
                _armazenar_candles_resposta(
                    response_active_id,
                    msg
                )

                for item in dados:
                    if not isinstance(item, dict):
                        continue

                    item_size = item.get("size")
                    if item_size is None:
                        item_size = msg.get("size")

                    # candles_by_size não coloca size dentro de cada item.
                    if item_size is not None:
                        _armazenar_candle_ws(
                            response_active_id,
                            item_size,
                            item
                        )
                        with _bullex_diag_lock:
                            _bullex_diag["stored"] += 1

        with _bullex_cv:
            _bullex_cv.notify_all()
        return

    # Outras respostas continuam disponíveis para chamadas que
    # eventualmente dependam de request_id.
    if request_id is not None:
        with _bullex_cv:
            _bullex_response_store[str(request_id)] = data
            _bullex_cv.notify_all()


def _on_bullex_error(ws, error):
    global _bullex_last_error

    _bullex_last_error = str(error)
    log(f"Bullex WebSocket erro: {error}")

    with _bullex_cv:
        _bullex_cv.notify_all()


def _on_bullex_close(ws, code, reason):
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

    log("Bullex WebSocket conectado.")

    try:
        # IMPORTANTE:
        # authenticate é enviado diretamente no topo.
        auth = _montar_auth_message()

        _bullex_auth_request_id = str(
            auth["request_id"]
        )

        texto = json.dumps(
            auth,
            separators=(",", ":")
        )

        ws.send(texto)

        log(
            "Autenticacao WebSocket enviada "
            f"(request_id={_bullex_auth_request_id}, "
            f"protocol={BULLEX_PROTOCOL}, "
            f"local_time={BULLEX_LOCAL_TIME})."
        )

    except Exception as e:
        _bullex_last_error = str(e)

        log(
            f"Erro ao enviar autenticacao Bullex: {e}"
        )

        _bullex_auth_event.set()


# ============================================================
# BULLEX - THREAD PERSISTENTE
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

            with _bullex_cv:
                _bullex_cv.notify_all()

            log(
                f"Falha no WebSocket Bullex: {e}"
            )

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
            ws = _bullex_ws
        else:
            ws = None

        if not _bullex_ws_thread_started:
            _bullex_ws_thread_started = True

            thread = threading.Thread(
                target=_thread_bullex_ws,
                daemon=True,
                name="bullex-websocket",
            )

            thread.start()

    if ws is not None:
        return ws

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
    """
    Aguarda confirmação REAL do servidor.

    Não considera mais um socket aberto como autenticado.
    """

    limite = time.time() + timeout

    while time.time() < limite:
        if _bullex_authenticated:
            return True

        if (
            not _bullex_connected
            and _bullex_last_error
        ):
            raise RuntimeError(
                "Conexao Bullex fechou antes da "
                "confirmacao da autenticacao: "
                f"{_bullex_last_error}"
            )

        if not _bullex_connected:
            raise RuntimeError(
                "Conexao Bullex fechou antes da "
                "confirmacao da autenticacao."
            )

        restante = limite - time.time()

        if restante <= 0:
            break

        _bullex_auth_event.wait(
            timeout=min(0.5, restante)
        )

    if _bullex_authenticated:
        return True

    if _bullex_last_error:
        raise RuntimeError(
            "Autenticacao Bullex nao confirmada: "
            f"{_bullex_last_error}"
        )

    raise RuntimeError(
        "Timeout aguardando confirmacao explicita "
        "da autenticacao Bullex."
    )


def _montar_subscribe_candle(active_id, size, request_id=None):
    """Monta a assinatura real observada no Traderoom."""
    return {
        "name": "subscribeMessage",
        "request_id": str(request_id or _next_request_id()),
        "local_time": int(time.time() * 1000) % 1_000_000,
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


def _assinar_candle(active_id, size):
    """Assina atualizações em tempo real do candle informado."""
    ws = conectar_bullex()
    _aguardar_autenticacao(timeout=15)

    payload = _montar_subscribe_candle(active_id, size)
    texto = json.dumps(payload, separators=(",", ":"))

    try:
        ws.send(texto)
        log(
            f"Assinatura candle-generated enviada: "
            f"active_id={active_id} size={size}"
        )
    except Exception as e:
        raise RuntimeError(
            f"Falha ao assinar candle-generated "
            f"active_id={active_id} size={size}: {e}"
        )


def _texto_tem_otc(valor):
    """Retorna True quando o valor representa claramente um instrumento OTC."""
    if valor is None:
        return False
    texto = str(valor).strip().upper()
    return (
        "OTC" in texto
        or texto.endswith("-OTC")
        or texto.endswith("_OTC")
        or texto.endswith("/OTC")
    )


def _iter_dicts_recursivo(obj):
    """Percorre listas/dicionários de qualquer formato retornado pela Traderoom."""
    if isinstance(obj, dict):
        yield obj
        for valor in obj.values():
            yield from _iter_dicts_recursivo(valor)
    elif isinstance(obj, list):
        for valor in obj:
            yield from _iter_dicts_recursivo(valor)


def _primeiro_valor(item, chaves):
    for chave in chaves:
        if chave in item and item[chave] not in (None, ""):
            return item[chave]
    return None


def _normalizar_instrumento_otc(item):
    """
    Converte diferentes formatos de underlying_list em:
    codigo -> {symbol, active_id, ticker}.

    A Traderoom pode mudar a posição/nome de campos entre versões.
    Por isso procuramos por aliases conhecidos em vez de depender de
    um único JSON rígido.
    """
    if not isinstance(item, dict):
        return None

    active_id = _primeiro_valor(
        item,
        (
            "active_id",
            "activeId",
            "activeID",
            "asset_id",
            "assetId",
            "instrument_id",
            "instrumentId",
            "id",
        ),
    )

    try:
        active_id = int(active_id)
    except (TypeError, ValueError):
        return None

    ticker = _primeiro_valor(
        item,
        (
            "ticker",
            "ticker_name",
            "tickerName",
            "display_name",
            "displayName",
            "instrument_name",
            "instrumentName",
            "name",
            "symbol",
            "underlying",
            "underlying_name",
            "underlyingName",
        ),
    )

    symbol = _primeiro_valor(
        item,
        (
            "symbol",
            "asset_name",
            "assetName",
            "underlying",
            "underlying_name",
            "underlyingName",
            "name",
        ),
    )

    # Alguns retornos trazem o OTC em um campo e o par em outro.
    campos_texto = [
        item.get(chave)
        for chave in (
            "ticker",
            "ticker_name",
            "tickerName",
            "display_name",
            "displayName",
            "instrument_name",
            "instrumentName",
            "name",
            "symbol",
            "underlying",
            "underlying_name",
            "underlyingName",
            "type",
            "instrument_type",
        )
    ]

    texto_otc = " ".join(
        str(x) for x in campos_texto if x not in (None, "")
    ).upper()

    if "OTC" not in texto_otc:
        return None

    # Ticker é preferido para identificar o ativo OTC.
    ticker_text = str(ticker or symbol or "").strip()
    symbol_text = str(symbol or ticker or "").strip()

    if not ticker_text:
        ticker_text = f"OTC-{active_id}"

    # Se só veio EURUSD-OTC, cria EUR/USD para a estratégia.
    base = ticker_text.upper()
    base = re.sub(r"[^A-Z]", "", base.replace("OTC", ""))
    if len(base) >= 6 and base[:6].isalpha():
        par = base[:6]
        symbol_normalizado = f"{par[:3]}/{par[3:6]}"
    else:
        base2 = re.sub(r"[^A-Z]", "", symbol_text.upper().replace("OTC", ""))
        if len(base2) >= 6:
            symbol_normalizado = f"{base2[:3]}/{base2[3:6]}"
        else:
            symbol_normalizado = symbol_text or ticker_text

    # Código interno estável, derivado do ticker/par.
    codigo_base = re.sub(r"[^A-Z0-9]", "", ticker_text.upper())
    if not codigo_base:
        codigo_base = re.sub(r"[^A-Z0-9]", "", symbol_normalizado.upper())
    codigo = codigo_base
    if not codigo.upper().endswith("OTC"):
        codigo = f"{codigo}OTC"

    return {
        "codigo": codigo,
        "symbol": symbol_normalizado,
        "active_id": active_id,
        "ticker": ticker_text,
        "raw": item,
    }


def _extrair_otcs_da_resposta(resposta):
    """Extrai todos os OTCs de uma resposta, mesmo com envelopes diferentes."""
    encontrados = {}

    for item in _iter_dicts_recursivo(resposta):
        normalizado = _normalizar_instrumento_otc(item)
        if not normalizado:
            continue

        active_id = normalizado["active_id"]
        atual = encontrados.get(active_id)

        # Prefere a ocorrência que tenha ticker explícito com OTC.
        if atual is None:
            encontrados[active_id] = normalizado
        else:
            atual_ticker = str(atual.get("ticker", "")).upper()
            novo_ticker = str(normalizado.get("ticker", "")).upper()
            if "OTC" in novo_ticker and "OTC" not in atual_ticker:
                encontrados[active_id] = normalizado

    resultado = list(encontrados.values())
    resultado.sort(key=lambda x: (x["ticker"].upper(), x["active_id"]))
    return resultado




# ============================================================
# BULLEX - COMANDOS ASSINCRONOS / INICIALIZACAO OTC
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
                    f"Bullex fechou a conexão durante "
                    f"{nome}: {_bullex_last_error}"
                )

            if not _bullex_connected:
                raise RuntimeError(
                    f"Bullex fechou a conexão durante {nome}."
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
        f"Timeout aguardando resposta Bullex: "
        f"{nome} request_id={request_id}"
    )

def _assinar_candles_otc():
    """Assina 5M e 15M dos ativos usados pelo robô."""
    assinaturas = set()

    for config in ATIVO_BULLEX.values():
        active_id = int(config["active_id"])
        for size in (300, 900):
            chave = (active_id, size)
            if chave in assinaturas:
                continue
            assinaturas.add(chave)
            try:
                _assinar_candle(active_id, size)
            except Exception as e:
                log(
                    f"Nao foi possivel assinar candle-generated "
                    f"active_id={active_id} size={size}: {e}"
                )

def _inicializar_ativos_otc():
    """Descobre OTCs e, somente depois, assina os candles."""
    global _bullex_assets_last_error

    try:
        otcs = _descobrir_otcs_automaticamente()

        # Assina somente após a tabela dinâmica estar pronta.
        _assinar_candles_otc()

        log(
            f"[OTC AUTO] Inicialização concluída com {len(otcs)} ativos OTC."
        )
    except Exception as e:
        _bullex_assets_last_error = str(e)
        log(f"[OTC AUTO] ERRO na descoberta automática: {e}")

        # Fallback: não derruba o robô se a lista automática falhar.
        log(
            "[OTC AUTO] Mantendo ativos de fallback já conhecidos "
            "para não interromper o robô."
        )
        try:
            _assinar_candles_otc()
        except Exception as sube:
            log(f"[OTC AUTO] Erro no fallback de assinaturas: {sube}")

def _corpo_lista_instrumentos(nome):
    """
    Corpo exigido pelo serviço de lista de instrumentos.

    A resposta bruta da Bullex confirmou que enviar body vazio para
    digital-option-instruments.get-underlying-list provoca:
    "body unmarshal error" + "EOF".

    Para o serviço digital, a estrutura compatível é informar o tipo
    digital-option. Para o serviço marginal, o endpoint observado já
    responde com body vazio, então mantemos None.
    """
    if nome == "digital-option-instruments.get-underlying-list":
        return {"type": "digital-option"}
    return None


def _consultar_lista_instrumentos(nome, versoes=("2.0", "1.0")):
    """
    Consulta um microserviço de instrumentos usando o body correto para
    cada serviço.
    """
    ultimo_erro = None
    body = _corpo_lista_instrumentos(nome)

    for versao in versoes:
        try:
            resposta = _enviar_e_aguardar(
                nome,
                versao,
                body,
                timeout=12,
            )
            # DIAGNÓSTICO TEMPORÁRIO: imprime a resposta bruta para descobrirmos
            # o formato EXATO usado pela Traderoom. Não altera a lógica de candles.
            try:
                bruto = json.dumps(resposta, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                bruto = repr(resposta)

            # Evita explodir o log do Render, mas preserva uma amostra grande.
            if len(bruto) > 50000:
                bruto_log = bruto[:50000] + "... [TRUNCADO EM 50000 CARACTERES]"
            else:
                bruto_log = bruto

            log(f"[OTC RAW] {nome} v{versao} RESPOSTA={bruto_log}")

            otcs = _extrair_otcs_da_resposta(resposta)
            if otcs:
                log(
                    f"[OTC AUTO] {nome} v{versao}: "
                    f"{len(otcs)} OTC encontrados."
                )
                return resposta, otcs
            log(
                f"[OTC AUTO] {nome} v{versao}: resposta recebida, "
                "mas nenhum OTC foi reconhecido."
            )
        except Exception as e:
            ultimo_erro = e
            log(
                f"[OTC AUTO] Falha em {nome} v{versao}: {e}"
            )

    if ultimo_erro:
        raise ultimo_erro
    return None, []


def _atualizar_ativos_otc(otcs, origem):
    """Substitui ATIVO_BULLEX/ATIVOS pela lista OTC descoberta."""
    global ATIVO_BULLEX
    global ATIVOS
    global _bullex_assets_detected
    global _bullex_assets_last_error
    global _bullex_assets_updated_at
    global _bullex_assets_source

    if not otcs:
        raise RuntimeError("Nenhum ativo OTC foi encontrado na Traderoom.")

    novos_bullex = {}
    novos_ativos = {}

    for item in otcs:
        codigo = item["codigo"]
        # Evita colisão de código caso a Traderoom envie dois registros
        # com o mesmo ticker textual.
        if codigo in novos_bullex:
            codigo = f"{codigo}_{item['active_id']}"

        novos_bullex[codigo] = {
            "symbol": item["symbol"],
            "active_id": int(item["active_id"]),
            "ticker": item["ticker"],
        }
        novos_ativos[codigo] = item["symbol"]

    with _bullex_assets_lock:
        ATIVO_BULLEX = novos_bullex
        ATIVOS = novos_ativos
        _bullex_assets_detected = True
        _bullex_assets_last_error = None
        _bullex_assets_updated_at = agora_brt().isoformat()
        _bullex_assets_source = origem

    log(
        "[OTC AUTO] Ativos carregados automaticamente: "
        + ", ".join(
            f"{cfg['ticker']}={cfg['active_id']}"
            for cfg in novos_bullex.values()
        )
    )

    estado["ativos_info"] = {
        "quantidade": len(novos_bullex),
        "status": "AUTOMÁTICO",
        "lista": ", ".join(
            f"{cfg['ticker']} (id {cfg['active_id']})"
            for cfg in novos_bullex.values()
        ) or "-",
    }


def _descobrir_otcs_automaticamente():
    """
    Descobre OTCs diretamente da lista de instrumentos digitais da Traderoom.

    O endpoint marginal-forex retornado pela Bullex contém Forex normal
    (EURUSD, EURGBP, GBPJPY, etc.) e não deve ser usado como fonte principal
    de OTC. A fonte digital é a que interessa para opções digitais.
    """
    nome_digital = "digital-option-instruments.get-underlying-list"

    try:
        resposta, otcs = _consultar_lista_instrumentos(nome_digital)
        if otcs:
            _atualizar_ativos_otc(
                otcs,
                nome_digital,
            )
            return otcs
    except Exception as e:
        log(f"[OTC AUTO] Falha na fonte digital {nome_digital}: {e}")

    # O endpoint marginal é mantido somente como diagnóstico/suplemento.
    # Ele pode retornar Forex normal e não contém necessariamente os OTCs.
    nome_marginal = "marginal-forex-instruments.get-underlying-list"
    try:
        resposta_marginal, otcs_marginal = _consultar_lista_instrumentos(
            nome_marginal
        )
        if otcs_marginal:
            _atualizar_ativos_otc(
                otcs_marginal,
                nome_marginal,
            )
            return otcs_marginal
    except Exception as e:
        log(f"[OTC AUTO] Fonte marginal indisponível: {e}")

    raise RuntimeError(
        "A Traderoom não retornou nenhum OTC na lista de instrumentos digitais."
    )


# ============================================================
# BULLEX - OBTENÇÃO DE CANDLES
# ============================================================

def _obter_ultimo_id(active_id, size, timeout=15):
    """Obtém o ID mais recente a partir do feed candle-generated.

    IMPORTANTE: no protocolo da Bullex, get-first-candles retorna o
    PRIMEIRO candle disponível para cada tamanho, e não o último.
    Portanto ele não pode ser usado para montar from_id/to_id do
    histórico recente. O último ID vem do candle-generated.
    """
    active_id = int(active_id)
    size = int(size)

    def _ultimo_do_cache():
        with _bullex_cv:
            bucket = _bullex_candles.get((active_id, size), {})
            candles = list(bucket.values())

        ids = []
        for candle in candles:
            try:
                cid = int(candle.get("id"))
            except (TypeError, ValueError, AttributeError):
                continue
            ids.append((cid, candle))

        if not ids:
            return None

        return max(ids, key=lambda x: x[0])

    limite = time.time() + float(timeout)
    while time.time() < limite:
        resultado = _ultimo_do_cache()
        if resultado is not None:
            ultimo_id, candle = resultado
            log(
                f"[DIAG CANDLE] ultimo_id pelo feed: "
                f"active_id={active_id} size={size} "
                f"id={ultimo_id} from={candle.get('from')} "
                f"to={candle.get('to')}"
            )
            return int(ultimo_id)

        with _bullex_cv:
            restante = limite - time.time()
            if restante <= 0:
                break
            _bullex_cv.wait(timeout=min(0.5, restante))

    # Fallback: get-first-candles é útil para descobrir o PRIMEIRO ID
    # disponível, mas não representa o candle atual. Retornamos esse ID
    # somente como último recurso e deixamos isso explícito no log.
    resposta = _enviar_e_aguardar(
        "get-first-candles",
        "1.0",
        {
            "active_id": active_id,
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
    valor = None
    if isinstance(por_tamanho, dict):
        valor = por_tamanho.get(str(size))
        if valor is None:
            valor = por_tamanho.get(size)

    itens = []
    if isinstance(valor, list):
        itens = valor
    elif isinstance(valor, dict):
        itens = [valor]

    ids = []
    for item in itens:
        if not isinstance(item, dict):
            continue
        try:
            ids.append(int(item["id"]))
        except (KeyError, TypeError, ValueError):
            pass

    if ids:
        primeiro_id = min(ids)
        log(
            f"[DIAG CANDLE] AVISO: sem candle-generated para "
            f"active_id={active_id} size={size}; "
            f"get-first-candles forneceu PRIMEIRO id={primeiro_id}."
        )
        return primeiro_id

    dados = _extrair_candles_da_resposta(resposta)
    ids = []
    for item in dados:
        if not isinstance(item, dict):
            continue
        try:
            ids.append(int(item["id"]))
        except (KeyError, TypeError, ValueError):
            pass

    if ids:
        primeiro_id = min(ids)
        log(
            f"[DIAG CANDLE] AVISO: fallback genérico para "
            f"active_id={active_id} size={size}; primeiro_id={primeiro_id}."
        )
        return primeiro_id

    raise RuntimeError(
        f"Nao foi possivel descobrir o ID do candle para "
        f"active_id={active_id}, size={size}."
    )

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

    size = _BULLEX_CANDLE_SIZES.get(
        interval
    )

    if size is None:
        raise RuntimeError(
            f"Timeframe nao suportado: {interval}"
        )

    cache = _candidatos_candles_cache(
        active_id,
        size
    )

    cache = ordenar_candles(cache)

    if len(cache) >= outputsize:
        return cache[-int(outputsize):]

    ultimo_id = _obter_ultimo_id(
        active_id,
        size,
        timeout=15,
    )

    # candle-generated normalmente aponta para a vela corrente (phase T).
    # Como o histórico solicitado usa only_closed=true, ela deve ficar fora
    # do intervalo. A Traderoom confirma esse comportamento no HAR.
    to_id = int(ultimo_id)
    cache_atual = _candidatos_candles_cache(active_id, size)
    for candle in cache_atual:
        try:
            if int(candle.get("id")) == int(ultimo_id):
                if candle.get("phase") == "T":
                    to_id = max(1, int(ultimo_id) - 1)
                break
        except (TypeError, ValueError, AttributeError):
            pass

    from_id = max(
        1,
        to_id - int(outputsize) + 1
    )

    log(
        f"[DIAG CANDLE] get-candles solicitado: "
        f"active_id={active_id} size={size} "
        f"from_id={from_id} to_id={to_id} "
        f"outputsize={outputsize}"
    )

    resposta = _enviar_e_aguardar(
        "get-candles",
        "2.0",
        {
            "active_id": int(active_id),
            "size": int(size),
            "from_id": int(from_id),
            "to_id": int(to_id),
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
        normalizado = _normalizar_candle_ws(
            item
        )

        if normalizado is None:
            continue

        _armazenar_candle_ws(
            active_id,
            size,
            item
        )

        candles.append(
            normalizado
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

    for i in range(
        period,
        len(ganhos)
    ):
        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + ganhos[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + perdas[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = (
        avg_gain / avg_loss
    )

    return (
        100
        -
        (
            100 / (1 + rs)
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
        - max(
            abertura,
            fechamento
        )
    )

    pavio_inferior = (
        min(
            abertura,
            fechamento
        )
        - minima
    )

    body_ratio = (
        corpo / range_vela
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
            preco - referencia
        )
        /
        abs(referencia)
    )


# ============================================================
# TENDÊNCIA
# ============================================================

def tendencia_timeframe(candles):
    if len(candles) < 40:
        return "NEUTRA"

    valores = closes(candles)

    ema5 = ema(valores, 5)
    ema13 = ema(valores, 13)
    ema21 = ema(valores, 21)

    if not (
        ema5
        and ema13
        and ema21
    ):
        return "NEUTRA"

    if (
        ema5 > ema13 > ema21
    ):
        return "ALTA"

    if (
        ema5 < ema13 < ema21
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
        <= ema13
        <= info["high"]
    )

    tocou_ema21 = (
        info["low"]
        <= ema21
        <= info["high"]
    )

    perto_ema13 = (
        percentual_distancia(
            info["low"],
            ema13
        )
        <= 0.0012
    )

    perto_ema21 = (
        percentual_distancia(
            info["low"],
            ema21
        )
        <= 0.0012
    )

    return (
        tocou_ema13
        or tocou_ema21
        or perto_ema13
        or perto_ema21
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
        <= ema13
        <= info["high"]
    )

    tocou_ema21 = (
        info["low"]
        <= ema21
        <= info["high"]
    )

    perto_ema13 = (
        percentual_distancia(
            info["high"],
            ema13
        )
        <= 0.0012
    )

    perto_ema21 = (
        percentual_distancia(
            info["high"],
            ema21
        )
        <= 0.0012
    )

    return (
        tocou_ema13
        or tocou_ema21
        or perto_ema13
        or perto_ema21
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
            ema5 - ema21
        )
        / preco
    )

    if distancia_5_21 < 0.00025:
        return True

    if atr14:
        atr_ratio = (
            atr14 / preco
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

    c = closes(candles_5m)

    preco = c[-1]

    ema5 = ema(c, 5)
    ema13 = ema(c, 13)
    ema21 = ema(c, 21)

    rsi14 = rsi(c, 14)
    atr14 = atr(candles_5m, 14)

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

    pullback_2 = candle_info(
        candles_5m[-3]
    )

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

    if (
        confirmacao["close"]
        >
        confirmacao["open"]
    ):
        rejeicao_inferior = (
            confirmacao["lower_wick"]
            >= confirmacao["body"] * 0.40
            and
            confirmacao["lower_wick"]
            >
            confirmacao["upper_wick"]
        )

        fechamento_forte = (
            confirmacao["body_ratio"]
            >= 0.45
            and
            (
                (
                    confirmacao["high"]
                    - confirmacao["close"]
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

    if (
        confirmacao["close"]
        <
        confirmacao["open"]
    ):
        rejeicao_superior = (
            confirmacao["upper_wick"]
            >= confirmacao["body"] * 0.40
            and
            confirmacao["upper_wick"]
            >
            confirmacao["lower_wick"]
        )

        fechamento_forte = (
            confirmacao["body_ratio"]
            >= 0.45
            and
            (
                (
                    confirmacao["close"]
                    - confirmacao["low"]
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

    movimento_4 = (
        c[-1] - c[-4]
    )

    movimento_8 = (
        c[-1] - c[-8]
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

    if (
        atr14 is not None
        and
        preco != 0
    ):
        atr_ratio = (
            atr14 / preco
        )

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

def enviar_sinal_telegram(
    symbol,
    resultado
):
    sinal = resultado.get("sinal")

    if sinal not in (
        "CALL",
        "PUT"
    ):
        return

    vela = resultado.get("vela")

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
            f"{symbol}: sinal duplicado ignorado."
        )
        return

    rsi_valor = resultado.get("rsi")

    def fmt(valor, casas=5):
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
    sinal = resultado.get("sinal")

    if sinal not in (
        "CALL",
        "PUT"
    ):
        return

    vela_sinal = resultado.get("vela")

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

    if (
        _ultimas_operacoes_registradas.get(
            symbol
        )
        ==
        chave
    ):
        log(
            f"{symbol}: operacao duplicada "
            "para a mesma vela ignorada."
        )
        return

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
        "preco_sinal": float(
            resultado["preco"]
        ),
        "vela_sinal": vela_sinal,
        "vela_entrada": vela_entrada,
        "vela_expiracao": vela_expiracao,
        "entrada": None,
        "saida": None,
        "resultado": "PENDENTE",
    }

    _operacoes_pendentes[
        symbol
    ] = operacao

    _ultimas_operacoes_registradas[
        symbol
    ] = chave

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
        f"Ativo: {operacao['symbol']}\n"
        f"Direcao: {operacao['sinal']}\n"
        f"Resultado: {resultado}\n\n"
        f"Entrada: {fmt(operacao.get('entrada'))}\n"
        f"Saida: {fmt(operacao.get('saida'))}\n"
        f"Fonte da vela: Bullex\n\n"
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
            if isinstance(
                preco,
                (float, int)
            )
            else "-"
        )

        vela = resultado.get("vela")

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
    log("================================")
    log("INICIANDO LEITURA")
    log("================================")

    try:
        _auth_body()
    except Exception as e:
        log(
            f"ERRO: autenticacao Bullex nao configurada: {e}"
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
            agora + timedelta(hours=1)
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

    time.sleep(segundos)


# ============================================================
# LOOP
# ============================================================

def loop_robo():
    log(
        "Loop do robo iniciado."
    )

    try:
        _auth_body()

        conectar_bullex()

        log(
            "Thread persistente do WebSocket Bullex iniciada."
        )

    except Exception as e:
        log(
            f"WebSocket sera iniciado sob demanda: {e}"
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
OTC detectados automaticamente
</h3>

<div class="linha">
<span>Quantidade</span>
<span class="valor">
{{ estado.ativos_info.quantidade }}
</span>
</div>

<div class="linha">
<span>Status</span>
<span class="valor">
{{ estado.ativos_info.status }}
</span>
</div>

<div class="observacao">
{{ estado.ativos_info.lista }}
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
        "bot_iniciado": _robo_started,
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
        "telegram_configurado":
            telegram_configurado(),
        "operacoes_pendentes":
            len(_operacoes_pendentes),
        "estatisticas":
            calcular_estatisticas(),
        "otc_automatico": {
            "detectado": _bullex_assets_detected,
            "quantidade": len(ATIVO_BULLEX),
            "atualizado_em": _bullex_assets_updated_at,
            "fonte": _bullex_assets_source,
            "erro": _bullex_assets_last_error,
            "ativos": [
                {
                    "codigo": codigo,
                    "symbol": config["symbol"],
                    "ticker": config["ticker"],
                    "active_id": config["active_id"],
                }
                for codigo, config in ATIVO_BULLEX.items()
            ],
        },
    })


# ============================================================
# EXECUÇÃO
# ============================================================

log(
    f"VERSAO DO APP: {BULLEX_DIAGNOSTIC_VERSION} | "
    f"ATIVOS={list(ATIVO_BULLEX.keys())} | "
    f"WS={BULLEX_WS_URL}"
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