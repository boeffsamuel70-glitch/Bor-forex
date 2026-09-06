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

ATIVO_BULLEX = {
    "EURUSD": {"symbol": "EUR/USD", "active_id": 76, "ticker": "EURUSD-OTC"},
    "EURJPY": {"symbol": "EUR/JPY", "active_id": 79, "ticker": "EURJPY-OTC"},
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
BULLEX_DIAGNOSTIC_VERSION = "OTC-AUTO-DEMO-5M-1S-3S-20260906"

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
# EXECUÇÃO AUTOMÁTICA - DEMO
# ============================================================

BULLEX_AUTO_TRADE = os.getenv(
    "BULLEX_AUTO_TRADE",
    "true"
).strip().lower() in ("1", "true", "yes", "sim", "on")

BULLEX_USER_BALANCE_ID = os.getenv(
    "BULLEX_USER_BALANCE_ID",
    ""
).strip()

VALORES_ENTRADA = [5.00, 10.50, 23.00]
EXPIRACAO_MINUTOS = 5
MAX_ATRASO_ENTRADA_SEGUNDOS = 3
DELAY_MINIMO_ENTRADA_SEGUNDOS = 1.0
UMA_OPERACAO_GLOBAL = True

# ============================================================
# ATIVOS
# ============================================================

ATIVOS = {
    "EURUSD": "EUR/USD",
    "EURJPY": "EUR/JPY",
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
        "bloqueio": "-",
    },

    "estatisticas": {
        "total": 0,
        "wins": 0,
        "losses": 0,
        "dojis": 0,
        "taxa": 0.0,
    },
}

estado["execucao"] = {
    "automatica": BULLEX_AUTO_TRADE,
    "modo": "DEMO",
    "valor_atual": VALORES_ENTRADA[0],
    "nivel_progressao": 0,
    "operacao_ativa": False,
    "ultima_ordem": None,
    "ultimo_erro": None,
    "balance_id_disponivel": bool(BULLEX_USER_BALANCE_ID),
    "balance_source": "ENV" if BULLEX_USER_BALANCE_ID else None,
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
_bullex_balance_id = None
_bullex_balance_source = None
_bullex_instrument_cache = {}

# ============================================================
# HORÁRIO DO SERVIDOR / JANELA DE ENTRADA 5M
# ============================================================

_bullex_server_timestamp = None
_bullex_server_timestamp_received_at = None
_bullex_server_time_lock = threading.Lock()


def _normalizar_timestamp_servidor(value):
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None

    if ts > 10**18:
        ts /= 1e9
    elif ts > 10**15:
        ts /= 1e6
    elif ts > 10**12:
        ts /= 1e3

    if 1_000_000_000 <= ts <= 5_000_000_000:
        return ts

    return None


def _atualizar_horario_servidor(data):
    global _bullex_server_timestamp
    global _bullex_server_timestamp_received_at

    if not isinstance(data, dict):
        return

    candidatos = []
    msg = data.get("msg")

    if isinstance(msg, (int, float, str)):
        candidatos.append(msg)
    elif isinstance(msg, dict):
        for key in (
            "server_timestamp",
            "serverTime",
            "timestamp",
            "time",
            "ts",
        ):
            if key in msg:
                candidatos.append(msg.get(key))

    for key in (
        "server_timestamp",
        "serverTime",
        "timestamp",
        "time",
        "ts",
    ):
        if key in data:
            candidatos.append(data.get(key))

    for value in candidatos:
        ts = _normalizar_timestamp_servidor(value)
        if ts is None:
            continue

        with _bullex_server_time_lock:
            _bullex_server_timestamp = ts
            _bullex_server_timestamp_received_at = time.time()

        return


def _horario_servidor_atual():
    with _bullex_server_time_lock:
        ts = _bullex_server_timestamp
        received_at = _bullex_server_timestamp_received_at

    if ts is not None and received_at is not None:
        return (
            ts + max(0.0, time.time() - received_at),
            "TIMESYNC",
        )

    return time.time(), "LOCAL_FALLBACK"


def _janela_execucao_5m():
    server_ts, source = _horario_servidor_atual()

    current = int(server_ts)
    candle_open = current - (current % 300)
    candle_close = candle_open + 300
    atraso = max(0.0, server_ts - candle_open)

    return {
        "server_ts": server_ts,
        "source": source,
        "candle_open": int(candle_open),
        "candle_close": int(candle_close),
        "atraso_segundos": float(atraso),
        "permitida": atraso <= MAX_ATRASO_ENTRADA_SEGUNDOS,
    }


def _mensagem_erro_ordem(resposta):
    if not isinstance(resposta, dict):
        return str(resposta)

    msg = resposta.get("msg")

    if isinstance(msg, dict):
        for key in ("message", "error", "reason", "msg"):
            value = msg.get(key)
            if value not in (None, ""):
                return str(value)

    for key in ("message", "error", "reason"):
        value = resposta.get(key)
        if value not in (None, ""):
            return str(value)

    return ""


def _ordem_option_confirmada(resposta):
    if not isinstance(resposta, dict):
        return False

    msg = resposta.get("msg")

    if isinstance(msg, dict):
        if msg.get("success") is True:
            return True
        if msg.get("id") not in (None, "", 0):
            return True

    if resposta.get("success") is True:
        return True

    bruto = json.dumps(
        resposta,
        ensure_ascii=False,
    ).lower()

    return (
        '"success":true' in bruto
        or "digital-option-placed" in bruto
        or "option-placed" in bruto
    )


_bullex_diag.update({
    "orders_sent": 0,
    "orders_confirmed": 0,
    "orders_errors": 0,
})

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
# BULLEX - BALANCE DEMO / EXECUÇÃO
# ============================================================

def _valor_entrada_atual():
    global _nivel_progressao
    _nivel_progressao = max(0, min(_nivel_progressao, len(VALORES_ENTRADA) - 1))
    return float(VALORES_ENTRADA[_nivel_progressao])


def _atualizar_estado_execucao():
    estado["execucao"].update({
        "automatica": BULLEX_AUTO_TRADE,
        "modo": "DEMO",
        "valor_atual": _valor_entrada_atual(),
        "nivel_progressao": _nivel_progressao,
        "operacao_ativa": _operacao_global_ativa is not None,
        "balance_id_disponivel": _bullex_balance_id is not None,
        "balance_source": _bullex_balance_source,
    })


def _extrair_balance_id(obj):
    """Procura exclusivamente uma conta DEMO (type=4), sem escolher saldo real por engano."""
    encontrados_demo = []
    encontrados_explicitos = []

    def walk(value):
        if isinstance(value, dict):
            for key in ("user_balance_id", "userBalanceId", "balance_id", "balanceId"):
                val = value.get(key)
                if val not in (None, ""):
                    encontrados_explicitos.append(str(val))

            if value.get("id") not in (None, ""):
                tipo = value.get("type")
                if str(tipo) == "4":
                    encontrados_demo.append(str(value["id"]))

            for v in value.values():
                walk(v)
        elif isinstance(value, list):
            for v in value:
                walk(v)

    walk(obj)

    # A variável de ambiente sempre tem prioridade.
    if BULLEX_USER_BALANCE_ID:
        return str(BULLEX_USER_BALANCE_ID), "ENV"

    # Se o servidor entregar explicitamente user_balance_id, aceitamos.
    if encontrados_explicitos:
        return encontrados_explicitos[0], "RESPONSE_EXPLICIT"

    # Fallback seguro: somente type=4 (DEMO).
    if encontrados_demo:
        return encontrados_demo[0], "DEMO_TYPE_4"

    return None, None


def _solicitar_balance_id_demo():
    global _bullex_balance_id, _bullex_balance_source, _bullex_last_error

    if BULLEX_USER_BALANCE_ID:
        _bullex_balance_id = str(BULLEX_USER_BALANCE_ID)
        _bullex_balance_source = "ENV"
        _atualizar_estado_execucao()
        log(f"Balance DEMO definido por BULLEX_USER_BALANCE_ID: {_bullex_balance_id}")
        return _bullex_balance_id

    try:
        resposta = _enviar_e_aguardar("get-balances", "1.0", None, timeout=15)
        balance_id, source = _extrair_balance_id(resposta)
        if balance_id is None:
            log("[BALANCE] Nenhum user_balance_id DEMO (type=4) encontrado.")
            return None

        _bullex_balance_id = str(balance_id)
        _bullex_balance_source = source
        _atualizar_estado_execucao()
        log(f"Balance DEMO encontrado: id={_bullex_balance_id} fonte={source}")
        return _bullex_balance_id
    except Exception as e:
        _bullex_last_error = str(e)
        log(f"[BALANCE] Falha ao consultar get-balances: {e}")
        return None


def _obter_balance_id():
    if _bullex_balance_id:
        return str(_bullex_balance_id)
    return _solicitar_balance_id_demo()


def _instrument_time():
    agora = agora_brt()
    minuto = (agora.minute // 5) * 5
    return agora.replace(minute=minuto, second=0, microsecond=0)


def _montar_instrument_id(active_id, dt=None):
    if dt is None:
        dt = _instrument_time()
    return f"do{int(active_id)}{dt.strftime('%Y%m%d')}D{dt.strftime('%H%M')}T5MPSPT"


def _extrair_instrumentos_recursivo(obj, active_id, out=None):
    if out is None:
        out = []
    if isinstance(obj, dict):
        aid = obj.get("asset_id", obj.get("active_id", obj.get("underlying_id")))
        iid = obj.get("instrument_id", obj.get("instrumentId", obj.get("id")))
        idx = obj.get("instrument_index", obj.get("instrumentIndex", obj.get("index")))
        if iid is not None and (aid is None or str(aid) == str(active_id)):
            out.append({"instrument_id": str(iid), "instrument_index": idx, "asset_id": aid or active_id})
        for v in obj.values():
            _extrair_instrumentos_recursivo(v, active_id, out)
    elif isinstance(obj, list):
        for v in obj:
            _extrair_instrumentos_recursivo(v, active_id, out)
    return out


def _instrumento_eh_5m(item, expected_id):
    iid = str(item.get("instrument_id", ""))
    return iid == expected_id or "T5M" in iid.upper()


def _buscar_instrumento(active_id, dt=None):
    expected = _montar_instrument_id(active_id, dt)
    cache_key = (int(active_id), expected)
    cached = _bullex_instrument_cache.get(cache_key)
    if cached:
        return cached

    for version, body in (("3.0", {"asset_id": int(active_id), "instrument_type": "digital"}),
                          ("2.0", {"asset_id": int(active_id)})):
        try:
            resposta = _enviar_e_aguardar("digital-options.get-instruments", version, body, timeout=15)
            candidatos = [x for x in _extrair_instrumentos_recursivo(resposta, active_id)
                          if _instrumento_eh_5m(x, expected)]
            if not candidatos:
                continue
            escolhido = next((x for x in candidatos if x["instrument_id"] == expected), candidatos[0])
            _bullex_instrument_cache[cache_key] = escolhido
            return escolhido
        except Exception as e:
            log(f"[INSTRUMENT] Falha get-instruments v{version} active_id={active_id}: {e}")

    return None


def _direcao_instrumento(sinal):
    return "call" if sinal == "CALL" else "put"


def executar_ordem_demo(symbol, sinal, resultado):
    global _operacao_global_ativa
    global _bullex_last_error

    if not BULLEX_AUTO_TRADE:
        return None

    if sinal not in ("CALL", "PUT"):
        return None

    with _execucao_lock:
        if (
            UMA_OPERACAO_GLOBAL
            and _operacao_global_ativa is not None
        ):
            log(
                "[AUTO DEMO] Ordem bloqueada: "
                "já existe operação global ativa em "
                f"{_operacao_global_ativa.get('symbol')}."
            )
            return "BLOQUEADA_GLOBAL"

    balance_id = _obter_balance_id()

    if not balance_id:
        estado["execucao"]["ultimo_erro"] = (
            "user_balance_id DEMO não encontrado."
        )
        _atualizar_estado_execucao()

        log(
            "[AUTO DEMO] user_balance_id DEMO "
            "não encontrado."
        )

        return "SEM_BALANCE_ID"

    config = next(
        (
            cfg
            for cfg in ATIVO_BULLEX.values()
            if cfg["symbol"] == symbol
        ),
        None,
    )

    if not config:
        return "SEM_ATIVO"

    active_id = int(config["active_id"])
    ticker = config.get("ticker")
    valor = _valor_entrada_atual()

    janela = _janela_execucao_5m()
    atraso = float(janela["atraso_segundos"])

    # Evita enviar exatamente no instante 00.000 da nova vela.
    # A Bullex pode ainda estar liberando a nova janela de compra.
    if atraso < DELAY_MINIMO_ENTRADA_SEGUNDOS:
        espera = DELAY_MINIMO_ENTRADA_SEGUNDOS - atraso
        log(
            f"[AUTO DEMO V4] Aguardando {espera:.3f}s "
            "para liberação técnica da nova vela."
        )
        time.sleep(espera)

        # Recalcula após a espera para garantir que continuamos
        # dentro da janela máxima de 3 segundos.
        janela = _janela_execucao_5m()
        atraso = float(janela["atraso_segundos"])

    candle_open_dt = datetime.fromtimestamp(
        janela["candle_open"],
        TZ,
    )
    candle_close_dt = datetime.fromtimestamp(
        janela["candle_close"],
        TZ,
    )

    if not janela["permitida"]:
        estado["execucao"]["ultimo_erro"] = (
            "Entrada bloqueada por atraso: "
            f"{atraso:.3f}s > "
            f"{MAX_ATRASO_ENTRADA_SEGUNDOS}s."
        )

        _atualizar_estado_execucao()

        log(
            "[AUTO DEMO] ATRASADA - ordem NÃO enviada | "
            f"{symbol} {sinal} | atraso={atraso:.3f}s | "
            f"vela={candle_open_dt.strftime('%H:%M:%S')} -> "
            f"{candle_close_dt.strftime('%H:%M:%S')}"
        )

        return "ATRASADA"

    body = {
        "user_balance_id": int(balance_id),
        "active_id": active_id,
        "option_type_id": 3,
        "direction": _direcao_instrumento(sinal),
        "expired": int(janela["candle_close"]),
        "price": float(valor),
        "refund_value": 0,
    }

    log(
        "[AUTO DEMO V4] Enviando ordem: "
        f"{symbol} ({ticker}) {sinal} "
        f"valor={valor:.2f} "
        f"atraso={atraso:.3f}s "
        f"expira={candle_close_dt.strftime('%H:%M:%S')} "
        f"fonte_horario={janela['source']}"
    )

    log(
        "[AUTO DEMO V4] body="
        + json.dumps(
            body,
            ensure_ascii=False,
        )
    )

    try:
        with _bullex_diag_lock:
            _bullex_diag["orders_sent"] += 1

        resposta = _enviar_e_aguardar(
            "binary-options.open-option",
            "1.0",
            body,
            timeout=20,
        )

        sucesso = _ordem_option_confirmada(
            resposta
        )

        if not sucesso:
            with _bullex_diag_lock:
                _bullex_diag["orders_errors"] += 1

            mensagem = _mensagem_erro_ordem(
                resposta
            )

            estado["execucao"]["ultimo_erro"] = (
                mensagem
                or f"Ordem não confirmada: {resposta}"
            )

            _atualizar_estado_execucao()

            log(
                "[AUTO DEMO V4] Ordem não confirmada: "
                + json.dumps(
                    resposta,
                    ensure_ascii=False,
                )
            )

            return "SEM_CONFIRMACAO"

        with _bullex_diag_lock:
            _bullex_diag["orders_confirmed"] += 1

        msg = resposta.get("msg")
        option_id = None

        if isinstance(msg, dict):
            option_id = msg.get("id")

        _operacao_global_ativa = {
            "symbol": symbol,
            "ticker": ticker,
            "sinal": sinal,
            "valor": valor,
            "asset_id": active_id,
            "balance_id": str(balance_id),
            "option_id": option_id,
            "option_type_id": 3,
            "expired": int(janela["candle_close"]),
            "expiracao": candle_close_dt.isoformat(),
            "candle_open": candle_open_dt.isoformat(),
            "atraso_segundos": round(atraso, 3),
            "fonte_horario": janela["source"],
            "enviada_em": agora_brt().isoformat(),
            "resultado": "PENDENTE",
            "response": resposta,
        }

        estado["execucao"]["ultima_ordem"] = (
            _operacao_global_ativa.copy()
        )

        estado["execucao"]["ultimo_erro"] = None

        _atualizar_estado_execucao()

        log(
            "[AUTO DEMO V4] ORDEM CONFIRMADA: "
            f"{symbol} {sinal} "
            f"R${valor:.2f} "
            f"id={option_id} "
            f"expira={candle_close_dt.strftime('%H:%M:%S')}"
        )

        return "CONFIRMADA"

    except Exception as e:
        with _bullex_diag_lock:
            _bullex_diag["orders_errors"] += 1

        _bullex_last_error = str(e)
        estado["execucao"]["ultimo_erro"] = str(e)

        _atualizar_estado_execucao()

        log(
            "[AUTO DEMO V4] ERRO ao enviar ordem: "
            f"{e}"
        )

        return "ERRO"


def _atualizar_progressao(resultado):
    global _nivel_progressao
    if resultado == "WIN":
        _nivel_progressao = 0
    elif resultado == "LOSS":
        if _nivel_progressao < len(VALORES_ENTRADA) - 1:
            _nivel_progressao += 1
        else:
            _nivel_progressao = 0
    _atualizar_estado_execucao()


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
            "phase": item.get("phase"),
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
            target=_solicitar_balance_id_demo,
            daemon=True,
            name="bullex-balance",
        ).start()

        threading.Thread(
            target=_assinar_candles_otc,
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


def ema_series(values, period):
    """Retorna a EMA alinhada a cada candle disponível."""
    if len(values) < period:
        return [None] * len(values)

    k = 2 / (period + 1)
    resultado = [None] * (period - 1)
    valor = sum(values[:period]) / period
    resultado.append(valor)

    for preco in values[period:]:
        valor = preco * k + valor * (1 - k)
        resultado.append(valor)

    return resultado


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

def pullback_na_vela(info, ema13, ema21, direcao):
    """Detecta pullback real perto das EMAs, alinhado à direção.

    CALL: a vela de pullback deve testar EMA13/EMA21 sem ser uma vela
    fortemente compradora.
    PUT: inverso.
    """
    if not info or ema13 is None or ema21 is None:
        return False

    if direcao == "CALL":
        referencias = (ema13, ema21)
        toque = any(
            info["low"] <= ref <= info["high"]
            for ref in referencias
        )
        proximidade = min(
            percentual_distancia(info["low"], ref)
            for ref in referencias
        ) <= 0.0007

        # Evita chamar uma vela de impulso forte de "pullback".
        vela_retracao = (
            info["close"] <= info["open"]
            or info["body_ratio"] <= 0.55
        )
        return (toque or proximidade) and vela_retracao

    if direcao == "PUT":
        referencias = (ema13, ema21)
        toque = any(
            info["low"] <= ref <= info["high"]
            for ref in referencias
        )
        proximidade = min(
            percentual_distancia(info["high"], ref)
            for ref in referencias
        ) <= 0.0007

        vela_retracao = (
            info["close"] >= info["open"]
            or info["body_ratio"] <= 0.55
        )
        return (toque or proximidade) and vela_retracao

    return False


# Compatibilidade com chamadas antigas.
def pullback_call_na_vela(info, ema13, ema21):
    return pullback_na_vela(info, ema13, ema21, "CALL")


def pullback_put_na_vela(info, ema13, ema21):
    return pullback_na_vela(info, ema13, ema21, "PUT")


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
    """Estratégia principal 5M + 15M + pullback + confirmação separada.

    A lógica mantém o núcleo conservador, mas elimina filtros redundantes
    que estavam transformando quase todos os setups válidos em AGUARDAR.
    """
    if len(candles_5m) < 40:
        return {
            "sinal": "AGUARDAR",
            "score": 0,
            "preco": float(candles_5m[-1]["close"]) if candles_5m else 0,
            "vela": candles_5m[-1]["_dt"] if candles_5m else None,
            "mensagem": "Poucas velas para análise.",
            "score_call": 0,
            "score_put": 0,
        }

    if len(candles_15m) < 40:
        return {
            "sinal": "AGUARDAR",
            "score": 0,
            "preco": float(candles_5m[-1]["close"]),
            "vela": candles_5m[-1]["_dt"],
            "mensagem": "Poucas velas de 15M.",
            "score_call": 0,
            "score_put": 0,
        }

    c = closes(candles_5m)
    preco = c[-1]

    ema5 = ema(c, 5)
    ema13 = ema(c, 13)
    ema21 = ema(c, 21)
    ema13_series = ema_series(c, 13)
    ema21_series = ema_series(c, 21)

    rsi14 = rsi(c, 14)
    atr14 = atr(candles_5m, 14)

    tendencia_5m = tendencia_timeframe(candles_5m)
    tendencia_15m = tendencia_timeframe(candles_15m)

    confirmacao = candle_info(candles_5m[-1])
    pullback_1 = candle_info(candles_5m[-2])
    pullback_2 = candle_info(candles_5m[-3])

    # EMA calculada no próprio candle do pullback, e não na vela atual.
    pb1_ema13 = ema13_series[-2]
    pb1_ema21 = ema21_series[-2]
    pb2_ema13 = ema13_series[-3]
    pb2_ema21 = ema21_series[-3]

    pb1_call = pullback_na_vela(
        pullback_1, pb1_ema13, pb1_ema21, "CALL"
    )
    pb2_call = pullback_na_vela(
        pullback_2, pb2_ema13, pb2_ema21, "CALL"
    )
    pb1_put = pullback_na_vela(
        pullback_1, pb1_ema13, pb1_ema21, "PUT"
    )
    pb2_put = pullback_na_vela(
        pullback_2, pb2_ema13, pb2_ema21, "PUT"
    )

    pullback_call = pb1_call or pb2_call
    pullback_put = pb1_put or pb2_put

    # A confirmação rompe a máxima/mínima da vela que realmente fez o pullback.
    pullback_call_info = pullback_1 if pb1_call else pullback_2 if pb2_call else None
    pullback_put_info = pullback_1 if pb1_put else pullback_2 if pb2_put else None

    confirmacao_call = False
    if confirmacao["close"] > confirmacao["open"] and pullback_call_info:
        rejeicao_inferior = (
            confirmacao["lower_wick"] >= confirmacao["body"] * 0.35
            and confirmacao["lower_wick"] > confirmacao["upper_wick"]
        )
        fechamento_forte = (
            confirmacao["body_ratio"] >= 0.40
            and (
                (confirmacao["high"] - confirmacao["close"])
                / confirmacao["range"]
            ) <= 0.30
        )
        rompeu_pullback = confirmacao["close"] > pullback_call_info["high"]
        confirmacao_call = (rejeicao_inferior or fechamento_forte) and rompeu_pullback

    confirmacao_put = False
    if confirmacao["close"] < confirmacao["open"] and pullback_put_info:
        rejeicao_superior = (
            confirmacao["upper_wick"] >= confirmacao["body"] * 0.35
            and confirmacao["upper_wick"] > confirmacao["lower_wick"]
        )
        fechamento_forte = (
            confirmacao["body_ratio"] >= 0.40
            and (
                (confirmacao["close"] - confirmacao["low"])
                / confirmacao["range"]
            ) <= 0.30
        )
        rompeu_pullback = confirmacao["close"] < pullback_put_info["low"]
        confirmacao_put = (rejeicao_superior or fechamento_forte) and rompeu_pullback

    movimento_4 = c[-1] - c[-4]
    movimento_8 = c[-1] - c[-8]
    contexto_call = movimento_4 > 0 and movimento_8 > 0
    contexto_put = movimento_4 < 0 and movimento_8 < 0

    # RSI deixa de ser uma trava absoluta. Ele vira confirmação de qualidade,
    # exceto quando está em extremo, situação que continua bloqueando a entrada.
    rsi_call_ok = rsi14 is not None and 50 <= rsi14 <= 68
    rsi_put_ok = rsi14 is not None and 32 <= rsi14 <= 50

    rsi_extremo = (
        rsi14 is not None
        and (rsi14 >= 72 or rsi14 <= 28)
    )

    atr_ok = True
    if atr14 is not None and preco != 0:
        atr_ratio = atr14 / preco
        if atr_ratio < 0.00008 or atr_ratio > 0.0035:
            atr_ok = False

    lateral = mercado_lateral(preco, ema5, ema13, ema21, atr14)

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

    sinal = "AGUARDAR"
    score = max(score_call, score_put)
    bloqueio = None

    # Núcleo obrigatório: tendência nos dois TFs + pullback + confirmação.
    # O 10º ponto vem de RSI OU contexto, evitando a antiga dupla trava.
    if lateral:
        bloqueio = "Mercado lateral ou tendência fraca."
    elif not atr_ok:
        bloqueio = "ATR fora da faixa ideal."
    elif rsi_extremo:
        bloqueio = f"RSI extremo ({rsi14:.2f})."
    elif tendencia_5m == "ALTA":
        if tendencia_15m != "ALTA":
            bloqueio = "5M em alta, mas 15M não confirma."
        elif not pullback_call:
            bloqueio = "Alta alinhada, mas sem pullback válido."
        elif not confirmacao_call:
            bloqueio = "Pullback encontrado, mas sem confirmação separada."
        elif score_call < 10:
            bloqueio = "Setup de alta sem confirmação adicional de qualidade."
        else:
            sinal = "CALL"
    elif tendencia_5m == "BAIXA":
        if tendencia_15m != "BAIXA":
            bloqueio = "5M em baixa, mas 15M não confirma."
        elif not pullback_put:
            bloqueio = "Baixa alinhada, mas sem pullback válido."
        elif not confirmacao_put:
            bloqueio = "Pullback encontrado, mas sem confirmação separada."
        elif score_put < 10:
            bloqueio = "Setup de baixa sem confirmação adicional de qualidade."
        else:
            sinal = "PUT"
    else:
        bloqueio = "5M sem tendência clara."

    detalhes_pullback = (
        "CONFIRMADO EM VELA ANTERIOR"
        if ((pullback_call and tendencia_5m == "ALTA") or
            (pullback_put and tendencia_5m == "BAIXA"))
        else "NÃO"
    )

    detalhes_confirmacao = (
        "CONFIRMADA"
        if ((confirmacao_call and tendencia_5m == "ALTA") or
            (confirmacao_put and tendencia_5m == "BAIXA"))
        else "NÃO"
    )

    if sinal == "CALL":
        mensagem = (
            "CALL FORTE | 5M ALTA + 15M ALTA | "
            "Pullback real | Confirmação em vela separada | "
            f"Score={score_call}/12 | RSI={rsi14:.2f}"
        )
    elif sinal == "PUT":
        mensagem = (
            "PUT FORTE | 5M BAIXA + 15M BAIXA | "
            "Pullback real | Confirmação em vela separada | "
            f"Score={score_put}/12 | RSI={rsi14:.2f}"
        )
    elif bloqueio:
        mensagem = f"AGUARDAR | {bloqueio}"
    else:
        mensagem = (
            f"AGUARDAR | 5M={tendencia_5m} | 15M={tendencia_15m} | "
            f"Pullback={detalhes_pullback} | Confirmação={detalhes_confirmacao} | "
            f"CALL={score_call} | PUT={score_put}"
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
        "lateral": "SIM" if lateral else "NÃO",
        "rsi_call_ok": rsi_call_ok,
        "rsi_put_ok": rsi_put_ok,
        "contexto_call": contexto_call,
        "contexto_put": contexto_put,
        "confirmacao_call": confirmacao_call,
        "confirmacao_put": confirmacao_put,
        "bloqueio": bloqueio or "SINAL",
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
    global _operacao_global_ativa

    sinal = resultado.get("sinal")
    if sinal not in ("CALL", "PUT"):
        return

    vela_sinal = resultado.get("vela")
    if not isinstance(vela_sinal, datetime):
        return

    vela_entrada = vela_sinal + timedelta(minutes=5)
    vela_expiracao = vela_entrada
    chave = f"{symbol}|{vela_sinal.isoformat()}"

    if _ultimas_operacoes_registradas.get(symbol) == chave:
        log(f"{symbol}: operacao duplicada para a mesma vela ignorada.")
        return
    if symbol in _operacoes_pendentes:
        log(f"{symbol}: ja existe operacao pendente.")
        return

    with _execucao_lock:
        if UMA_OPERACAO_GLOBAL and _operacao_global_ativa is not None:
            log(f"{symbol}: sinal ignorado porque existe operação global ativa.")
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
        "ordem_automatica": False,
        "valor": _valor_entrada_atual(),
        "instrument_id": None,
        "instrument_index": None,
        "balance_id": None,
    }

    if BULLEX_AUTO_TRADE:
        status = executar_ordem_demo(symbol, sinal, resultado)
        if status != "CONFIRMADA":
            return
        with _execucao_lock:
            info = _operacao_global_ativa or {}
        operacao.update({
            "ordem_automatica": True,
            "valor": info.get("valor", operacao["valor"]),
            "instrument_id": info.get("instrument_id"),
            "instrument_index": info.get("instrument_index"),
            "balance_id": info.get("balance_id"),
        })

    _operacoes_pendentes[symbol] = operacao
    _ultimas_operacoes_registradas[symbol] = chave
    log(f"{symbol}: operacao registrada {sinal} | vela entrada={vela_entrada.strftime('%H:%M')}")


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
    inicio_processamento = time.time()
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

            "bloqueio": resultado.get(
                "bloqueio",
                "-"
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
            f" | bloqueio="
            f"{resultado.get('bloqueio', '-')}"
        )

        if resultado["sinal"] in (
            "CALL",
            "PUT"
        ):
            janela_diag = _janela_execucao_5m()
            log(
                f"[LATENCIA] {symbol} sinal pronto em "
                f"{time.time() - inicio_processamento:.3f}s | "
                f"atraso_na_vela={janela_diag['atraso_segundos']:.3f}s"
            )

            # CAMINHO CRÍTICO:
            # primeiro tenta registrar/enviar a ordem automática.
            # Telegram fica fora do caminho crítico para não consumir
            # a janela máxima de 3 segundos.
            registrar_operacao(
                symbol,
                resultado,
                fechadas_5m
            )

            threading.Thread(
                target=enviar_sinal_telegram,
                args=(symbol, resultado),
                daemon=True,
                name=f"telegram-sinal-{chave}",
            ).start()

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
            second=0,
            microsecond=100000,
        )

    else:
        proxima = agora.replace(
            minute=proximo_bloco,
            second=0,
            microsecond=100000,
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
<h3>Execução automática DEMO</h3>
<div class="linha"><span>Modo</span><span class="valor">{{ estado.execucao.modo }}</span></div>
<div class="linha"><span>Automática</span><span class="valor">{{ "ATIVA" if estado.execucao.automatica else "DESATIVADA" }}</span></div>
<div class="linha"><span>Entrada atual</span><span class="valor">R$ {{ "%.2f"|format(estado.execucao.valor_atual) }}</span></div>
<div class="linha"><span>Progressão</span><span class="valor">{{ estado.execucao.nivel_progressao + 1 }}/3</span></div>
<div class="linha"><span>Operação ativa</span><span class="valor">{{ "SIM" if estado.execucao.operacao_ativa else "NÃO" }}</span></div>
<div class="linha"><span>Balance DEMO</span><span class="valor">{{ "ENCONTRADO" if estado.execucao.balance_id_disponivel else "AGUARDANDO" }}</span></div>
<div class="linha"><span>Último erro</span><span class="valor">{{ estado.execucao.ultimo_erro or "-" }}</span></div>
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
        "execucao_automatica": BULLEX_AUTO_TRADE,
        "modo_execucao": "DEMO",
        "valor_entrada_atual": _valor_entrada_atual(),
        "nivel_progressao": _nivel_progressao,
        "operacao_global_ativa": _operacao_global_ativa,
        "balance_id_disponivel": _bullex_balance_id is not None,
        "balance_id_fonte": _bullex_balance_source,
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
    })


# ============================================================
# EXECUÇÃO
# ============================================================

_atualizar_estado_execucao()

log(f"AUTO TRADE DEMO={'ATIVO' if BULLEX_AUTO_TRADE else 'DESATIVADO'} | entrada inicial=R${_valor_entrada_atual():.2f}")
log(f"BULLEX_USER_BALANCE_ID={'CONFIGURADO' if BULLEX_USER_BALANCE_ID else 'AUTO-DESCOBERTA'}")

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
