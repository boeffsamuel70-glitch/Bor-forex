import os
import time
import threading
import secrets
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

# R6: o navegador mostrou local_time variável entre autenticações
# (ex.: 8869 e 7985). Nesta versão o valor é gerado dinamicamente
# para cada authenticate. A variável antiga BULLEX_LOCAL_TIME pode
# permanecer no Render, mas não é usada na autenticação R6.
BULLEX_LOCAL_TIME_LEGACY = os.getenv("BULLEX_LOCAL_TIME", "").strip()


def _gerar_local_time_auth():
    # Faixa de 4 dígitos observada nas capturas do navegador.
    return 1000 + secrets.randbelow(9000)

BULLEX_USER_AGENT = os.getenv(
    "BULLEX_USER_AGENT",
    "Mozilla/5.0 (Linux; Android 10; K) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/152.0.0.0 Mobile Safari/537.36"
).strip()

# ============================================================
# ATIVOS BULLEX
# ============================================================

# Somente mercado aberto. Os active_id não ficam fixos no código:
# são descobertos automaticamente na lista digital da Traderoom após autenticar.
ATIVO_BULLEX = {
    "EURUSD_OTC": {"symbol": "EUR/USD OTC", "active_id": 76, "ticker": "EURUSD-OTC", "is_otc": True, "mercado": "OTC"},
    "EURJPY_OTC": {"symbol": "EUR/JPY OTC", "active_id": 79, "ticker": "EURJPY-OTC", "is_otc": True, "mercado": "OTC"},
    "GBPUSD_OTC": {"symbol": "GBP/USD OTC", "active_id": 81, "ticker": "GBPUSD-OTC", "is_otc": True, "mercado": "OTC"},
    "USDJPY_OTC": {"symbol": "USD/JPY OTC", "active_id": 85, "ticker": "USDJPY-OTC", "is_otc": True, "mercado": "OTC"},
    "GBPJPY_OTC": {"symbol": "GBP/JPY OTC", "active_id": 84, "ticker": "GBPJPY-OTC", "is_otc": True, "mercado": "OTC"},
}

PARES_MERCADO_ABERTO = {}

# OTC populares. Os active_id NÃO são fixos:
# são descobertos automaticamente na lista da Traderoom.
PARES_OTC_ALVO = {
    "EURUSD": "EUR/USD OTC",
    "EURJPY": "EUR/JPY OTC",
    "GBPUSD": "GBP/USD OTC",
    "USDJPY": "USD/JPY OTC",
    "GBPJPY": "GBP/JPY OTC",
}

_bullex_assets_lock = threading.RLock()
_bullex_assets_detected = True
_bullex_assets_last_error = None
_bullex_assets_updated_at = None
_bullex_assets_source = "OTC_STATIC_FALLBACK_76_79_81_85_84"
_bullex_assets_ready_event = threading.Event()
_bullex_assets_init_lock = threading.Lock()

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
BULLEX_DIAGNOSTIC_VERSION = "OTC-ONLY-BINARY-5-BRL-20260909-R18-SR-M5-INTRABAR"

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

HORA_INICIO = 22
HORA_FIM = 20

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

VALORES_ENTRADA = [5.00]
EXPIRACAO_MINUTOS = 5
# A antiga janela de 3 segundos foi removida.
# Esta estratégia entra DURANTE a vela atual e expira no fechamento da MESMA vela.
INTRAVELA_MIN_SEGUNDOS_DECORRIDOS = 20
INTRAVELA_MIN_SEGUNDOS_RESTANTES = 35

# Estratégia R17: somente suporte/resistência M5.
# O nível precisa ter pelo menos 3 toques em velas M5 fechadas.
SR_M5_LOOKBACK = 100
SR_M5_PIVOT_JANELA = 2
SR_M5_MIN_TOQUES = 3
SR_M5_TOLERANCIA_ATR = 0.16

# A vela atual precisa nascer longe do nível.
# Se abrir colada no suporte/resistência, NÃO opera.
SR_M5_DISTANCIA_ABERTURA_ATR_MIN = 0.55

# Rejeição/retração depois do toque.
INTRAVELA_RETRACAO_MIN = 0.20
INTRAVELA_RETRACAO_MAX = 0.68
INTRAVELA_REJEICAO_ATR_MIN = 0.10
INTRAVELA_PAVIO_MIN_FRACAO_MOVIMENTO = 0.10

UMA_OPERACAO_GLOBAL = True

_intravela_lock = threading.RLock()
_intravela_estado = {}
_intravela_velas_tentadas = set()

# ============================================================
# ATIVOS
# ============================================================

ATIVOS = {
    "EURUSD_OTC": "EUR/USD OTC",
    "EURJPY_OTC": "EUR/JPY OTC",
    "GBPUSD_OTC": "GBP/USD OTC",
    "USDJPY_OTC": "USD/JPY OTC",
    "GBPJPY_OTC": "GBP/JPY OTC",
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
        "regime": "-",
        "estrategia": "-",
        "zona_fibonacci": "-",
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
# Base histórica consolidada antes da estratégia de retração intravela:
# 44 operações decididas = 22 WIN / 22 LOSS = 50,00%.
_historico_resultados = (
    [{"resultado": "WIN", "origem": "BASE_ANTES_R13", "estrategia": "BASE"} for _ in range(22)]
    + [{"resultado": "LOSS", "origem": "BASE_ANTES_R13", "estrategia": "BASE"} for _ in range(22)]
)
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

    restante = max(0.0, candle_close - server_ts)

    return {
        "server_ts": server_ts,
        "source": source,
        "candle_open": int(candle_open),
        "candle_close": int(candle_close),
        "atraso_segundos": float(atraso),
        "segundos_restantes": float(restante),
        "permitida": restante > 0,
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
    Gera request_id no padrão observado na Traderoom:
    <unix_seconds>_<numero_grande_variavel>.

    Exemplos capturados no navegador tinham sufixos de 9-10 dígitos.
    """

    # Mantém um contador interno apenas para compatibilidade/diagnóstico,
    # mas o sufixo enviado segue o formato grande e variável do navegador.
    global _bullex_request_counter
    with _bullex_request_lock:
        _bullex_request_counter += 1

    sufixo = 100_000_000 + secrets.randbelow(1_900_000_000)
    return f"{int(time.time())}_{sufixo}"


def _montar_auth_message():
    """
    Monta EXATAMENTE a estrutura principal observada no HAR:

    {
      "name": "authenticate",
      "request_id": "...",
      "local_time": <dinamico>,
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
        "local_time": _gerar_local_time_auth(),
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


def _extrair_balance_id(obj=None):
    """Aceita somente BULLEX_USER_BALANCE_ID. Sem descoberta automática."""
    if BULLEX_USER_BALANCE_ID:
        return str(BULLEX_USER_BALANCE_ID), "ENV"
    return None, None


def _solicitar_balance_id_demo():
    global _bullex_balance_id, _bullex_balance_source, _bullex_last_error

    if BULLEX_USER_BALANCE_ID:
        _bullex_balance_id = str(BULLEX_USER_BALANCE_ID)
        _bullex_balance_source = "ENV"
        _atualizar_estado_execucao()
        log(f"Balance definido por BULLEX_USER_BALANCE_ID: {_bullex_balance_id}")
        return _bullex_balance_id

    _bullex_balance_id = None
    _bullex_balance_source = None
    _atualizar_estado_execucao()
    log("[BALANCE] BULLEX_USER_BALANCE_ID vazio. Nenhum balance_id sera escolhido automaticamente.")
    return None


def _obter_balance_id():
    if BULLEX_USER_BALANCE_ID:
        return str(BULLEX_USER_BALANCE_ID)
    return None


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
            resposta = _enviar_e_aguardar("digital-options.get-instruments", version, body, timeout=0.9)
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


def _resposta_indica_indisponibilidade_produto(resposta):
    texto = json.dumps(resposta, ensure_ascii=False).lower() if isinstance(resposta, dict) else str(resposta).lower()
    termos = (
        "not available", "unavailable", "closed", "market is closed",
        "purchasing options is over", "time for purchasing", "4104",
        "instrument not found", "instrument unavailable", "asset is closed",
        "option is closed", "not tradable", "temporarily unavailable",
    )
    return any(t in texto for t in termos)


def _registrar_ordem_confirmada(symbol, ticker, sinal, valor, active_id, balance_id,
                                produto, resposta, janela, instrument_id=None,
                                instrument_index=None):
    global _operacao_global_ativa

    candle_open_dt = datetime.fromtimestamp(janela["candle_open"], TZ)
    candle_close_dt = datetime.fromtimestamp(janela["candle_close"], TZ)
    msg = resposta.get("msg") if isinstance(resposta, dict) else None
    option_id = msg.get("id") if isinstance(msg, dict) else None

    _operacao_global_ativa = {
        "symbol": symbol,
        "ticker": ticker,
        "sinal": sinal,
        "valor": valor,
        "asset_id": active_id,
        "balance_id": str(balance_id),
        "produto": produto,
        "option_id": option_id,
        "instrument_id": instrument_id,
        "instrument_index": instrument_index,
        "expired": int(janela["candle_close"]),
        "expiracao": candle_close_dt.isoformat(),
        "candle_open": candle_open_dt.isoformat(),
        "atraso_segundos": round(float(janela["atraso_segundos"]), 3),
        "fonte_horario": janela["source"],
        "enviada_em": agora_brt().isoformat(),
        "resultado": "PENDENTE",
        "response": resposta,
    }
    estado["execucao"]["ultima_ordem"] = _operacao_global_ativa.copy()
    estado["execucao"]["ultimo_erro"] = None
    _atualizar_estado_execucao()
    log(
        f"[AUTO] ORDEM CONFIRMADA via {produto}: "
        f"{symbol} {sinal} R${valor:.2f} id={option_id}"
    )
    return "CONFIRMADA"


def executar_ordem_intravela(symbol, sinal, resultado):
    global _operacao_global_ativa
    global _bullex_last_error

    if not BULLEX_AUTO_TRADE:
        return None

    if sinal not in ("CALL", "PUT"):
        return None

    if not BULLEX_USER_BALANCE_ID:
        estado["execucao"]["ultimo_erro"] = "SEM_BALANCE_ID"
        _atualizar_estado_execucao()
        return "SEM_BALANCE_ID"

    with _execucao_lock:
        if UMA_OPERACAO_GLOBAL and _operacao_global_ativa is not None:
            log(
                f"[INTRAVELA] {symbol}: sinal ignorado; "
                "já existe operação global ativa."
            )
            return "BLOQUEADA_GLOBAL"

    balance_id = _obter_balance_id()
    if not balance_id:
        return "SEM_BALANCE_ID"

    config = next(
        (cfg for cfg in ATIVO_BULLEX.values() if cfg["symbol"] == symbol),
        None,
    )
    if not config:
        return "SEM_ATIVO"

    active_id = int(config["active_id"])
    ticker = config.get("ticker")
    valor = _valor_entrada_atual()

    server_ts, source = _horario_servidor_atual()
    candle_from = int(resultado["candle_from"])
    candle_to = int(resultado["candle_to"])
    restantes = candle_to - server_ts

    # Proteção específica desta estratégia:
    # não muda a expiração para a vela seguinte.
    if server_ts < candle_from or server_ts >= candle_to:
        log(
            f"[INTRAVELA] {symbol}: vela do sinal já encerrou; "
            "ordem NÃO enviada."
        )
        return "VELA_ENCERRADA"

    if restantes < INTRAVELA_MIN_SEGUNDOS_RESTANTES:
        log(
            f"[INTRAVELA] {symbol}: restam apenas {restantes:.1f}s; "
            "ordem NÃO enviada para evitar cair na próxima vela."
        )
        return "POUCO_TEMPO"

    janela = {
        "server_ts": server_ts,
        "source": source,
        "candle_open": candle_from,
        "candle_close": candle_to,
        "atraso_segundos": server_ts - candle_from,
        "segundos_restantes": restantes,
        "permitida": True,
    }

    body = {
        "user_balance_id": int(balance_id),
        "active_id": active_id,
        "option_type_id": 3,
        "direction": _direcao_instrumento(sinal),
        "expired": candle_to,
        "price": float(valor),
        "refund_value": 0,
    }

    log(
        f"[AUTO INTRAVELA] Enviando {symbol} {sinal} R${valor:.2f} | "
        f"entrada_estimada={resultado['preco']:.5f} | "
        f"expira_na_mesma_vela={datetime.fromtimestamp(candle_to, TZ).strftime('%H:%M:%S')} | "
        f"restam={restantes:.1f}s"
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

        if not _ordem_option_confirmada(resposta):
            with _bullex_diag_lock:
                _bullex_diag["orders_errors"] += 1
            estado["execucao"]["ultimo_erro"] = (
                _mensagem_erro_ordem(resposta)
                or f"Ordem intravela não confirmada: {resposta}"
            )
            _atualizar_estado_execucao()
            log(
                "[AUTO INTRAVELA] Ordem não confirmada: "
                + json.dumps(resposta, ensure_ascii=False)
            )
            threading.Thread(
                target=enviar_status_ordem_telegram,
                args=(
                    symbol,
                    sinal,
                    "NÃO ABERTA",
                    _mensagem_erro_ordem(resposta) or str(resposta),
                ),
                daemon=True,
                name=f"telegram-ordem-recusada-{active_id}",
            ).start()
            return "SEM_CONFIRMACAO"

        with _bullex_diag_lock:
            _bullex_diag["orders_confirmed"] += 1

        status = _registrar_ordem_confirmada(
            symbol,
            ticker,
            sinal,
            valor,
            active_id,
            balance_id,
            "BINARIA_INTRAVELA",
            resposta,
            janela,
        )

        threading.Thread(
            target=enviar_status_ordem_telegram,
            args=(
                symbol,
                sinal,
                "CONFIRMADA",
                f"R${valor:.2f} | expira "
                f"{datetime.fromtimestamp(candle_to, TZ).strftime('%H:%M:%S')}",
            ),
            daemon=True,
            name=f"telegram-ordem-confirmada-{active_id}",
        ).start()

        with _execucao_lock:
            if _operacao_global_ativa is not None:
                _operacao_global_ativa["preco_entrada_estimado"] = float(resultado["preco"])
                _operacao_global_ativa["estrategia"] = "RETRACAO_MESMA_VELA"
                _operacao_global_ativa["regime"] = "INTRAVELA"

        return status

    except Exception as e:
        with _bullex_diag_lock:
            _bullex_diag["orders_errors"] += 1
        _bullex_last_error = str(e)
        estado["execucao"]["ultimo_erro"] = str(e)
        _atualizar_estado_execucao()
        log(f"[AUTO INTRAVELA] ERRO ao enviar ordem: {e}")
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


def _aguardar_probe_pos_auth(request_id, nome, version, timeout=8):
    """Espera a resposta do primeiro comando enviado dentro do callback auth.

    O envio ocorre de forma síncrona dentro de ``authenticated`` para testar
    se a Traderoom exige uma inicialização imediata. A espera precisa ficar
    fora do callback para não bloquear o recebimento das respostas do socket.
    """
    limite = time.time() + timeout
    resposta = None

    with _bullex_cv:
        while time.time() < limite:
            resposta = _bullex_response_store.pop(str(request_id), None)
            if resposta is not None:
                break

            if not _bullex_connected:
                break

            restante = limite - time.time()
            if restante <= 0:
                break
            _bullex_cv.wait(timeout=min(0.25, restante))

    if resposta is None:
        if not _bullex_connected:
            log(
                f"[POS-AUTH IMEDIATO] Socket fechou após envio de {nome} "
                f"v{version} request_id={request_id}, antes da resposta."
            )
        else:
            log(
                f"[POS-AUTH IMEDIATO] Timeout aguardando {nome} "
                f"v{version} request_id={request_id}."
            )
        return

    ativos = _extrair_mercado_aberto_da_resposta(resposta)
    log(
        f"[POS-AUTH IMEDIATO] Resposta recebida de {nome} v{version}: "
        f"{len(ativos)} par(es) normal(is) reconhecido(s)."
    )

    if ativos:
        try:
            _atualizar_ativos_mercado_aberto(ativos, f"{nome} v{version} IMEDIATO")
            _assinar_candles_mercado_aberto()
            log(
                f"[OPEN MARKET] Inicialização imediata concluída com "
                f"{len(ativos)} ativo(s)."
            )
            return
        except Exception as e:
            log(f"[POS-AUTH IMEDIATO] Falha ao aplicar ativos: {e}")

    # Se a primeira resposta vier sem os pares esperados, a rotina normal
    # tenta as versões/filtros alternativos, desde que o socket continue vivo.
    if _bullex_connected and _bullex_authenticated:
        _inicializar_ativos_mercado_aberto()


def _enviar_primeiro_comando_no_authenticated(ws):
    """Envia a primeira consulta ainda dentro do callback authenticated."""
    nome = "digital-option-instruments.get-underlying-list"
    version = "2.0"
    body = {"type": "digital-option"}
    payload = _montar_send_message(nome, version, body)
    request_id = str(payload["request_id"])

    with _bullex_cv:
        _bullex_response_store.pop(request_id, None)

    log(
        f"[POS-AUTH IMEDIATO] ENVIANDO dentro de authenticated: "
        f"{nome} v{version} request_id={request_id}"
    )
    ws.send(json.dumps(payload, separators=(",", ":")))
    log(
        f"[POS-AUTH IMEDIATO] ENVIO CONCLUÍDO: {nome} "
        f"request_id={request_id}"
    )

    threading.Thread(
        target=_aguardar_probe_pos_auth,
        args=(request_id, nome, version),
        daemon=True,
        name="bullex-pos-auth-probe",
    ).start()


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
    ):
        log(
            f"[DIAG WS] name={nome} request_id={request_id} "
            f"active_id={active_id} size={size} "
            f"msg_type={type(msg).__name__} msg_value={msg!r} "
            f"client_session_id_present={bool(data.get('client_session_id'))} "
            f"keys={list(data.keys())[:12]}"
        )

    # ========================================================
    # AUTENTICAÇÃO
    # ========================================================

    if nome == "authenticated":
        log(
            "[AUTH DIAG] "
            f"msg={msg!r} | "
            f"request_id={request_id!r} | "
            f"client_session_id_present={bool(data.get('client_session_id'))}"
        )

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

        # Balance por variável de ambiente não envia comando ao servidor.
        threading.Thread(
            target=_solicitar_balance_id_demo,
            daemon=True,
            name="bullex-balance",
        ).start()

        # R4: o primeiro comando pós-login é enviado IMEDIATAMENTE, ainda
        # dentro deste callback. Isso testa se a Bullex exige inicialização
        # antes de encerrar a sessão autenticada.
        try:
            _enviar_primeiro_comando_no_authenticated(ws)
        except Exception as e:
            log(f"[POS-AUTH IMEDIATO] Falha ao enviar primeiro comando: {e}")
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

                # Estratégia única R13: observa a vela de 5M ainda aberta.
                if int(size) == 300:
                    threading.Thread(
                        target=_processar_sinal_intravela,
                        args=(active_id, dict(msg)),
                        daemon=True,
                        name=f"intravela-scan-{active_id}",
                    ).start()
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

    texto_erro = str(error)
    if "closed normally" in texto_erro.lower() or "code 1000" in texto_erro.lower():
        _bullex_last_error = None
        log(f"Bullex WebSocket encerrou normalmente; reconexão automática será feita: {error}")
    else:
        _bullex_last_error = texto_erro
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
            f"local_time={auth['local_time']})."
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


def _iter_dicts_recursivo(obj):
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


def _normalizar_par_mercado_aberto(item):
    """Normaliza Forex normal e os OTC explicitamente configurados."""
    if not isinstance(item, dict):
        return None

    campos_texto = [
        item.get(chave)
        for chave in (
            "ticker", "ticker_name", "tickerName", "display_name", "displayName",
            "instrument_name", "instrumentName", "name", "symbol", "underlying",
            "underlying_name", "underlyingName", "description", "type",
            "instrument_type",
        )
    ]
    texto = " ".join(
        str(x) for x in campos_texto if x not in (None, "")
    ).upper()

    is_otc = (
        item.get("is_otc") is True
        or item.get("isOtc") is True
        or "OTC" in texto
    )

    active_id = _primeiro_valor(
        item,
        ("active_id", "activeId", "activeID", "asset_id", "assetId", "underlying_id"),
    )
    try:
        active_id = int(active_id)
    except (TypeError, ValueError):
        return None

    ticker = _primeiro_valor(
        item,
        ("ticker", "ticker_name", "tickerName", "symbol", "name", "underlying", "display_name"),
    )
    symbol = _primeiro_valor(
        item,
        ("symbol", "asset_name", "assetName", "underlying", "underlying_name", "name", "ticker"),
    )

    base = re.sub(r"[^A-Z]", "", str(ticker or symbol or "").upper())
    if len(base) < 6:
        return None

    par = base[:6]

    if is_otc:
        if par not in PARES_OTC_ALVO:
            return None
        codigo = f"{par}_OTC"
        symbol_final = PARES_OTC_ALVO[par]
        mercado = "OTC"
    else:
        if par not in PARES_MERCADO_ABERTO:
            return None
        codigo = par
        symbol_final = PARES_MERCADO_ABERTO[par]
        mercado = "ABERTO"

    return {
        "codigo": codigo,
        "symbol": symbol_final,
        "active_id": active_id,
        "ticker": str(ticker or (par + ("-OTC" if is_otc else ""))).strip(),
        "is_otc": bool(is_otc),
        "mercado": mercado,
        "raw": item,
    }


def _extrair_mercado_aberto_da_resposta(resposta):
    encontrados = {}
    for item in _iter_dicts_recursivo(resposta):
        normalizado = _normalizar_par_mercado_aberto(item)
        if not normalizado:
            continue
        codigo = normalizado["codigo"]
        atual = encontrados.get(codigo)
        if atual is None:
            encontrados[codigo] = normalizado
            continue
        # Prefere registro explicitamente visível/ativo quando houver duplicidade.
        raw_novo = normalizado.get("raw") or {}
        raw_atual = atual.get("raw") or {}
        score_novo = int(raw_novo.get("is_visible") is True) + int(raw_novo.get("is_active") is True)
        score_atual = int(raw_atual.get("is_visible") is True) + int(raw_atual.get("is_active") is True)
        if score_novo > score_atual:
            encontrados[codigo] = normalizado
    ordem = (
        list(PARES_MERCADO_ABERTO.keys())
        + [f"{c}_OTC" for c in PARES_OTC_ALVO.keys()]
    )
    return [encontrados[c] for c in ordem if c in encontrados]


def _corpo_lista_instrumentos(nome):
    if nome == "digital-option-instruments.get-underlying-list":
        return {"type": "digital-option"}
    return None


def _consultar_lista_mercado_aberto(nome, versoes=("2.0", "1.0")):
    """Consulta e agrega mercado aberto + OTC configurados.

    Algumas respostas da Traderoom podem variar conforme versão/body.
    A R17 não para na primeira resposta parcial: junta todos os ativos
    reconhecidos para não perder os OTC.
    """
    ultimo_erro = None
    encontrados = {}

    if nome == "digital-option-instruments.get-underlying-list":
        corpos = (
            {"type": "digital-option"},
            {"type": "digital"},
            None,
        )
    else:
        corpos = (None,)

    ultima_resposta = None

    for versao in versoes:
        for body in corpos:
            try:
                resposta = _enviar_e_aguardar(
                    nome,
                    versao,
                    body,
                    timeout=12,
                )
                ultima_resposta = resposta
                ativos = _extrair_mercado_aberto_da_resposta(resposta)

                log(
                    f"[ATIVOS] {nome} v{versao} body={body}: "
                    f"{len(ativos)} configurado(s) reconhecido(s)."
                )

                for item in ativos:
                    encontrados[item["codigo"]] = item

            except Exception as e:
                ultimo_erro = e
                log(
                    f"[ATIVOS] Falha em {nome} v{versao} "
                    f"body={body}: {e}"
                )

    if encontrados:
        ordem = (
            list(PARES_MERCADO_ABERTO.keys())
            + [f"{c}_OTC" for c in PARES_OTC_ALVO.keys()]
        )
        ativos_finais = [
            encontrados[c]
            for c in ordem
            if c in encontrados
        ]
        return ultima_resposta, ativos_finais

    if ultimo_erro:
        raise ultimo_erro

    return ultima_resposta, []


def _atualizar_ativos_mercado_aberto(ativos, origem):
    global ATIVO_BULLEX
    global ATIVOS
    global _bullex_assets_detected
    global _bullex_assets_last_error
    global _bullex_assets_updated_at
    global _bullex_assets_source

    if not ativos:
        raise RuntimeError("Nenhum ativo configurado (aberto/OTC) foi encontrado na Traderoom.")

    novos_bullex = {}
    novos_ativos = {}
    for item in ativos:
        codigo = item["codigo"]
        novos_bullex[codigo] = {
            "symbol": item["symbol"],
            "active_id": int(item["active_id"]),
            "ticker": item["ticker"],
            "is_otc": bool(item.get("is_otc")),
            "mercado": item.get("mercado", "OTC" if item.get("is_otc") else "ABERTO"),
        }
        novos_ativos[codigo] = item["symbol"]

    with _bullex_assets_lock:
        ATIVO_BULLEX = novos_bullex
        ATIVOS = novos_ativos
        _bullex_assets_detected = True
        _bullex_assets_last_error = None
        _bullex_assets_updated_at = agora_brt().isoformat()
        _bullex_assets_source = origem
        _bullex_assets_ready_event.set()

    estado["ativos_info"] = {
        "tipo": "OTC",
        "quantidade": len(novos_bullex),
        "status": "AUTOMÁTICO",
        "lista": ", ".join(
            f"{cfg['ticker']} (id {cfg['active_id']})"
            for cfg in novos_bullex.values()
        ) or "-",
    }
    log(
        "[ATIVOS] Mercado aberto + OTC carregados: "
        + ", ".join(
            f"{cfg['ticker']}={cfg['active_id']}"
            for cfg in novos_bullex.values()
        )
    )

    desejados = set(PARES_MERCADO_ABERTO.keys()) | {
        f"{c}_OTC" for c in PARES_OTC_ALVO.keys()
    }
    faltantes = sorted(desejados - set(novos_bullex.keys()))
    if faltantes:
        log(
            "[ATIVOS] Configurados mas não retornados pela Traderoom: "
            + ", ".join(faltantes)
        )


def _inicializar_ativos_mercado_aberto():
    """Descobre Forex normal e os OTC configurados automaticamente.

    A inicialização é serializada para impedir duas descobertas concorrentes
    após reconexões rápidas do WebSocket.
    """
    global _bullex_assets_last_error

    if not _bullex_assets_init_lock.acquire(blocking=False):
        log("[OPEN MARKET] Descoberta de ativos já está em andamento.")
        return

    try:
        _bullex_assets_ready_event.clear()
        fonte_digital = "digital-option-instruments.get-underlying-list"

        try:
            _, ativos = _consultar_lista_mercado_aberto(fonte_digital)
            if not ativos:
                raise RuntimeError(
                    "Lista digital não retornou os pares configurados."
                )

            _atualizar_ativos_mercado_aberto(ativos, fonte_digital)
            _assinar_candles_mercado_aberto()
            log(
                f"[OPEN MARKET] Inicialização concluída com {len(ativos)} ativo(s)."
            )
            return

        except Exception as e:
            _bullex_assets_last_error = str(e)
            log(f"[OPEN MARKET] Descoberta digital falhou: {e}")

        # Diagnóstico adicional. IDs marginais nunca são usados para ordens.
        try:
            nome_marginal = "marginal-forex-instruments.get-underlying-list"
            _, diagnostico = _consultar_lista_mercado_aberto(nome_marginal)
            if diagnostico:
                log(
                    "[OPEN MARKET] A lista marginal reconheceu: "
                    + ", ".join(
                        f"{x['ticker']}={x['active_id']}" for x in diagnostico
                    )
                    + ". Mantidos apenas como diagnóstico; nenhuma ordem usa esses IDs."
                )
        except Exception as diag_e:
            log(f"[OPEN MARKET] Diagnóstico marginal indisponível: {diag_e}")

        # Mantém ATIVOS (a lista lógica dos pares) intacta. Somente o mapa
        # de active_id fica vazio enquanto a Traderoom não retornar IDs válidos.
        with _bullex_assets_lock:
            ATIVO_BULLEX.clear()
            _bullex_assets_ready_event.clear()

        estado["ativos_info"] = {
            "tipo": "OTC",
            "quantidade": 0,
            "status": "AGUARDANDO",
            "lista": "-",
            "erro": _bullex_assets_last_error,
        }
        log(
            "[OPEN MARKET] Ativos ainda não disponíveis. "
            "A leitura ficará bloqueada até nova autenticação/descoberta."
        )
    finally:
        _bullex_assets_init_lock.release()


def _aguardar_ativos_mercado_aberto(timeout=30):
    """Aguarda o mapa de active_id sem deixar a estratégia rodar com mapa vazio."""
    limite = time.time() + timeout

    while time.time() < limite:
        with _bullex_assets_lock:
            prontos = bool(ATIVO_BULLEX) and _bullex_assets_detected

        if prontos:
            _bullex_assets_ready_event.set()
            return True

        if not _bullex_connected:
            # A thread persistente reconecta automaticamente.
            time.sleep(0.25)
            continue

        if not _bullex_authenticated:
            time.sleep(0.1)
            continue

        restante = limite - time.time()
        if restante <= 0:
            break

        _bullex_assets_ready_event.wait(timeout=min(0.5, restante))

    return False

def _assinar_candles_mercado_aberto():
    """Assina somente M5 dos ativos abertos/OTC usados pela R17."""
    assinaturas = set()

    with _bullex_assets_lock:
        configs = list(ATIVO_BULLEX.values())

    for config in configs:
        active_id = int(config["active_id"])
        chave = (active_id, 300)
        if chave in assinaturas:
            continue
        assinaturas.add(chave)

        try:
            _assinar_candle(active_id, 300)
            log(
                f"[ATIVOS] Assinatura M5 ativa: "
                f"{config.get('symbol')} [{config.get('mercado', 'ABERTO')}] "
                f"active_id={active_id}"
            )
        except Exception as e:
            log(
                f"[ATIVOS] Falha assinatura M5 "
                f"active_id={active_id}: {e}"
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
        log(
            f"[WS SEND] primeiro/seguinte comando pós-auth: {nome} "
            f"v{version} request_id={request_id}"
        )
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

    with _bullex_assets_lock:
        config = ATIVO_BULLEX.get(codigo)

    if not config:
        raise RuntimeError(
            f"ACTIVE_ID_AGUARDANDO: {codigo} ainda não foi carregado pela Traderoom."
        )

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
# ESTRATÉGIA ÚNICA - S/R M15 + RETRAÇÃO NA MESMA VELA M5
# ============================================================

def _symbol_por_active_id(active_id):
    try:
        aid = int(active_id)
    except Exception:
        return None, None

    with _bullex_assets_lock:
        for codigo, cfg in ATIVO_BULLEX.items():
            if int(cfg.get("active_id")) == aid:
                return codigo, cfg.get("symbol")
    return None, None


def _candles_cache(active_id, size):
    with _bullex_cv:
        bucket = _bullex_candles.get((int(active_id), int(size)), {})
        candles = [dict(x) for x in bucket.values()]
    return ordenar_candles(candles)


def _atr_cache_5m(active_id):
    candles = _candles_cache(active_id, 300)
    fechadas = somente_velas_fechadas(candles, 5)
    if len(fechadas) < 15:
        return None
    return atr(fechadas, 14)


def _atr_cache_15m(active_id):
    candles = _candles_cache(active_id, 900)
    fechadas = somente_velas_fechadas(candles, 15)
    if len(fechadas) < 15:
        return None
    return atr(fechadas, 14)


def _pivos_sr(candles, janela):
    """Retorna pivôs de suporte e resistência usando apenas candles M15 fechados."""
    infos = [candle_info(c) for c in candles]
    suportes = []
    resistencias = []
    w = janela

    for i in range(w, len(infos) - w):
        atual = infos[i]
        viz = infos[i - w:i] + infos[i + 1:i + w + 1]

        if all(atual["low"] <= x["low"] for x in viz):
            suportes.append(atual["low"])

        if all(atual["high"] >= x["high"] for x in viz):
            resistencias.append(atual["high"])

    return suportes, resistencias


def _agrupar_niveis(valores, tolerancia):
    """Agrupa pivôs próximos e conta quantas vezes o nível foi respeitado."""
    if not valores or tolerancia <= 0:
        return []

    grupos = []
    for valor in sorted(valores):
        achou = None
        for grupo in grupos:
            if abs(valor - grupo["nivel"]) <= tolerancia:
                achou = grupo
                break

        if achou is None:
            grupos.append({
                "nivel": float(valor),
                "valores": [float(valor)],
                "toques": 1,
            })
        else:
            achou["valores"].append(float(valor))
            achou["toques"] += 1
            achou["nivel"] = sum(achou["valores"]) / len(achou["valores"])

    return grupos


def _niveis_sr_m15(active_id):
    candles = _candles_cache(active_id, 900)
    fechadas = somente_velas_fechadas(candles, 15)

    if len(fechadas) < 25:
        return [], [], None

    fechadas = fechadas[-SR_M15_LOOKBACK:]
    atr15 = atr(fechadas, 14)
    if not atr15 or atr15 <= 0:
        return [], [], None

    tolerancia = atr15 * SR_M15_TOLERANCIA_ATR
    sup_pivos, res_pivos = _pivos_sr(fechadas, SR_M15_PIVOT_JANELA)

    suportes = [
        g for g in _agrupar_niveis(sup_pivos, tolerancia)
        if g["toques"] >= SR_M15_MIN_TOQUES
    ]
    resistencias = [
        g for g in _agrupar_niveis(res_pivos, tolerancia)
        if g["toques"] >= SR_M15_MIN_TOQUES
    ]

    return suportes, resistencias, atr15



def _niveis_sr_m5(active_id):
    candles = _candles_cache(active_id, 300)
    fechadas = somente_velas_fechadas(candles, 5)
    if len(fechadas) < 25:
        return [], [], None
    fechadas = fechadas[-SR_M5_LOOKBACK:]
    atr5 = atr(fechadas, 14)
    if not atr5 or atr5 <= 0:
        return [], [], None
    tolerancia = atr5 * SR_M5_TOLERANCIA_ATR
    sup, res = _pivos_sr(fechadas, SR_M5_PIVOT_JANELA)
    suportes = [g for g in _agrupar_niveis(sup, tolerancia) if g["toques"] >= SR_M5_MIN_TOQUES]
    resistencias = [g for g in _agrupar_niveis(res, tolerancia) if g["toques"] >= SR_M5_MIN_TOQUES]
    return suportes, resistencias, atr5


def _combinar_niveis_sr(n15, n5, atr5):
    """M15 tem prioridade; M5 exige >=3 toques; confluência M5+M15 é a mais forte."""
    limite = atr5 * SR_CONFLUENCIA_ATR5
    saida = []
    for a in n15:
        item = dict(a)
        item["timeframe"] = "M15"
        item["confluencia"] = False
        perto = [b for b in n5 if abs(b["nivel"] - a["nivel"]) <= limite]
        if perto:
            b = min(perto, key=lambda x: abs(x["nivel"] - a["nivel"]))
            item["nivel"] = (a["nivel"] + b["nivel"]) / 2.0
            item["timeframe"] = "M5+M15"
            item["confluencia"] = True
            item["toques"] = a["toques"] + b["toques"]
        saida.append(item)
    for b in n5:
        if any(abs(b["nivel"] - a["nivel"]) <= limite for a in n15):
            continue
        item = dict(b)
        item["timeframe"] = "M5"
        item["confluencia"] = False
        saida.append(item)
    return saida

def _nivel_mais_proximo(niveis, preco, lado):
    """Escolhe o nível M15 relevante mais próximo do preço atual."""
    if not niveis:
        return None

    if lado == "SUPORTE":
        candidatos = [g for g in niveis if g["nivel"] <= preco]
        if not candidatos:
            candidatos = niveis
        return min(candidatos, key=lambda g: abs(preco - g["nivel"]))

    candidatos = [g for g in niveis if g["nivel"] >= preco]
    if not candidatos:
        candidatos = niveis
    return min(candidatos, key=lambda g: abs(preco - g["nivel"]))


def _resultado_retracao_intravela(msg, active_id):
    """S/R M5 + rejeição/retração na MESMA vela M5.

    CALL:
      - existe suporte M5 com >= 3 toques anteriores;
      - a vela atual abriu suficientemente acima do suporte;
      - durante a própria vela o preço toca a região;
      - rejeita para cima e confirma a retração;
      - a entrada vence no fechamento desta mesma vela.

    PUT:
      - mesma lógica invertida em resistência M5.

    Impulso/retração sem S/R M5 válido nunca gera entrada.
    """
    if not isinstance(msg, dict):
        return None

    try:
        abertura = float(msg["open"])
        fechamento = float(msg["close"])
        maxima = float(msg.get("max", msg.get("high")))
        minima = float(msg.get("min", msg.get("low")))
        candle_from = int(float(msg["from"]))
        candle_to = int(float(msg.get("to") or (candle_from + 300)))
    except Exception:
        return None

    server_ts, _ = _horario_servidor_atual()
    decorridos = max(0.0, server_ts - candle_from)
    restantes = max(0.0, candle_to - server_ts)

    if decorridos < INTRAVELA_MIN_SEGUNDOS_DECORRIDOS:
        return None
    if restantes < INTRAVELA_MIN_SEGUNDOS_RESTANTES:
        return None

    suportes, resistencias, atr5 = _niveis_sr_m5(active_id)
    if atr5 is None or atr5 <= 0:
        return None

    if not suportes and not resistencias:
        return None

    tolerancia_nivel = atr5 * SR_M5_TOLERANCIA_ATR
    distancia_minima_abertura = atr5 * SR_M5_DISTANCIA_ABERTURA_ATR_MIN

    candle_key = (int(active_id), candle_from)

    with _intravela_lock:
        st = _intravela_estado.setdefault(
            candle_key,
            {
                "ultimo_close": fechamento,
                "eventos": 0,
                "maxima": maxima,
                "minima": minima,
            }
        )
        ultimo_close = float(st.get("ultimo_close", fechamento))
        st["eventos"] = int(st.get("eventos", 0)) + 1
        st["maxima"] = max(float(st.get("maxima", maxima)), maxima)
        st["minima"] = min(float(st.get("minima", minima)), minima)
        st["ultimo_close"] = fechamento
        eventos = st["eventos"]

    # Precisa observar pelo menos uma atualização anterior da mesma vela.
    if eventos < 2:
        return None

    # --------------------------------------------------------
    # CALL: rejeição de suporte M5
    # --------------------------------------------------------
    suporte = _nivel_mais_proximo(suportes, minima, "SUPORTE")
    if suporte is not None:
        nivel = float(suporte["nivel"])
        distancia_abertura = abertura - nivel
        tocou = minima <= nivel + tolerancia_nivel
        veio_de_longe = distancia_abertura >= distancia_minima_abertura

        movimento = max(0.0, abertura - minima)
        rejeicao = fechamento - minima
        ratio = rejeicao / max(movimento, 1e-12)
        pavio_inf = min(abertura, fechamento) - minima

        confirmou = (
            tocou
            and veio_de_longe
            and fechamento > ultimo_close
            and INTRAVELA_RETRACAO_MIN <= ratio <= INTRAVELA_RETRACAO_MAX
            and rejeicao >= atr5 * INTRAVELA_REJEICAO_ATR_MIN
            and pavio_inf >= max(
                movimento * INTRAVELA_PAVIO_MIN_FRACAO_MOVIMENTO,
                atr5 * 0.04,
            )
        )

        if confirmou:
            score = 10
            if suporte["toques"] >= 4:
                score += 1
            if distancia_abertura >= atr5 * 0.90:
                score += 1

            return {
                "sinal": "CALL",
                "score": score,
                "score_call": score,
                "score_put": 1,
                "preco": fechamento,
                "vela": datetime.fromtimestamp(candle_from, TZ),
                "estrategia": "SR_M5_RETRACAO_MESMA_VELA",
                "regime": "INTRAVELA",
                "pullback": f"REJEICAO SUPORTE M5 | retração={ratio*100:.1f}%",
                "rejeicao": "CONFIRMADA",
                "lateral": "N/A",
                "atr": atr5,
                "rsi": None,
                "ema5": None,
                "ema13": None,
                "ema21": None,
                "tendencia_5m": "S/R M5",
                "tendencia_15m": "NÃO UTILIZADO",
                "zona_fibonacci": f"SUPORTE M5 {nivel:.5f} ({suporte['toques']} toques)",
                "bloqueio": "SINAL",
                "mensagem": (
                    f"CALL | suporte M5={nivel:.5f} | "
                    f"toques={suporte['toques']} | "
                    f"dist_abertura={distancia_abertura:.6f} | "
                    f"retração={ratio*100:.1f}% | "
                    f"restam={restantes:.1f}s"
                ),
                "candle_from": candle_from,
                "candle_to": candle_to,
                "segundos_decorridos": decorridos,
                "segundos_restantes": restantes,
                "impulso": movimento,
                "retracao_ratio": ratio,
                "nivel_sr": nivel,
                "tipo_nivel": "SUPORTE_M5",
                "toques_nivel": suporte["toques"],
                "distancia_abertura_nivel": distancia_abertura,
            }

    # --------------------------------------------------------
    # PUT: rejeição de resistência M5
    # --------------------------------------------------------
    resistencia = _nivel_mais_proximo(
        resistencias,
        maxima,
        "RESISTENCIA",
    )
    if resistencia is not None:
        nivel = float(resistencia["nivel"])
        distancia_abertura = nivel - abertura
        tocou = maxima >= nivel - tolerancia_nivel
        veio_de_longe = distancia_abertura >= distancia_minima_abertura

        movimento = max(0.0, maxima - abertura)
        rejeicao = maxima - fechamento
        ratio = rejeicao / max(movimento, 1e-12)
        pavio_sup = maxima - max(abertura, fechamento)

        confirmou = (
            tocou
            and veio_de_longe
            and fechamento < ultimo_close
            and INTRAVELA_RETRACAO_MIN <= ratio <= INTRAVELA_RETRACAO_MAX
            and rejeicao >= atr5 * INTRAVELA_REJEICAO_ATR_MIN
            and pavio_sup >= max(
                movimento * INTRAVELA_PAVIO_MIN_FRACAO_MOVIMENTO,
                atr5 * 0.04,
            )
        )

        if confirmou:
            score = 10
            if resistencia["toques"] >= 4:
                score += 1
            if distancia_abertura >= atr5 * 0.90:
                score += 1

            return {
                "sinal": "PUT",
                "score": score,
                "score_call": 1,
                "score_put": score,
                "preco": fechamento,
                "vela": datetime.fromtimestamp(candle_from, TZ),
                "estrategia": "SR_M5_RETRACAO_MESMA_VELA",
                "regime": "INTRAVELA",
                "pullback": f"REJEICAO RESISTENCIA M5 | retração={ratio*100:.1f}%",
                "rejeicao": "CONFIRMADA",
                "lateral": "N/A",
                "atr": atr5,
                "rsi": None,
                "ema5": None,
                "ema13": None,
                "ema21": None,
                "tendencia_5m": "S/R M5",
                "tendencia_15m": "NÃO UTILIZADO",
                "zona_fibonacci": f"RESISTENCIA M5 {nivel:.5f} ({resistencia['toques']} toques)",
                "bloqueio": "SINAL",
                "mensagem": (
                    f"PUT | resistência M5={nivel:.5f} | "
                    f"toques={resistencia['toques']} | "
                    f"dist_abertura={distancia_abertura:.6f} | "
                    f"retração={ratio*100:.1f}% | "
                    f"restam={restantes:.1f}s"
                ),
                "candle_from": candle_from,
                "candle_to": candle_to,
                "segundos_decorridos": decorridos,
                "segundos_restantes": restantes,
                "impulso": movimento,
                "retracao_ratio": ratio,
                "nivel_sr": nivel,
                "tipo_nivel": "RESISTENCIA_M5",
                "toques_nivel": resistencia["toques"],
                "distancia_abertura_nivel": distancia_abertura,
            }

    return None


def _atualizar_dashboard_intravela(symbol, resultado):
    estado["ativo"] = symbol
    estado["sinal"] = resultado.get("sinal", "AGUARDAR")
    estado["score"] = resultado.get("score", 0)
    estado["preco"] = f"{float(resultado.get('preco', 0)):.5f}"
    vela = resultado.get("vela")
    estado["vela"] = (
        vela.strftime("%Y-%m-%d %H:%M:%S BRT")
        if isinstance(vela, datetime) else "-"
    )
    estado["atualizado"] = agora_brt().strftime("%H:%M:%S BRT")
    estado["atualidade_min"] = "TEMPO REAL"
    estado["mensagem"] = resultado.get("mensagem", "")
    estado["detalhes"] = {
        "score_call": resultado.get("score_call", "-"),
        "score_put": resultado.get("score_put", "-"),
        "rsi": "-",
        "ema5": "-",
        "ema13": "-",
        "ema21": "-",
        "tendencia_5m": "N/A",
        "tendencia_15m": "N/A",
        "pullback": resultado.get("pullback", "-"),
        "confirmacao": resultado.get("rejeicao", "-"),
        "lateral": "N/A",
        "atr": (
            f"{resultado['atr']:.6f}"
            if isinstance(resultado.get("atr"), (int, float)) else "-"
        ),
        "bloqueio": resultado.get("bloqueio", "-"),
        "regime": "INTRAVELA",
        "estrategia": "SR_M5_RETRACAO_MESMA_VELA",
        "zona_fibonacci": "-",
    }


def _processar_sinal_intravela(active_id, msg):
    codigo, symbol = _symbol_por_active_id(active_id)
    if not codigo or not symbol:
        return

    if not dentro_do_horario():
        return

    resultado = _resultado_retracao_intravela(msg, active_id)
    if resultado is None:
        return

    candle_key = (int(active_id), int(resultado["candle_from"]))

    with _intravela_lock:
        if candle_key in _intravela_velas_tentadas:
            return
        _intravela_velas_tentadas.add(candle_key)

    _atualizar_dashboard_intravela(symbol, resultado)

    # O Telegram recebe o SINAL assim que o setup técnico é confirmado.
    # Isso independe de a Bullex aceitar ou recusar a ordem depois.
    threading.Thread(
        target=enviar_sinal_telegram,
        args=(symbol, resultado),
        daemon=True,
        name=f"telegram-sinal-{codigo}-{resultado['candle_from']}",
    ).start()

    log(
        f"[INTRAVELA] {symbol} [{_mercado_do_symbol(symbol)}] -> {resultado['sinal']} | "
        f"score={resultado['score']} | "
        f"nivel={resultado.get('tipo_nivel')} "
        f"{resultado.get('nivel_sr', 0):.5f} | "
        f"toques={resultado.get('toques_nivel')} | "
        f"dist_abertura={resultado.get('distancia_abertura_nivel', 0):.6f} | "
        f"{resultado['pullback']} | "
        f"decorridos={resultado['segundos_decorridos']:.1f}s | "
        f"restantes={resultado['segundos_restantes']:.1f}s | "
        f"preco={resultado['preco']:.5f}"
    )

    # Nunca bloqueia o callback do WebSocket esperando a resposta da ordem.
    threading.Thread(
        target=registrar_operacao_intravela,
        args=(symbol, resultado),
        daemon=True,
        name=f"intravela-order-{codigo}-{resultado['candle_from']}",
    ).start()


def calcular_estatisticas_por_estrategia():
    wins = losses = dojis = 0
    for item in _historico_resultados:
        if item.get("estrategia") != "SR_M5_RETRACAO_MESMA_VELA":
            continue
        r = item.get("resultado")
        if r == "WIN":
            wins += 1
        elif r == "LOSS":
            losses += 1
        elif r == "DOJI":
            dojis += 1
    total = wins + losses + dojis
    decididos = wins + losses
    return {
        "SR_M5_RETRACAO_MESMA_VELA": {
            "total": total,
            "wins": wins,
            "losses": losses,
            "dojis": dojis,
            "taxa": round(wins / decididos * 100 if decididos else 0.0, 2),
        }
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


def _mercado_do_symbol(symbol):
    texto = str(symbol or "").upper()
    return "OTC" if "OTC" in texto else "ABERTO"


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
        f"Mercado: {_mercado_do_symbol(symbol)}\n"
        f"Direcao: {sinal}\n"
        f"Score: {resultado.get('score', 0)}\n"
        f"Estrategia: {resultado.get('estrategia', '-')}\n"
        f"Regime: {resultado.get('regime', '-')}\n"
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
        f"➡️ ENTRADA: RETRACAO NA VELA ATUAL\n"
        f"⏱️ EXPIRACAO: FIM DA MESMA VELA M5\n\n"
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


def enviar_status_ordem_telegram(symbol, sinal, status, detalhe=""):
    mercado = _mercado_do_symbol(symbol)
    icone = "✅" if status == "CONFIRMADA" else "⚠️"
    texto = (
        f"{icone} STATUS DA ORDEM\n\n"
        f"Ativo: {symbol}\n"
        f"Mercado: {mercado}\n"
        f"Direcao: {sinal}\n"
        f"Status: {status}\n"
    )
    if detalhe:
        texto += f"Detalhe: {detalhe}\n"
    enviar_telegram(texto)


def registrar_operacao_intravela(symbol, resultado):
    sinal = resultado.get("sinal")
    if sinal not in ("CALL", "PUT"):
        return

    candle_from = int(resultado["candle_from"])
    candle_dt = datetime.fromtimestamp(candle_from, TZ)
    chave = f"{symbol}|INTRAVELA|{candle_from}"

    if _ultimas_operacoes_registradas.get(symbol) == chave:
        return

    if symbol in _operacoes_pendentes:
        return

    status = executar_ordem_intravela(symbol, sinal, resultado)
    if status != "CONFIRMADA":
        return

    with _execucao_lock:
        info = (_operacao_global_ativa or {}).copy()

    operacao = {
        "id": chave,
        "symbol": symbol,
        "mercado": _mercado_do_symbol(symbol),
        "sinal": sinal,
        "score": resultado.get("score", 0),
        "estrategia": "SR_M5_RETRACAO_MESMA_VELA",
        "regime": "INTRAVELA",
        "preco_sinal": float(resultado["preco"]),
        "vela_sinal": candle_dt,
        "vela_entrada": candle_dt,
        "vela_expiracao": candle_dt,
        "entrada": float(resultado["preco"]),
        "saida": None,
        "resultado": "PENDENTE",
        "ordem_automatica": True,
        "valor": info.get("valor", _valor_entrada_atual()),
        "balance_id": info.get("balance_id"),
        "produto": info.get("produto", "BINARIA_INTRAVELA"),
        "option_id": info.get("option_id"),
        "candle_to": int(resultado["candle_to"]),
        "retracao_ratio": resultado.get("retracao_ratio"),
        "impulso": resultado.get("impulso"),
        "nivel_sr": resultado.get("nivel_sr"),
        "tipo_nivel": resultado.get("tipo_nivel"),
        "toques_nivel": resultado.get("toques_nivel"),
        "distancia_abertura_nivel": resultado.get("distancia_abertura_nivel"),
    }

    _operacoes_pendentes[symbol] = operacao
    _ultimas_operacoes_registradas[symbol] = chave

    log(
        f"[INTRAVELA] {symbol}: operação registrada {sinal} | "
        f"entrada={operacao['entrada']:.5f} | "
        f"expira={datetime.fromtimestamp(int(resultado['candle_to']), TZ).strftime('%H:%M:%S')}"
    )





# ============================================================
# AVALIAR WIN / LOSS
# ============================================================

def avaliar_operacao(symbol, candles):
    global _operacao_global_ativa

    operacao = _operacoes_pendentes.get(symbol)
    if not operacao:
        return

    agora = agora_brt()
    alvo_dt = operacao["vela_expiracao"]

    for candle in ordenar_candles(candles):
        dt = candle["_dt"]
        if dt != alvo_dt:
            continue

        if dt + timedelta(minutes=5) > agora:
            return

        info = candle_info(candle)
        entrada = float(operacao.get("entrada") or operacao.get("preco_sinal"))
        saida = info["close"]

        operacao["entrada"] = entrada
        operacao["saida"] = saida

        if operacao["sinal"] == "CALL":
            resultado = "WIN" if saida > entrada else "LOSS" if saida < entrada else "DOJI"
        else:
            resultado = "WIN" if saida < entrada else "LOSS" if saida > entrada else "DOJI"

        operacao["resultado"] = resultado
        operacao["finalizado_em"] = agora
        _historico_resultados.append(operacao.copy())
        del _operacoes_pendentes[symbol]

        with _execucao_lock:
            if (
                _operacao_global_ativa is not None
                and _operacao_global_ativa.get("symbol") == symbol
            ):
                _operacao_global_ativa = None

        _atualizar_progressao(resultado)
        _atualizar_estado_execucao()

        estatisticas = calcular_estatisticas()

        log(
            f"[RESULTADO INTRAVELA] {symbol} {operacao['sinal']} -> {resultado} | "
            f"entrada={entrada:.5f} | fechamento_mesma_vela={saida:.5f} | "
            f"taxa_total={estatisticas['taxa']:.2f}%"
        )

        enviar_resultado_telegram(operacao, estatisticas)
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
        f"Mercado: {_mercado_do_symbol(operacao['symbol'])}\n"
        f"Direcao: {operacao['sinal']}\n"
        f"Estrategia: {operacao.get('estrategia', '-')}\n"
        f"Regime: {operacao.get('regime', '-')}\n"
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
# FINALIZAR OPERAÇÕES VENCIDAS ANTES DE PROCURAR NOVOS SINAIS
# ============================================================

def finalizar_operacoes_vencidas_antes_da_leitura():
    """Libera a trava global antes de analisar a nova vela.

    Isso evita perder um sinal de outro par só porque a operação anterior
    ainda seria finalizada mais tarde na ordem do loop dos ativos.
    """
    if not _operacoes_pendentes:
        return

    pendentes = list(_operacoes_pendentes.keys())

    for symbol in pendentes:
        try:
            candles_5m = obter_candles(
                symbol,
                TIMEFRAME,
                OUTPUTSIZE
            )
            avaliar_operacao(
                symbol,
                candles_5m
            )
        except Exception as e:
            log(
                f"[TRAVA] Não foi possível avaliar operação pendente de "
                f"{symbol} antes da nova leitura: {e}"
            )


# ============================================================
# PROCESSAR ATIVO
# ============================================================

def processar_ativo(chave, symbol, executar_sinal=False):
    """Na R13 o loop de 5 minutos não cria sinais.

    Ele apenas mantém histórico atualizado e finaliza operações.
    Os sinais surgem exclusivamente do candle-generated da vela corrente.
    """
    with _bullex_assets_lock:
        config = ATIVO_BULLEX.get(chave)

    if not config:
        return None

    try:
        candles_5m = obter_candles(symbol, TIMEFRAME, OUTPUTSIZE)
        avaliar_operacao(symbol, candles_5m)

        ultimo, idade = idade_do_ultimo_candle(candles_5m)
        if ultimo is not None:
            estado["ativo"] = symbol
            estado["preco"] = f"{float(ultimo['close']):.5f}"
            estado["atualizado"] = agora_brt().strftime("%H:%M:%S BRT")
            estado["atualidade_min"] = f"{idade:.1f} min" if idade is not None else "-"
            if estado.get("sinal") not in ("CALL", "PUT"):
                estado["sinal"] = "AGUARDAR"
                estado["mensagem"] = "Monitorando retração na vela atual em tempo real."
        return None

    except Exception as e:
        log(f"ERRO manutenção {symbol}: {e}")
        return None




# ============================================================
# HORÁRIO
# ============================================================

def dentro_do_horario():
    hora = agora_brt().hour

    if HORA_INICIO < HORA_FIM:
        return HORA_INICIO <= hora < HORA_FIM

    if HORA_INICIO > HORA_FIM:
        return hora >= HORA_INICIO or hora < HORA_FIM

    return True


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

    if not _aguardar_ativos_mercado_aberto(timeout=8):
        erro_ativos = _bullex_assets_last_error or "aguardando resposta da Traderoom"
        log(
            "[OPEN MARKET] Leitura adiada: active_id dos pares ainda não está pronto. "
            f"Detalhe: {erro_ativos}"
        )
        estado["sinal"] = "AGUARDAR"
        estado["score"] = 0
        estado["mensagem"] = (
            "Aguardando carregamento dos pares de mercado aberto na Bullex."
        )
        estado["atualizado"] = agora_brt().strftime("%H:%M:%S BRT")
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

    # A R17 gera sinais EXCLUSIVAMENTE em tempo real no candle-generated.
    # Este ciclo de 5 minutos apenas finaliza/atualiza operações e saúde dos ativos.
    finalizar_operacoes_vencidas_antes_da_leitura()

    with _bullex_assets_lock:
        ativos_ciclo = list(ATIVOS.items())
        qtd_aberto = sum(
            1 for cfg in ATIVO_BULLEX.values()
            if not cfg.get("is_otc")
        )
        qtd_otc = sum(
            1 for cfg in ATIVO_BULLEX.values()
            if cfg.get("is_otc")
        )

    log(
        f"[MONITOR OTC] ativos mapeados={len(ativos_ciclo)} | "
        f"ABERTO={qtd_aberto} | OTC={qtd_otc} | "
        "sinais=intravela/candle-generated"
    )

    for chave, symbol in ativos_ciclo:
        processar_ativo(chave, symbol, executar_sinal=False)

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
        _aguardar_autenticacao(timeout=20)

        log(
            "Thread persistente do WebSocket Bullex iniciada e autenticada."
        )

        if _aguardar_ativos_mercado_aberto(timeout=30):
            with _bullex_assets_lock:
                ativos_prontos = ", ".join(ATIVO_BULLEX.keys())
            log(f"[OPEN MARKET] Pronto para leitura: {ativos_prontos}")
        else:
            log(
                "[OPEN MARKET] Inicialização ainda incompleta; "
                "a primeira leitura ficará em AGUARDAR, sem gerar KeyError."
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

Estratégia única: S/R M5 + retração intravela na mesma vela

</div>

<div class="card">
<h3>Execução automática DEMO</h3>
<div class="linha"><span>Modo</span><span class="valor">{{ estado.execucao.modo }}</span></div>
<div class="linha"><span>Automática</span><span class="valor">{{ "ATIVA" if estado.execucao.automatica else "DESATIVADA" }}</span></div>
<div class="linha"><span>Entrada atual</span><span class="valor">R$ {{ "%.2f"|format(estado.execucao.valor_atual) }}</span></div>
<div class="linha"><span>Progressão</span><span class="valor">DESATIVADA (R$ 5,00 FIXO)</span></div>
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
<span>Regime</span>
<span class="valor">{{ estado.detalhes.regime }}</span>
</div>

<div class="linha">
<span>Estratégia</span>
<span class="valor">{{ estado.detalhes.estrategia }}</span>
</div>

<div class="linha">
<span>Nível M15</span>
<span class="valor">MESMA VELA 5M</span>
</div>

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
                "S/R M5 + retracao intravela na mesma vela M5 | SOMENTE OTC"
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
        "entrada_fixa": 5.00,
        "progressao_ativa": False,
        "mercado": "ABERTO",
        "ativos_mercado_aberto": {
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
        "estatisticas":
            calcular_estatisticas(),
        "estatisticas_por_estrategia":
            calcular_estatisticas_por_estrategia(),
    })


# ============================================================
# EXECUÇÃO
# ============================================================

_atualizar_estado_execucao()

log(f"AUTO TRADE DEMO={'ATIVO' if BULLEX_AUTO_TRADE else 'DESATIVADO'} | entrada fixa=R${_valor_entrada_atual():.2f} | progressao=DESATIVADA")
log(f"BULLEX_USER_BALANCE_ID={'CONFIGURADO' if BULLEX_USER_BALANCE_ID else 'AUSENTE'}")

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
