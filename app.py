import os
import time
import threading
import secrets
from datetime import datetime, timedelta, timezone
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

# SOMENTE OTC. Primeiro usamos os active_id conhecidos como fallback e,
# após autenticar, a Traderoom pode atualizar os ids/tickers dinamicamente.
ATIVO_BULLEX = {
    "EURUSD_OTC": {"symbol": "EUR/USD OTC", "active_id": 76, "ticker": "EURUSD-OTC", "is_otc": True, "mercado": "OTC"},
    "GBPUSD_OTC": {"symbol": "GBP/USD OTC", "active_id": 81, "ticker": "GBPUSD-OTC", "is_otc": True, "mercado": "OTC"},
    "USDJPY_OTC": {"symbol": "USD/JPY OTC", "active_id": 85, "ticker": "USDJPY-OTC", "is_otc": True, "mercado": "OTC"},
    "GBPJPY_OTC": {"symbol": "GBP/JPY OTC", "active_id": 84, "ticker": "GBPJPY-OTC", "is_otc": True, "mercado": "OTC"},
    "AUDCAD_OTC": {"symbol": "AUD/CAD OTC", "active_id": 86, "ticker": "AUDCAD-OTC", "is_otc": True, "mercado": "OTC"},
    "USDCHF_OTC": {"symbol": "USD/CHF OTC", "active_id": 78, "ticker": "USDCHF-OTC", "is_otc": True, "mercado": "OTC"},
}

PARES_MERCADO_ABERTO = {}

PARES_OTC_ALVO = {
    "EURUSD": "EUR/USD OTC",
    "GBPUSD": "GBP/USD OTC",
    "USDJPY": "USD/JPY OTC",
    "GBPJPY": "GBP/JPY OTC",
    "AUDCAD": "AUD/CAD OTC",
    "AUDNZD": "AUD/NZD OTC",
    "USDCHF": "USD/CHF OTC",
}

_bullex_assets_lock = threading.RLock()
_bullex_assets_detected = True
_bullex_assets_last_error = None
_bullex_assets_updated_at = None
_bullex_assets_source = "DIGITAL_OTC_DYNAMIC_ALL"
_bullex_assets_ready_event = threading.Event()
_bullex_assets_init_lock = threading.Lock()

_BULLEX_CANDLE_SIZES = {"1min": 60, "5min": 300, "15min": 900}

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
BULLEX_DIAGNOSTIC_VERSION = "OTC-M1-R40-RADAR-AO-VIVO-CORRIGIDO-20260918"

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

TIMEFRAME = "1min"
TIMEFRAME_TREND = "1min"

TIMEZONE = "America/Sao_Paulo"
TZ = ZoneInfo(TIMEZONE)

OUTPUTSIZE = 150
OUTPUTSIZE_15M = 100

HORA_INICIO = 0
HORA_FIM = 0  # R25: sem restrição de horário; dentro_do_horario() sempre True

MAX_ATRASO_MINUTOS = 8

# ============================================================
# EXECUÇÃO AUTOMÁTICA - DEMO
# ============================================================

BULLEX_AUTO_TRADE = False  # R39: painel manual; não executa ordens automaticamente

BULLEX_USER_BALANCE_ID = os.getenv(
    "BULLEX_USER_BALANCE_ID",
    ""
).strip()

VALORES_ENTRADA = [5.00]
VALOR_GALE = 6.00
# Se a Bullex não devolver o payout no retorno da ordem, usa este valor apenas como fallback.
BULLEX_PAYOUT_FALLBACK = float(os.getenv("BULLEX_PAYOUT_FALLBACK", "87").strip() or "87")
EXPIRACAO_MINUTOS = 15
# A antiga janela de 3 segundos foi removida.
# Esta estratégia entra DURANTE a vela atual e expira no fechamento da MESMA vela.
INTRAVELA_MIN_SEGUNDOS_DECORRIDOS = 8
INTRAVELA_MAX_SEGUNDOS_DECORRIDOS = 48
INTRAVELA_MIN_SEGUNDOS_RESTANTES = 7
# R38: entrada na retração durante a vela M1; expiração no fechamento da própria vela.

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

# R30 - M15: suporte/resistência + LTA/LTB + retração intravela, sem Martingale.
SR_M15_LOOKBACK = 100
SR_M15_PIVOT_JANELA = 2
SR_M15_MIN_TOQUES = 3
SR_M15_TOLERANCIA_ATR = 0.18
M15_DISTANCIA_ABERTURA_ATR_MIN = 0.45
M15_TOQUE_TOLERANCIA_ATR = 0.20
M15_REJEICAO_ATR_MIN = 0.10
M15_RETRACAO_MIN = 0.18
M15_RETRACAO_MAX = 0.72
M15_LINHA_TOLERANCIA_ATR = 0.22
MARTINGALE_ATIVO = False

MAX_OPERACOES_GLOBAIS = 2
MAX_OPERACOES_POR_ATIVO = 1
AUTONOMO_MIN_AMOSTRAS = 45
AUTONOMO_K_VIZINHOS = 17
AUTONOMO_CONFIANCA_MIN = 0.62
AUTONOMO_MARGEM_MIN = 0.12
# R6: só abre a primeira entrada quando padrões históricos semelhantes mostram
# boa chance de encerrar o ciclo em WIN na primeira ou no único Gale.
AUTONOMO_CICLO_CONFIANCA_MIN = 0.70
AUTONOMO_CICLO_MIN_VIZINHOS = 12

# R7: 2 ciclos completos perdedores seguidos bloqueiam somente o ativo por 1 hora.
BLOQUEIO_ATIVO_CICLOS_LOSS = 2
BLOQUEIO_ATIVO_SEGUNDOS = 60 * 60

# R2 - segunda camada de aprendizado: aprende quando NÃO operar.
# O KNN continua escolhendo CALL/PUT; esta camada mede o desempenho REAL
# por faixa de confiança e por ativo. Só bloqueia depois de ter amostra mínima.
AUTONOMO_ADAPTATIVO_ATIVO_MIN = 6
AUTONOMO_ADAPTATIVO_FAIXA_MIN = 12
AUTONOMO_ADAPTATIVO_TAXA_BLOQUEIO = 0.48
AUTONOMO_ADAPTATIVO_ULTIMAS = 80

# R12 - confirmação adicional sem alterar a execução DIGITAL/Gale.
# Tendência M15 só bloqueia quando estiver FORTE e contra o sinal.
AUTONOMO_M15_ADX_FORTE = 25.0
AUTONOMO_M15_PENALIDADE_CONTRA_FRACA = 0.04
# Força mínima do M5: evita mercado praticamente parado.
AUTONOMO_M5_ADX_MIN = 12.0
# Resultados REAIS dos ciclos encerrados passam a calibrar a confiança de ciclo.
AUTONOMO_REAL_CICLOS_MIN = 10
AUTONOMO_REAL_CICLO_BLOQUEIO = 0.48
AUTONOMO_REAL_AJUSTE_MAX = 0.15

_intravela_lock = threading.RLock()
_intravela_estado = {}
_intravela_velas_tentadas = set()

# ============================================================
# ATIVOS
# ============================================================

ATIVOS = {
    "EURUSD_OTC": "EUR/USD OTC",
    "GBPUSD_OTC": "GBP/USD OTC",
    "USDJPY_OTC": "USD/JPY OTC",
    "GBPJPY_OTC": "GBP/JPY OTC",
    "AUDCAD_OTC": "AUD/CAD OTC",
    "AUDNZD_OTC": "AUD/NZD OTC",
    "USDCHF_OTC": "USD/CHF OTC",
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
    "modo": "DEMO DIGITAL",
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
# R22 inicia a estatística da estratégia do zero.
# Somente resultados realmente obtidos após este deploy entram na contagem.
_historico_resultados = []
_execucao_lock = threading.RLock()
_operacao_global_ativa = None
# R24: também bloqueia novas ordens enquanto uma ordem está sendo enviada,
# evitando corrida entre dois ativos que sinalizem praticamente ao mesmo tempo.
_operacao_global_em_envIO_LEGACY = False
_operacoes_ativas_por_symbol = {}
_operacoes_em_envio = set()
_active_ids_em_envio = set()  # trava forte: nunca envia 2 ordens para o mesmo active_id

def _qtd_operacoes_globais_em_andamento():
    """Conta símbolos únicos ativos, em envio ou aguardando resultado."""
    return len(
        set(_operacoes_ativas_por_symbol.keys())
        | set(_operacoes_em_envio)
        | set(_operacoes_pendentes.keys())
    )

def _ha_vaga_operacao_global():
    return _qtd_operacoes_globais_em_andamento() < int(MAX_OPERACOES_GLOBAIS)
_gales_pendentes = {}  # symbol -> dados do Gale 1 para a próxima vela M5
_bloqueios_por_symbol = {}  # symbol -> {ate_ts, motivo, sequencia}
_sequencia_ciclos_loss = {}  # symbol -> ciclos completos perdidos em sequência

# R24: candidatos da mesma abertura M5 são comparados antes da execução.
# Uma pequena janela de coleta permite escolher o setup mais forte sem atrasar
# a entrada para o meio da vela.
_r24_candidatos_lock = threading.RLock()
_r24_candidatos = {}
_r24_dispatchers = set()
_r24_velas_finalizadas = set()
R24_JANELA_CLASSIFICACAO_SEGUNDOS = 7.00
_nivel_progressao = 0
_bullex_balance_id = None
_bullex_balance_source = None
_bullex_instrument_cache = {}
_bullex_instrument_event = threading.Event()
_bullex_digital_position_events = {}
_digital_raw_diag_count = 0
DIGITAL_RAW_DIAG_MAX = 4

# ============================================================
# R22 - PRELOAD OBRIGATÓRIO M5 + M15
# ============================================================
_historico_pronto_event = threading.Event()
_historico_preload_lock = threading.Lock()
_historico_preload_status = {}
_historico_preload_ultima_tentativa = None

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
        or "digital-options.place-digital-option" in bruto
        or "position_id" in bruto
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
    return float(VALORES_ENTRADA[0])

def _valor_gale_atual():
    return float(VALOR_GALE)

def _extrair_payout_percent(obj):
    """Procura um percentual de payout/profit retornado pela Bullex."""
    chaves = {"payout", "payout_percent", "payout_percentage", "profit_percent", "profit_percentage"}
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if str(k).lower() in chaves:
                    try:
                        n = float(v)
                        if 0 < n <= 1:
                            n *= 100.0
                        if 1 <= n <= 100:
                            return n
                    except Exception:
                        pass
            for v in x.values():
                r = walk(v)
                if r is not None:
                    return r
        elif isinstance(x, list):
            for v in x:
                r = walk(v)
                if r is not None:
                    return r
        return None
    return walk(obj)

def calcular_financeiro():
    lucro = 0.0
    wins = losses = 0
    for op in _historico_resultados:
        r = op.get("resultado")
        valor = float(op.get("valor", 0.0) or 0.0)
        payout = float(op.get("payout_percent", BULLEX_PAYOUT_FALLBACK) or BULLEX_PAYOUT_FALLBACK)
        if r == "WIN":
            lucro += valor * payout / 100.0
            wins += 1
        elif r == "LOSS":
            lucro -= valor
            losses += 1
    return {"lucro_total": round(lucro, 2), "wins": wins, "losses": losses}



def _atualizar_estado_execucao():
    estado["execucao"].update({
        "automatica": BULLEX_AUTO_TRADE,
        "modo": "DEMO DIGITAL",
        "valor_atual": _valor_entrada_atual(),
        "nivel_progressao": _nivel_progressao,
        "operacao_ativa": bool(_operacoes_ativas_por_symbol),
        "operacoes_ativas": len(_operacoes_ativas_por_symbol),
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


def _sanitizar_digital_raw(obj, profundidade=0):
    """Remove segredos antes de registrar uma amostra do catálogo DIGITAL."""
    if profundidade > 7:
        return "<max-depth>"
    segredos = {
        "ssid", "cookie", "authorization", "token", "access_token",
        "refresh_token", "session", "session_id", "client_session_id",
        "password", "passwd", "secret", "api_key", "apikey",
    }
    if isinstance(obj, dict):
        saida = {}
        for k, v in list(obj.items())[:40]:
            chave = str(k)
            if chave.lower() in segredos or any(x in chave.lower() for x in ("token", "secret", "password", "cookie")):
                saida[chave] = "<redacted>"
            else:
                saida[chave] = _sanitizar_digital_raw(v, profundidade + 1)
        return saida
    if isinstance(obj, list):
        return [_sanitizar_digital_raw(v, profundidade + 1) for v in obj[:8]]
    if isinstance(obj, str) and len(obj) > 500:
        return obj[:500] + "...<truncated>"
    return obj


def _log_digital_raw(data):
    """Mostra poucas respostas reais de instruments para ajustar o parser sem vazar sessão."""
    global _digital_raw_diag_count
    if _digital_raw_diag_count >= DIGITAL_RAW_DIAG_MAX:
        return
    _digital_raw_diag_count += 1
    try:
        seguro = _sanitizar_digital_raw(data)
        texto = json.dumps(seguro, ensure_ascii=False, separators=(",", ":"))
        if len(texto) > 7000:
            texto = texto[:7000] + "...<truncated>"
        log(f"[DIGITAL RAW {_digital_raw_diag_count}/{DIGITAL_RAW_DIAG_MAX}] {texto}")
    except Exception as e:
        log(f"[DIGITAL RAW] falha ao serializar diagnóstico: {e}")


def _armazenar_instrumentos_digitais(data):
    """Armazena a lista REAL `name=instruments` recebida da Traderoom.

    Cada item traz index, asset_id, expiration, period e data[] com os
    symbols CALL/PUT. Para a estratégia usamos somente period=300 e strike=SPT.
    """
    if not isinstance(data, dict) or data.get("name") != "instruments":
        return 0
    msg = data.get("msg")
    if not isinstance(msg, dict):
        return 0
    instrumentos = msg.get("instruments")
    if not isinstance(instrumentos, list):
        return 0
    gravados = 0
    with _bullex_cv:
        for inst in instrumentos:
            if not isinstance(inst, dict):
                continue
            try:
                asset_id = int(inst.get("asset_id"))
                expiration = int(inst.get("expiration"))
                period = int(inst.get("period"))
                index = int(inst.get("index"))
            except (TypeError, ValueError):
                continue
            for opcao in inst.get("data") or []:
                if not isinstance(opcao, dict) or str(opcao.get("strike")) != "SPT":
                    continue
                direction = str(opcao.get("direction") or "").lower()
                symbol = opcao.get("symbol")
                if direction not in ("call", "put") or not symbol:
                    continue
                key = (asset_id, expiration, period, direction)
                _bullex_instrument_cache[key] = {
                    "instrument_id": str(symbol),
                    "instrument_index": index,
                    "asset_id": asset_id,
                    "expiration": expiration,
                    "period": period,
                    "direction": direction,
                    "source": "BULLEX_INSTRUMENTS_REAL",
                }
                gravados += 1
        if gravados:
            _bullex_instrument_event.set()
            _bullex_cv.notify_all()
    return gravados


def _instrumento_digital_cache(active_id, sinal, candle_to):
    """Localiza um contrato DIGITAL M15 REAL recebido da Bullex.

    Prioriza o vencimento exato da vela. Se a Traderoom publicar o mesmo
    contrato M15 com vencimento ligeiramente diferente, aceita somente um
    vencimento FUTURO real, dentro de uma janela máxima de 15 minutos.
    Nunca inventa instrument_id/index.
    """
    direction = _direcao_instrumento(sinal)
    active_id = int(active_id)
    candle_to = int(candle_to)
    key = (active_id, candle_to, 900, direction)
    item = _bullex_instrument_cache.get(key)
    if item:
        return item

    server_ts, _ = _horario_servidor_atual()
    candidatos = []
    for (asset_id, expiration, period, direcao), inst in list(_bullex_instrument_cache.items()):
        if asset_id != active_id or period != 900 or direcao != direction:
            continue
        # Não aceita contrato já vencido e não pula mais de um ciclo M15.
        if expiration <= int(server_ts):
            continue
        distancia = abs(int(expiration) - candle_to)
        if distancia <= 900:
            candidatos.append((distancia, int(expiration), inst))

    if not candidatos:
        return None
    candidatos.sort(key=lambda x: (x[0], x[1]))
    return candidatos[0][2]


def _solicitar_instrumentos_digitais(active_id, timeout=3.0):
    """Solicita o catálogo Digital do ativo e deixa `name=instruments` alimentar o cache.

    O envelope/namespace segue a família observada na Traderoom. O parser não
    inventa index nem instrument_id: ambos precisam vir da resposta `instruments`.
    """
    # A API v3 exige o campo singular `asset_id` dentro de msg.body.
    # O diagnóstico RAW da Bullex confirmou explicitamente esse nome de campo.
    body = {"asset_id": int(active_id)}
    resposta = _enviar_e_aguardar(
        "digital-option-instruments.get-instruments",
        "3.0",
        body,
        timeout=timeout,
    )

    # Não tratar uma resposta de erro como se fosse um catálogo vazio.
    status = resposta.get("status") if isinstance(resposta, dict) else None
    if status not in (None, 0, 200, 2000):
        msg = resposta.get("msg") if isinstance(resposta, dict) else None
        reason = msg.get("reason") if isinstance(msg, dict) else None
        raise RuntimeError(
            f"get-instruments rejeitado pela Bullex status={status}: {reason or resposta}"
        )

    _armazenar_instrumentos_digitais(resposta)
    return resposta


def _buscar_instrumento(active_id, sinal, ticker, candle_to, symbol=None):
    """Obtém index + symbol SPT M15 diretamente do catálogo REAL da Bullex."""
    item = _instrumento_digital_cache(active_id, sinal, candle_to)
    if item:
        return item

    try:
        _solicitar_instrumentos_digitais(active_id, timeout=2.5)
    except Exception as e:
        log(f"[DIGITAL INSTRUMENT] {symbol or ticker}: consulta instruments falhou: {e}")

    item = _instrumento_digital_cache(active_id, sinal, candle_to)
    if item:
        exp_real = int(item.get("expiration", candle_to))
        ajuste = exp_real - int(candle_to)
        extra = f" | ajuste_exp={ajuste:+d}s" if ajuste else ""
        log(
            f"[DIGITAL INSTRUMENT] {symbol or ticker}: {sinal} M15 REAL -> "
            f"index={item['instrument_index']} id={item['instrument_id']} "
            f"expiration={exp_real}{extra}"
        )
        return item

    # Diagnóstico: mostra o que o catálogo REAL trouxe para este ativo.
    direction = _direcao_instrumento(sinal)
    disponiveis = []
    for (asset_id, expiration, period, direcao), inst in list(_bullex_instrument_cache.items()):
        if int(asset_id) == int(active_id) and direcao == direction:
            disponiveis.append(f"P{period}/EXP{expiration}/IDX{inst.get('instrument_index')}")
    resumo = ", ".join(disponiveis[:8]) if disponiveis else "nenhum SPT armazenado"
    log(
        f"[DIGITAL INSTRUMENT] {symbol or ticker}: catálogo não trouxe SPT M15 utilizável "
        f"asset_id={active_id} alvo={int(candle_to)} | disponíveis={resumo}; ordem não enviada."
    )
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
    candle_open_dt=datetime.fromtimestamp(janela['candle_open'],TZ)
    candle_close_dt=datetime.fromtimestamp(janela['candle_close'],TZ)
    msg=resposta.get('msg') if isinstance(resposta,dict) else None
    option_id=(msg.get('id') or msg.get('position_id') or msg.get('positionId')) if isinstance(msg,dict) else None
    info={
        'symbol':symbol,'ticker':ticker,'sinal':sinal,'valor':valor,'asset_id':active_id,
        'balance_id':str(balance_id),'produto':produto,'option_id':option_id,
        'instrument_id':instrument_id,'instrument_index':instrument_index,
        'expired':int(janela['candle_close']),'expiracao':candle_close_dt.isoformat(),
        'candle_open':candle_open_dt.isoformat(),'atraso_segundos':round(float(janela['atraso_segundos']),3),
        'fonte_horario':janela['source'],'enviada_em':agora_brt().isoformat(),'resultado':'PENDENTE',
        'response':resposta,
    }
    payout_detectado = _extrair_payout_percent(resposta)
    info['payout_percent'] = round(float(payout_detectado if payout_detectado is not None else BULLEX_PAYOUT_FALLBACK), 2)
    info['payout_source'] = 'BULLEX' if payout_detectado is not None else 'FALLBACK'
    with _execucao_lock:
        _operacoes_ativas_por_symbol[symbol]=info
        _operacoes_em_envio.discard(symbol)
    estado['execucao']['ultima_ordem']=info.copy()
    estado['execucao']['ultimo_erro']=None
    _atualizar_estado_execucao()
    log(f"[AUTO] ORDEM CONFIRMADA via {produto}: {symbol} {sinal} R${valor:.2f} id={option_id}")
    return 'CONFIRMADA'

def executar_ordem_intravela(symbol, sinal, resultado, valor_override=None, tipo_entrada="PRIMEIRA"):
    """Envia uma ordem. valor_override é usado pelo Gale 1 (R$ 6,00)."""
    global _bullex_last_error

    if not BULLEX_AUTO_TRADE:
        return None

    if sinal not in ("CALL", "PUT"):
        return None

    if not BULLEX_USER_BALANCE_ID:
        estado["execucao"]["ultimo_erro"] = "SEM_BALANCE_ID"
        _atualizar_estado_execucao()
        return "SEM_BALANCE_ID"

    # Resolve o active_id ANTES de reservar a vaga. A trava passa a usar tanto
    # o nome do ativo quanto o active_id real da Bullex, evitando duplicidade
    # mesmo se o mesmo ativo aparecer com nomes/chaves diferentes.
    config = next(
        (cfg for cfg in ATIVO_BULLEX.values() if cfg["symbol"] == symbol),
        None,
    )
    if not config:
        return "SEM_ATIVO"
    active_id = int(config["active_id"])

    with _execucao_lock:
        active_ids_ocupados = {
            int(info.get("asset_id"))
            for info in _operacoes_ativas_por_symbol.values()
            if info.get("asset_id") is not None
        }
        active_ids_ocupados.update(
            int(op.get("asset_id"))
            for op in _operacoes_pendentes.values()
            if op.get("asset_id") is not None
        )

        if (
            symbol in _operacoes_ativas_por_symbol
            or symbol in _operacoes_em_envio
            or symbol in _operacoes_pendentes
            or active_id in _active_ids_em_envio
            or active_id in active_ids_ocupados
        ):
            log(f"[AUTONOMO ATIVO] {symbol} active_id={active_id}: já existe operação deste ativo; segunda entrada BLOQUEADA.")
            return "BLOQUEADA_ATIVO"
        if not _ha_vaga_operacao_global():
            log(
                f"[AUTONOMO GLOBAL] {symbol}: limite de {MAX_OPERACOES_GLOBAIS} "
                f"operações simultâneas atingido; entrada ignorada."
            )
            return "BLOQUEADA_GLOBAL"
        _operacoes_em_envio.add(symbol)
        _active_ids_em_envio.add(active_id)

    balance_id = _obter_balance_id()
    if not balance_id:
        with _execucao_lock:
            _operacoes_em_envio.discard(symbol)
            _active_ids_em_envio.discard(active_id)
        return "SEM_BALANCE_ID"

    ticker = config.get("ticker")
    valor = float(valor_override) if valor_override is not None else _valor_entrada_atual()

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
        with _execucao_lock:
            _operacoes_em_envio.discard(symbol)
            _active_ids_em_envio.discard(active_id)
        return "VELA_ENCERRADA"

    if restantes < INTRAVELA_MIN_SEGUNDOS_RESTANTES:
        log(
            f"[INTRAVELA] {symbol}: restam apenas {restantes:.1f}s; "
            "ordem NÃO enviada para evitar cair na próxima vela."
        )
        with _execucao_lock:
            _operacoes_em_envio.discard(symbol)
            _active_ids_em_envio.discard(active_id)
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

    # R11: o seletor global pode ter validado o contrato Digital antes do envio.
    # Reutiliza exatamente o instrument_id/index recebido da Bullex para evitar
    # uma segunda consulta e impedir troca de contrato entre seleção e ordem.
    instrumento = resultado.get("instrumento_digital_preselecionado")
    if not instrumento:
        instrumento = _buscar_instrumento(active_id, sinal, ticker, candle_to, symbol)
    if not instrumento or not instrumento.get("instrument_id"):
        with _execucao_lock:
            _operacoes_em_envio.discard(symbol)
            _active_ids_em_envio.discard(active_id)
        log(f"[DIGITAL] {symbol}: instrument_id {sinal} M1 não encontrado; ordem não enviada.")
        return "SEM_INSTRUMENTO_DIGITAL"
    instrument_id = str(instrumento["instrument_id"])
    instrument_index = instrumento.get("instrument_index")
    # O vencimento usado para acompanhamento deve ser o do contrato REAL.
    expiration_real = int(instrumento.get("expiration") or candle_to)
    if instrument_index is None:
        with _execucao_lock:
            _operacoes_em_envio.discard(symbol)
            _active_ids_em_envio.discard(active_id)
        log(f"[DIGITAL] {symbol}: instrument_index ausente; ordem NÃO enviada.")
        return "SEM_INSTRUMENT_INDEX"

    # Se o catálogo publicou um vencimento M5 real diferente do alvo teórico,
    # acompanha o fechamento pelo vencimento efetivamente comprado.
    janela["candle_close"] = expiration_real

    # Formato confirmado na captura real da Traderoom Digital.
    amount_txt = str(int(valor)) if float(valor).is_integer() else str(float(valor))
    body = {
        "user_balance_id": int(balance_id),
        "instrument_id": instrument_id,
        "amount": amount_txt,
        "instrument_index": int(instrument_index),
        "asset_id": int(active_id),
    }

    log(
        f"[AUTO DIGITAL] Enviando {symbol} {sinal} R${valor:.2f} | "
        f"instrument_id={instrument_id} | entrada_estimada={resultado['preco']:.5f} | "
        f"expira={datetime.fromtimestamp(expiration_real, TZ).strftime('%H:%M:%S')} | restam={restantes:.1f}s"
    )

    try:
        with _bullex_diag_lock:
            _bullex_diag["orders_sent"] += 1

        resposta = _enviar_e_aguardar(
            "digital-options.place-digital-option",
            "3.0",
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
            with _execucao_lock:
                _operacoes_em_envio.discard(symbol)
            _active_ids_em_envio.discard(active_id)
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
            "DIGITAL_INTRAVELA",
            resposta,
            janela,
            instrument_id=instrument_id,
            instrument_index=instrument_index,
        )

        with _execucao_lock:
            _operacoes_em_envio.discard(symbol)
            _active_ids_em_envio.discard(active_id)

        with _execucao_lock:
            if symbol in _operacoes_ativas_por_symbol:
                _operacoes_ativas_por_symbol[symbol]["preco_entrada_estimado"] = float(resultado["preco"])
                _operacoes_ativas_por_symbol[symbol]["estrategia"] = resultado.get("estrategia", "M1_SR_LTA_LTB_RETRACAO")
                _operacoes_ativas_por_symbol[symbol]["regime"] = resultado.get("regime", "AUTONOMO")
                _operacoes_ativas_por_symbol[symbol]["tipo_entrada"] = tipo_entrada

        threading.Thread(
            target=enviar_status_ordem_telegram,
            args=(symbol, sinal, "CONFIRMADA", ""),
            daemon=True,
            name=f"telegram-ordem-confirmada-{active_id}",
        ).start()

        return status

    except Exception as e:
        with _bullex_diag_lock:
            _bullex_diag["orders_errors"] += 1
        _bullex_last_error = str(e)
        estado["execucao"]["ultimo_erro"] = str(e)
        _atualizar_estado_execucao()
        with _execucao_lock:
            _operacoes_em_envio.discard(symbol)
            _active_ids_em_envio.discard(active_id)
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
                f"[OTC] Inicialização imediata concluída com "
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
    # DIGITAL - CATÁLOGO REAL DE INSTRUMENTOS
    # ========================================================

    if nome == "instruments":
        _log_digital_raw(data)
        qtd = _armazenar_instrumentos_digitais(data)
        if request_id is not None:
            with _bullex_cv:
                _bullex_response_store[str(request_id)] = data
                _bullex_cv.notify_all()
        if qtd:
            log(f"[DIGITAL INSTRUMENTS] {qtd} SPT(s) armazenado(s) do catálogo real.")
        return

    # Eventos de posição Digital confirmam que a ordem realmente abriu.
    if nome == "position-changed" and isinstance(msg, dict):
        instrument_id_evt = msg.get("instrument_id")
        if not instrument_id_evt and isinstance(msg.get("raw_event"), dict):
            for raw in msg["raw_event"].values():
                if isinstance(raw, dict) and raw.get("instrument_id"):
                    instrument_id_evt = raw.get("instrument_id")
                    break
        if instrument_id_evt:
            with _bullex_cv:
                _bullex_digital_position_events[str(instrument_id_evt)] = data
                _bullex_cv.notify_all()

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

                # Estratégia R30: observa a vela M1 ainda aberta.
                if int(size) == 60:
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
    """Normaliza os OTC explicitamente configurados."""
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
        # R8: aceita dinamicamente TODOS os pares OTC devolvidos pela lista DIGITAL
        # da Bullex. Não depende mais de PARES_OTC_ALVO para decidir o universo.
        if len(par) != 6 or not par.isalpha():
            return None
        codigo = f"{par}_OTC"
        symbol_final = f"{par[:3]}/{par[3:]} OTC"
        mercado = "OTC"
    else:
        # Esta versão opera SOMENTE Digital OTC. Mercado aberto é ignorado.
        return None

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
    # R8: lista dinâmica; devolve todos os OTC digitais encontrados.
    return sorted(encontrados.values(), key=lambda x: x.get("codigo", ""))


def _corpo_lista_instrumentos(nome):
    if nome == "digital-option-instruments.get-underlying-list":
        return {"type": "digital-option"}
    return None


def _consultar_lista_mercado_aberto(nome, versoes=("2.0", "1.0")):
    """Consulta e agrega todos os pares OTC retornados pela lista digital.

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
        ativos_finais = sorted(encontrados.values(), key=lambda x: x.get("codigo", ""))
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
        "[ATIVOS OTC] Ativos OTC carregados: "
        + ", ".join(
            f"{cfg['ticker']}={cfg['active_id']}"
            for cfg in novos_bullex.values()
        )
    )

    # R8: universo OTC é descoberto dinamicamente; não há lista fixa de faltantes.


def _inicializar_ativos_mercado_aberto():
    """Descobre automaticamente todos os pares OTC disponíveis na lista DIGITAL.

    A inicialização é serializada para impedir duas descobertas concorrentes
    após reconexões rápidas do WebSocket.
    """
    global _bullex_assets_last_error

    if not _bullex_assets_init_lock.acquire(blocking=False):
        log("[OTC] Descoberta de ativos já está em andamento.")
        return

    try:
        _bullex_assets_ready_event.clear()
        fonte_digital = "digital-option-instruments.get-underlying-list"

        try:
            _, ativos = _consultar_lista_mercado_aberto(fonte_digital)
            if not ativos:
                raise RuntimeError(
                    "Lista digital não retornou pares OTC disponíveis."
                )

            _atualizar_ativos_mercado_aberto(ativos, fonte_digital)
            _assinar_candles_mercado_aberto()
            log(
                f"[OTC] Inicialização concluída com {len(ativos)} ativo(s)."
            )
            return

        except Exception as e:
            _bullex_assets_last_error = str(e)
            log(f"[OTC] Descoberta digital falhou: {e}")

        # Diagnóstico adicional. IDs marginais nunca são usados para ordens.
        try:
            nome_marginal = "marginal-forex-instruments.get-underlying-list"
            _, diagnostico = _consultar_lista_mercado_aberto(nome_marginal)
            if diagnostico:
                log(
                    "[OTC] A lista marginal reconheceu: "
                    + ", ".join(
                        f"{x['ticker']}={x['active_id']}" for x in diagnostico
                    )
                    + ". Mantidos apenas como diagnóstico; nenhuma ordem usa esses IDs."
                )
        except Exception as diag_e:
            log(f"[OTC] Diagnóstico marginal indisponível: {diag_e}")

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
            "[OTC] Ativos ainda não disponíveis. "
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
    """R22: assina M5 e M15 para alimentar estratégia e preload recente."""
    assinaturas = set()

    with _bullex_assets_lock:
        configs = list(ATIVO_BULLEX.values())

    for config in configs:
        active_id = int(config["active_id"])
        for size, rotulo in ((60, "M1"), (300, "M5"), (900, "M15")):
            chave = (active_id, size)
            if chave in assinaturas:
                continue
            assinaturas.add(chave)

            try:
                _assinar_candle(active_id, size)
                log(
                    f"[ATIVOS] Assinatura {rotulo} ativa: "
                    f"{config.get('symbol')} [{config.get('mercado', 'ABERTO')}] "
                    f"active_id={active_id}"
                )
            except Exception as e:
                log(
                    f"[ATIVOS] Falha assinatura {rotulo} "
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


def _contar_fechadas_cache(active_id, size):
    segundos = 300 if int(size) == 300 else 900
    return len(somente_velas_fechadas(_candidatos_candles_cache(active_id, size), segundos // 60))


def _historico_m15_pronto_ativo(active_id, minimo=55):
    """Compatibilidade R40: retorna True quando o ativo já tem M1 suficiente."""
    try:
        return len(somente_velas_fechadas(_candles_cache(int(active_id), 60), 1)) >= int(minimo)
    except Exception:
        return False


def _precarregar_historico_r22(forcar=False):
    """R30 M15: carrega M15 por ativo sem travar o robô inteiro por um ativo atrasado.

    A estratégia operacional é M15. Cada ativo fica apto individualmente quando
    possui pelo menos 55 velas M15 fechadas. O evento global significa que há
    pelo menos um ativo pronto para análise; ativos ainda incompletos continuam
    aguardando sem bloquear os demais.
    """
    global _historico_preload_ultima_tentativa

    if _historico_pronto_event.is_set() and not forcar:
        return True

    if not _historico_preload_lock.acquire(blocking=False):
        return _historico_pronto_event.is_set()

    try:
        _historico_preload_ultima_tentativa = agora_brt().isoformat()

        with _bullex_assets_lock:
            itens = [(codigo, dict(cfg)) for codigo, cfg in ATIVO_BULLEX.items()]

        if not itens:
            _historico_pronto_event.clear()
            log("[R30 PRELOAD M15] Nenhum ativo mapeado ainda.")
            return False

        log(f"[R30 PRELOAD M15] Iniciando M15 para {len(itens)} ativo(s).")
        status_local = {}
        qtd_prontos = 0

        for codigo, cfg in itens:
            symbol = cfg.get("symbol")
            active_id = int(cfg.get("active_id"))
            erro = None
            try:
                obter_candles(symbol, TIMEFRAME_TREND, max(OUTPUTSIZE_15M, 90))
            except Exception as e:
                erro = str(e)

            m15 = len(somente_velas_fechadas(_candles_cache(active_id, 60), 1))
            ok = m15 >= 55
            if ok:
                qtd_prontos += 1

            status_local[codigo] = {
                "symbol": symbol,
                "active_id": active_id,
                "m15": m15,
                "pronto": ok,
                "erro": erro,
            }
            log(
                f"[R30 PRELOAD M15] {symbol} | M15={m15} | "
                f"status={'PRONTO' if ok else 'AGUARDANDO'}"
                + (f" | erro={erro}" if erro else "")
            )

        _historico_preload_status.clear()
        _historico_preload_status.update(status_local)

        if qtd_prontos > 0:
            _historico_pronto_event.set()
            log(
                f"[R30 PRELOAD M15] LIBERADO: {qtd_prontos}/{len(itens)} ativo(s) "
                "com M15 suficiente. Ativos incompletos não bloqueiam os demais."
            )
            return True

        _historico_pronto_event.clear()
        log("[R30 PRELOAD M15] AGUARDANDO: nenhum ativo possui M15 suficiente ainda.")
        return False
    finally:
        _historico_preload_lock.release()


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
# ESTRATÉGIA R22 - TENDÊNCIA M15 + PULLBACK M5 + CONFIRMAÇÃO INTRAVELA
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
    candles = _candles_cache(active_id, 60)
    fechadas = somente_velas_fechadas(candles, 1)
    if len(fechadas) < 15:
        return None
    return atr(fechadas, 14)


def _pivos_sr(candles, janela):
    """Retorna pivôs de suporte e resistência usando apenas candles M1 fechados."""
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
    candles = _candles_cache(active_id, 60)
    fechadas = somente_velas_fechadas(candles, 1)

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


def _adx_candles(candles, period=14):
    """ADX simples (Wilder) calculado somente com candles fechados."""
    if len(candles) < period * 2 + 2:
        return None
    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, len(candles)):
        h = float(candles[i]["high"]); l = float(candles[i]["low"])
        ph = float(candles[i-1]["high"]); pl = float(candles[i-1]["low"])
        pc = float(candles[i-1]["close"])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
        up = h-ph; down = pl-l
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
    if len(trs) < period:
        return None
    atr_w=sum(trs[:period]); pdm=sum(plus_dm[:period]); mdm=sum(minus_dm[:period])
    dx=[]
    for i in range(period, len(trs)):
        atr_w = atr_w - atr_w/period + trs[i]
        pdm = pdm - pdm/period + plus_dm[i]
        mdm = mdm - mdm/period + minus_dm[i]
        if atr_w <= 0: continue
        pdi=100*pdm/atr_w; mdi=100*mdm/atr_w
        den=pdi+mdi
        if den>0: dx.append(100*abs(pdi-mdi)/den)
    if len(dx) < period:
        return sum(dx)/len(dx) if dx else None
    adx=sum(dx[:period])/period
    for v in dx[period:]: adx=((adx*(period-1))+v)/period
    return adx


def _fechadas_antes(candles, candle_from, segundos):
    """Evita usar a vela atual ou informação futura na análise."""
    out=[]
    for c in ordenar_candles(candles):
        dt=c.get("_dt")
        if dt is not None and dt.timestamp()+segundos <= candle_from+0.001:
            out.append(c)
    return out


def _autonomo_features(candles, idx):
    """Cria uma fotografia numérica usando somente informação conhecida até idx."""
    if idx < 22 or idx >= len(candles):
        return None
    janela = candles[:idx + 1]
    c = closes(janela)
    a = atr(janela, 14)
    if not a or a <= 0:
        return None
    preco = c[-1]
    e5, e13, e21 = ema(c, 5), ema(c, 13), ema(c, 21)
    rv = rsi(c, 14)
    if None in (e5, e13, e21, rv):
        return None
    info = candle_info(janela[-1])
    def ret(n):
        base = c[-1-n]
        return (preco-base)/a if base else 0.0
    return [
        ret(1), ret(2), ret(3), ret(6), ret(12),
        (preco-e5)/a, (e5-e13)/a, (e13-e21)/a,
        (rv-50.0)/50.0,
        (info['close']-info['open'])/a,
        info['range']/a,
        info['upper_wick']/a, info['lower_wick']/a,
    ]


def _autonomo_distancia(a, b):
    return sum((x-y)*(x-y) for x,y in zip(a,b)) ** 0.5


def _autonomo_ajuste_online(symbol, sinal):
    """Pequeno ajuste baseado apenas em operações reais já encerradas deste processo."""
    itens=[x for x in _historico_resultados if x.get('symbol')==symbol and x.get('sinal')==sinal and x.get('resultado') in ('WIN','LOSS')][-30:]
    if len(itens) < 5:
        return 0.0, len(itens)
    wins=sum(1 for x in itens if x.get('resultado')=='WIN')
    taxa=wins/len(itens)
    return max(-0.08,min(0.08,(taxa-0.5)*0.20)), len(itens)


def _autonomo_faixa_confianca(confianca):
    pct=float(confianca or 0.0)*100.0
    if pct < 65: return "62-64.9"
    if pct < 70: return "65-69.9"
    if pct < 75: return "70-74.9"
    return "75+"


def _autonomo_desempenho(itens):
    decididos=[x for x in itens if x.get("resultado") in ("WIN","LOSS")]
    wins=sum(1 for x in decididos if x.get("resultado")=="WIN")
    total=len(decididos)
    return wins,total,(wins/total if total else None)


def _autonomo_filtro_adaptativo(symbol, sinal, confianca):
    """Retorna (permitir, motivo, diagnostico). Nunca aprende com ordem recusada."""
    faixa=_autonomo_faixa_confianca(confianca)
    base=[x for x in _historico_resultados
          if x.get("resultado") in ("WIN","LOSS")
          and x.get("estrategia")=="AUTONOMO_KNN_M5"][-AUTONOMO_ADAPTATIVO_ULTIMAS:]
    por_faixa=[x for x in base if x.get("faixa_confianca")==faixa and x.get("sinal")==sinal]
    por_ativo=[x for x in por_faixa if x.get("symbol")==symbol]
    wa,na,ta=_autonomo_desempenho(por_ativo)
    wf,nf,tf=_autonomo_desempenho(por_faixa)
    diag={"faixa":faixa,"ativo_n":na,"ativo_taxa":ta,"faixa_n":nf,"faixa_taxa":tf}
    if na >= AUTONOMO_ADAPTATIVO_ATIVO_MIN and ta is not None and ta < AUTONOMO_ADAPTATIVO_TAXA_BLOQUEIO:
        return False, f"NAO_OPERAR ativo+faixa {symbol} {sinal} {faixa}: {wa}/{na} WIN ({ta*100:.1f}%)", diag
    if nf >= AUTONOMO_ADAPTATIVO_FAIXA_MIN and tf is not None and tf < AUTONOMO_ADAPTATIVO_TAXA_BLOQUEIO:
        return False, f"NAO_OPERAR faixa {sinal} {faixa}: {wf}/{nf} WIN ({tf*100:.1f}%)", diag
    return True, "LIBERADO_ADAPTATIVO", diag


def _agregar_m15_desde_m5(active_id, candle_from, limite=100):
    """Monta candles M1 fechados a partir do M5 quando o feed M15 nativo ainda não chegou.

    Usa somente grupos completos de 3 candles M5 já fechados antes de candle_from,
    portanto não olha a vela atual nem informação futura.
    """
    m5 = _fechadas_antes(_candles_cache(active_id, 300), candle_from, 300)
    grupos = {}
    for c in m5:
        dt = c.get("_dt")
        if dt is None:
            continue
        ts = int(dt.timestamp())
        inicio15 = ts - (ts % 900)
        grupos.setdefault(inicio15, []).append(c)

    saida = []
    for inicio15 in sorted(grupos):
        # O M15 inteiro precisa estar encerrado antes da abertura da vela analisada.
        if inicio15 + 900 > candle_from + 0.001:
            continue
        itens = ordenar_candles(grupos[inicio15])
        # Exige exatamente a estrutura temporal M5 00/05/10 dentro do bloco M15.
        por_ts = {int(x["_dt"].timestamp()): x for x in itens if x.get("_dt") is not None}
        esperados = [inicio15, inicio15 + 300, inicio15 + 600]
        if not all(t in por_ts for t in esperados):
            continue
        trio = [por_ts[t] for t in esperados]
        dt15 = datetime.fromtimestamp(inicio15, tz=TZ)
        saida.append({
            "id": f"M15_FROM_M5_{active_id}_{inicio15}",
            "datetime": dt15.isoformat(),
            "_dt": dt15,
            "open": float(trio[0]["open"]),
            "high": max(float(x["high"]) for x in trio),
            "low": min(float(x["low"]) for x in trio),
            "close": float(trio[-1]["close"]),
            "volume": sum(float(x.get("volume", 0) or 0) for x in trio),
            "phase": "C",
        })
    return saida[-int(limite):]


def _autonomo_contexto_m15_forca(active_id, candle_from, sinal):
    """Confirma contexto sem olhar o futuro: M15 fechado + força M5/M15.

    Prefere M15 nativo da Bullex. Se ainda não houver histórico M15 suficiente
    para um ativo dinâmico, reconstrói M15 com os candles M5 já fechados.
    """
    m15 = _fechadas_antes(_candles_cache(active_id, 60), candle_from, 900)[-100:]
    m5 = _fechadas_antes(_candles_cache(active_id, 300), candle_from, 300)[-100:]
    fonte_m15 = "NATIVO"
    if len(m15) < 30:
        m15_agregado = _agregar_m15_desde_m5(active_id, candle_from, 100)
        if len(m15_agregado) >= 30:
            m15 = m15_agregado
            fonte_m15 = "AGREGADO_M5"
    if len(m15) < 30 or len(m5) < 30:
        return False, f"SEM_CONTEXTO_M15 m15={len(m15)} m5={len(m5)}", {"fonte_m15": fonte_m15, "m15_n": len(m15), "m5_n": len(m5)}
    c15 = closes(m15)
    e5 = ema(c15, 5); e13 = ema(c15, 13); e21 = ema(c15, 21)
    if None in (e5, e13, e21):
        return False, "SEM_EMA_M15", {}
    t15 = "ALTA" if e5 > e13 > e21 else "BAIXA" if e5 < e13 < e21 else "NEUTRA"
    adx15 = _adx_candles(m15, 14)
    adx5 = _adx_candles(m5, 14)
    if adx5 is not None and adx5 < AUTONOMO_M5_ADX_MIN:
        return False, f"M5_SEM_FORCA adx={adx5:.1f}", {"t15":t15,"adx5":adx5,"adx15":adx15,"fonte_m15":fonte_m15}
    contra = (sinal == "CALL" and t15 == "BAIXA") or (sinal == "PUT" and t15 == "ALTA")
    if contra and adx15 is not None and adx15 >= AUTONOMO_M15_ADX_FORTE:
        return False, f"M15_FORTE_CONTRA {t15} adx={adx15:.1f}", {"t15":t15,"adx5":adx5,"adx15":adx15,"fonte_m15":fonte_m15}
    penalidade = AUTONOMO_M15_PENALIDADE_CONTRA_FRACA if contra else 0.0
    return True, "CONTEXTO_OK", {"t15":t15,"adx5":adx5,"adx15":adx15,"penalidade":penalidade,"fonte_m15":fonte_m15}


def _autonomo_ciclos_reais(symbol, sinal):
    """Mede ciclos reais: WIN primeira ou resultado final do Gale; LOSS inicial não conta duas vezes."""
    itens=[x for x in _historico_resultados if x.get("symbol")==symbol and x.get("sinal")==sinal and x.get("resultado") in ("WIN","LOSS")]
    wins=losses=0
    for x in itens[-160:]:
        tipo=x.get("tipo_entrada","PRIMEIRA")
        r=x.get("resultado")
        if tipo == "GALE":
            wins += int(r == "WIN"); losses += int(r == "LOSS")
        elif r == "WIN":
            wins += 1
        # LOSS da primeira é ignorada aqui: o ciclo termina no Gale.
    n=wins+losses
    taxa=wins/n if n else None
    return wins,losses,n,taxa


def _linha_tendencia_m15(fechadas, lado, atr15):
    """Projeta LTA pelos 2 últimos pivôs de mínima ou LTB pelos 2 últimos pivôs de máxima."""
    j = SR_M15_PIVOT_JANELA
    pts=[]
    for i in range(j, len(fechadas)-j):
        if lado == "LTA":
            v=float(fechadas[i]["low"])
            if all(v <= float(fechadas[k]["low"]) for k in range(i-j,i+j+1) if k != i): pts.append((i,v))
        else:
            v=float(fechadas[i]["high"])
            if all(v >= float(fechadas[k]["high"]) for k in range(i-j,i+j+1) if k != i): pts.append((i,v))
    if len(pts)<2: return None
    p1,p2=pts[-2],pts[-1]
    if p2[0] == p1[0]: return None
    slope=(p2[1]-p1[1])/(p2[0]-p1[0])
    # LTA precisa subir; LTB precisa cair.
    if lado == "LTA" and slope <= 0: return None
    if lado == "LTB" and slope >= 0: return None
    nivel=p2[1] + slope*(len(fechadas)-p2[0])
    return {"nivel":float(nivel),"slope":float(slope),"toques":2,"timeframe":"M15","tipo":lado}


def _resultado_retracao_intravela(msg, active_id):
    """R30: M15, suporte/resistência + LTA/LTB, entrada na retração e expiração na mesma vela."""
    if not isinstance(msg,dict) or int(msg.get("size",60) or 60) != 60: return None
    try:
        abertura=float(msg['open']); preco=float(msg['close'])
        maxima=float(msg.get('max',msg.get('high'))); minima=float(msg.get('min',msg.get('low')))
        candle_from=int(float(msg['from'])); candle_to=int(float(msg.get('to') or candle_from+60))
    except Exception: return None
    server_ts,_=_horario_servidor_atual()
    decorridos=max(0.0,server_ts-candle_from); restantes=max(0.0,candle_to-server_ts)
    if decorridos < INTRAVELA_MIN_SEGUNDOS_DECORRIDOS or decorridos > INTRAVELA_MAX_SEGUNDOS_DECORRIDOS: return None
    if restantes < INTRAVELA_MIN_SEGUNDOS_RESTANTES: return None

    m15=_fechadas_antes(_candles_cache(active_id,60),candle_from,60)[-SR_M15_LOOKBACK:]
    if len(m15)<45: return None
    a=atr(m15,14)
    if not a or a<=0: return None
    c=closes(m15); e5,e13,e21=ema(c,5),ema(c,13),ema(c,21); rv=rsi(c,14); adx15=_adx_candles(m15,14)
    if None in (e5,e13,e21,rv): return None
    tendencia='ALTA' if e5>e13>e21 else 'BAIXA' if e5<e13<e21 else 'NEUTRA'

    sup,res,_=_niveis_sr_m15(active_id)
    lta=_linha_tendencia_m15(m15,'LTA',a); ltb=_linha_tendencia_m15(m15,'LTB',a)
    candidatos=[]
    for g in sup:
        candidatos.append(('CALL','SUPORTE',float(g['nivel']),int(g.get('toques',0))))
    for g in res:
        candidatos.append(('PUT','RESISTENCIA',float(g['nivel']),int(g.get('toques',0))))
    if lta: candidatos.append(('CALL','LTA',lta['nivel'],2))
    if ltb: candidatos.append(('PUT','LTB',ltb['nivel'],2))
    if not candidatos: return None

    amplitude=max(maxima-minima,1e-12)
    movimento_alta=maxima-abertura; movimento_baixa=abertura-minima
    aprovados=[]
    for sinal,tipo,nivel,toques in candidatos:
        # Opera retração a favor da estrutura/tendência; neutro é aceito só em S/R forte.
        if sinal=='CALL' and tendencia=='BAIXA': continue
        if sinal=='PUT' and tendencia=='ALTA': continue
        dist_abertura=abs(abertura-nivel)/a
        if dist_abertura < M15_DISTANCIA_ABERTURA_ATR_MIN: continue
        tol=a*(M15_LINHA_TOLERANCIA_ATR if tipo in ('LTA','LTB') else M15_TOQUE_TOLERANCIA_ATR)
        tocou = minima <= nivel+tol if sinal=='CALL' else maxima >= nivel-tol
        if not tocou: continue
        if sinal=='CALL':
            impulso=max(movimento_baixa,1e-12); rejeicao=preco-minima; retracao=rejeicao/impulso
            if preco <= nivel-a*0.03: continue
        else:
            impulso=max(movimento_alta,1e-12); rejeicao=maxima-preco; retracao=rejeicao/impulso
            if preco >= nivel+a*0.03: continue
        if rejeicao < a*M15_REJEICAO_ATR_MIN: continue
        if not (M15_RETRACAO_MIN <= retracao <= M15_RETRACAO_MAX): continue
        confluencia=0
        for s2,t2,n2,_ in candidatos:
            if s2==sinal and t2!=tipo and abs(n2-nivel)<=a*0.25: confluencia+=1
        score=(toques if tipo in ('SUPORTE','RESISTENCIA') else 2) + confluencia*2 + (1 if tendencia!='NEUTRA' else 0) + (1 if adx15 and adx15>=18 else 0)
        aprovados.append((score,sinal,tipo,nivel,toques,retracao,rejeicao,confluencia))
    if not aprovados: return None
    aprovados.sort(reverse=True,key=lambda x:x[0]); score,sinal,tipo,nivel,toques,retracao,rejeicao,confluencia=aprovados[0]
    # confiança é um indicador interno de qualidade do setup, não probabilidade garantida.
    confianca=min(0.90,0.58+0.035*score)
    symbol=_symbol_por_active_id(active_id)[1]
    log(f"[M1 RETRACAO] {symbol} {sinal} | {tipo}={nivel:.5f} toques={toques} | tendencia={tendencia} ADX={adx15 if adx15 is not None else 0:.1f} | retracao={retracao*100:.1f}% | confluencia={confluencia}")
    return {
      'sinal':sinal,'score':round(confianca*100,1),'score_call':round(confianca*100,1) if sinal=='CALL' else 0,'score_put':round(confianca*100,1) if sinal=='PUT' else 0,
      'preco':preco,'vela':datetime.fromtimestamp(candle_from,TZ),'estrategia':'M1_SR_LTA_LTB_RETRACAO','regime':tendencia,
      'pullback':f'RETRACAO {retracao*100:.1f}% EM {tipo}','rejeicao':f'REJEICAO {rejeicao/a:.2f} ATR','atr':a,'rsi':rv,
      'ema5':e5,'ema13':e13,'ema21':e21,'tendencia_5m':'N/A','tendencia_15m':tendencia,'bloqueio':'SINAL_M1_RETRACAO',
      'mensagem':f'{sinal} M1 | {tipo} + retração | confluência={confluencia} | qualidade={confianca*100:.1f}%',
      'candle_from':candle_from,'candle_to':candle_to,'segundos_decorridos':decorridos,'segundos_restantes':restantes,
      'impulso':impulso,'retracao_ratio':retracao,'nivel_sr':nivel,'tipo_nivel':tipo,'toques_nivel':toques,'distancia_abertura_nivel':dist_abertura,
      'adx15':adx15,'confianca':confianca,'confianca_ciclo':confianca,'amostras_ciclo':0,'amostras_modelo':len(m15),'margem':0.0,
      'ajuste_online':0.0,'faixa_confianca':'M1','adaptativo':{},'filtro_adaptativo':'N/A'
    }

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
        "rsi": f"{resultado.get('rsi'):.2f}" if isinstance(resultado.get("rsi"), (int, float)) else "-",
        "ema5": f"{resultado.get('ema5'):.5f}" if isinstance(resultado.get("ema5"), (int, float)) else "-",
        "ema13": f"{resultado.get('ema13'):.5f}" if isinstance(resultado.get("ema13"), (int, float)) else "-",
        "ema21": f"{resultado.get('ema21'):.5f}" if isinstance(resultado.get("ema21"), (int, float)) else "-",
        "tendencia_5m": resultado.get("tendencia_5m", "-"),
        "tendencia_15m": resultado.get("tendencia_15m", "-"),
        "pullback": resultado.get("pullback", "-"),
        "confirmacao": resultado.get("rejeicao", "-"),
        "lateral": "N/A",
        "atr": (
            f"{resultado['atr']:.6f}"
            if isinstance(resultado.get("atr"), (int, float)) else "-"
        ),
        "bloqueio": resultado.get("bloqueio", "-"),
        "regime": resultado.get("regime", "AUTONOMO"),
        "estrategia": resultado.get("estrategia", "AUTONOMO_KNN_M5"),
        "zona_fibonacci": "-",
    }


_r22_diag_lock = threading.RLock()
_r22_diag_emitidos = set()


def _diagnostico_r22(active_id, msg):
    """Gera um diagnóstico por ativo/vela sem alterar a decisão da estratégia."""
    if not isinstance(msg, dict):
        return None
    try:
        abertura=float(msg["open"]); fechamento=float(msg["close"])
        maxima=float(msg.get("max",msg.get("high"))); minima=float(msg.get("min",msg.get("low")))
        candle_from=int(float(msg["from"])); candle_to=int(float(msg.get("to") or candle_from+300))
    except Exception:
        return None

    server_ts,_=_horario_servidor_atual()
    decorridos=max(0.0,server_ts-candle_from); restantes=max(0.0,candle_to-server_ts)
    if decorridos < INTRAVELA_MIN_SEGUNDOS_DECORRIDOS or restantes < INTRAVELA_MIN_SEGUNDOS_RESTANTES:
        return None

    m5=_fechadas_antes(_candles_cache(active_id,300),candle_from,300)[-90:]
    m15=_fechadas_antes(_candles_cache(active_id,900),candle_from,900)[-90:]
    if len(m5)<55 or len(m15)<55:
        return {"candle_from":candle_from,"motivo":f"HISTORICO_INSUFICIENTE M5={len(m5)} M15={len(m15)}"}

    c5=closes(m5); c15=closes(m15)
    atr5=atr(m5,14); adx5=_adx_candles(m5,14); adx15=_adx_candles(m15,14)
    e9=ema(c5,9); e20=ema(c5,20); e50=ema(c5,50)
    e20_15=ema(c15,20); e50_15=ema(c15,50)
    e20_15_prev=ema(c15[:-3],20) if len(c15[:-3])>=20 else None
    rsi5=rsi(c5,14)
    vals=(atr5,adx5,adx15,e9,e20,e50,e20_15,e50_15,e20_15_prev,rsi5)
    if any(v is None for v in vals) or not atr5 or atr5<=0:
        return {"candle_from":candle_from,"motivo":"INDICADORES_INDISPONIVEIS"}

    prev=m5[-1]
    po=float(prev["open"]); pc=float(prev["close"]); ph=float(prev["high"]); pl=float(prev["low"])
    prange=max(ph-pl,1e-12); pbody=abs(pc-po)
    current_range=max(maxima-minima,1e-12); current_body=abs(fechamento-abertura)
    tol=atr5*0.18
    touch20 = pl <= e20+tol and ph >= e20-tol
    trend_call=(e20_15>e50_15 and e20_15>e20_15_prev and c15[-1]>e20_15 and e9>e20>e50)
    trend_put=(e20_15<e50_15 and e20_15<e20_15_prev and c15[-1]<e20_15 and e9<e20<e50)
    prev_bear = pc < po or pbody/prange <= 0.35
    prev_bull = pc > po or pbody/prange <= 0.35
    call_confirm=(fechamento>abertura and fechamento>ph and fechamento>e9 and current_body>=atr5*0.18)
    put_confirm=(fechamento<abertura and fechamento<pl and fechamento<e9 and current_body>=atr5*0.18)

    if adx15 < 20: motivo="ADX15_FRACO"
    elif adx5 < 17: motivo="ADX5_FRACO"
    elif current_range > atr5*1.55 or current_body > atr5*1.20: motivo="VELA_ATUAL_ESTICADA"
    elif not (trend_call or trend_put): motivo="SEM_TENDENCIA_ALINHADA"
    elif not touch20: motivo="SEM_PULLBACK_EMA20"
    elif trend_call and not prev_bear: motivo="PULLBACK_CALL_SEM_RETRACAO"
    elif trend_put and not prev_bull: motivo="PULLBACK_PUT_SEM_RETRACAO"
    elif trend_call and pl < e50-tol: motivo="CALL_ATRAVESSOU_EMA50"
    elif trend_put and ph > e50+tol: motivo="PUT_ATRAVESSOU_EMA50"
    elif trend_call and not (50 <= rsi5 <= 68): motivo="RSI_CALL_FORA"
    elif trend_put and not (32 <= rsi5 <= 50): motivo="RSI_PUT_FORA"
    elif trend_call and not call_confirm: motivo="CALL_AGUARDA_ROMPIMENTO"
    elif trend_put and not put_confirm: motivo="PUT_AGUARDA_ROMPIMENTO"
    else: motivo="SETUP_APROVADO"

    tendencia15="ALTA" if e20_15>e50_15 else "BAIXA" if e20_15<e50_15 else "NEUTRA"
    tendencia5="ALTA" if e9>e20>e50 else "BAIXA" if e9<e20<e50 else "MISTA"
    return {"candle_from":candle_from,"motivo":motivo,"adx5":adx5,"adx15":adx15,
            "rsi":rsi5,"t5":tendencia5,"t15":tendencia15,"touch20":touch20,
            "call_confirm":call_confirm,"put_confirm":put_confirm,"restantes":restantes}


def _log_diagnostico_r22(active_id, symbol, msg):
    diag=_diagnostico_r22(active_id,msg)
    if not diag:
        return
    key=(int(active_id),int(diag["candle_from"]))
    with _r22_diag_lock:
        if key in _r22_diag_emitidos:
            return
        _r22_diag_emitidos.add(key)
        # evita crescimento ilimitado em processos longos
        if len(_r22_diag_emitidos)>1000:
            limite=int(diag["candle_from"])-86400
            _r22_diag_emitidos.intersection_update({k for k in _r22_diag_emitidos if k[1]>=limite})
    if "adx5" not in diag:
        log(f"[R24 DIAG] {symbol} | bloqueio={diag['motivo']}")
        return
    log(
        f"[R24 DIAG] {symbol} | M15={diag['t15']} ADX15={diag['adx15']:.1f} | "
        f"M5={diag['t5']} ADX5={diag['adx5']:.1f} RSI={diag['rsi']:.1f} | "
        f"pullbackEMA20={'SIM' if diag['touch20'] else 'NÃO'} | "
        f"confCALL={'SIM' if diag['call_confirm'] else 'NÃO'} "
        f"confPUT={'SIM' if diag['put_confirm'] else 'NÃO'} | "
        f"bloqueio={diag['motivo']} | restam={diag['restantes']:.1f}s"
    )



def _r24_chave_classificacao(resultado):
    """Ranking do modo global: prioriza qualidade do ciclo e confiança KNN."""
    ciclo = float(resultado.get("confianca_ciclo") or 0.0)
    confianca = float(resultado.get("confianca") or 0.0)
    margem = float(resultado.get("margem") or 0.0)
    amostras = float(resultado.get("amostras_modelo") or 0.0)
    return (ciclo, confianca, margem, amostras)


def _ticker_por_symbol(symbol):
    """Retorna o ticker atual do ativo a partir do mapa dinâmico da Bullex."""
    with _bullex_assets_lock:
        for cfg in ATIVO_BULLEX.values():
            if cfg.get("symbol") == symbol:
                ticker = cfg.get("ticker")
                if ticker:
                    return str(ticker)
    return None


def _r24_despachar_melhor(candle_from):
    """Compara os sinais OTC da vela M1 e executa até as vagas globais disponíveis."""
    candle_from = int(candle_from)

    # R35: M15 é intravela. O candidato pode surgir em qualquer momento dos
    # 15 minutos, então a janela de comparação precisa começar AGORA, e não
    # ficar ancorada nos primeiros segundos da abertura da vela.
    espera = min(1.5, float(R24_JANELA_CLASSIFICACAO_SEGUNDOS))
    if espera > 0:
        time.sleep(espera)

    with _r24_candidatos_lock:
        candidatos = list(_r24_candidatos.pop(candle_from, []))

    try:
        if not candidatos:
            return

        if _gales_pendentes:
            log(f"[SELETOR GLOBAL] vela={candle_from}: novas entradas ignoradas; existe Gale pendente.")
            return

        with _execucao_lock:
            vagas = max(0, int(MAX_OPERACOES_GLOBAIS) - _qtd_operacoes_globais_em_andamento())
            if vagas <= 0:
                log(
                    f"[SELETOR GLOBAL] vela={candle_from}: candidatos ignorados; "
                    f"limite de {MAX_OPERACOES_GLOBAIS} operações simultâneas atingido."
                )
                return

        candidatos.sort(key=lambda x: _r24_chave_classificacao(x[2]), reverse=True)
        ranking = ", ".join(
            f"{sym}:{res.get('sinal')} conf={res.get('confianca',0)*100:.1f}% ciclo={res.get('confianca_ciclo',0)*100:.1f}%"
            for _, sym, res in candidatos
        )
        log(f"[SELETOR GLOBAL] candidatos={ranking}")

        escolhidos = []
        for active_id, symbol, resultado in candidatos:
            with _execucao_lock:
                if not _ha_vaga_operacao_global() or len(escolhidos) >= vagas:
                    break

            ticker = _ticker_por_symbol(symbol)
            if not ticker:
                log(f"[SELETOR GLOBAL] {symbol}: ticker não encontrado; pulando candidato.")
                continue
            candle_to = int(resultado.get("candle_from", candle_from)) + 900
            instrumento = _buscar_instrumento(
                int(active_id), resultado.get("sinal"), ticker, candle_to, symbol
            )
            if not instrumento:
                log(f"[SELETOR GLOBAL] {symbol}: sem DIGITAL M1 SPT; tentando próximo candidato.")
                continue
            resultado["instrumento_digital_preselecionado"] = dict(instrumento)
            escolhidos.append((active_id, symbol, resultado))

        if not escolhidos:
            log(f"[SELETOR GLOBAL] vela={candle_from}: nenhum candidato possui DIGITAL M1 SPT disponível; sem entrada.")
            return

        for posicao, (active_id, symbol, resultado) in enumerate(escolhidos, start=1):
            log(
                f"[SELETOR GLOBAL] ESCOLHIDO {posicao}/{len(escolhidos)}={symbol} {resultado.get('sinal')} | "
                f"index={resultado['instrumento_digital_preselecionado'].get('instrument_index')}"
            )
            registrar_operacao_intravela(symbol, resultado)

    finally:
        with _r24_candidatos_lock:
            _r24_dispatchers.discard(candle_from)
            # R35: não marca a vela M1 inteira como finalizada. Novos ativos
            # podem gerar setups válidos mais tarde dentro da mesma vela.

def _status_bloqueio_ativo(symbol):
    info = _bloqueios_por_symbol.get(symbol)
    if not info:
        return None
    if time.time() >= float(info.get("ate_ts", 0) or 0):
        _bloqueios_por_symbol.pop(symbol, None)
        _sequencia_ciclos_loss[symbol] = 0
        log(f"[BLOQUEIO ATIVO] {symbol}: 1 hora concluída; ativo LIBERADO.")
        return None
    return info


def _registrar_fim_de_ciclo(symbol, operacao, resultado):
    tipo = operacao.get("tipo_entrada", "PRIMEIRA")
    if resultado == "WIN":
        _sequencia_ciclos_loss[symbol] = 0
        return
    # LOSS da primeira ainda não encerra o ciclo porque pode haver Gale.
    if resultado != "LOSS" or tipo != "GALE":
        return
    seq = int(_sequencia_ciclos_loss.get(symbol, 0) or 0) + 1
    _sequencia_ciclos_loss[symbol] = seq
    log(f"[PROTEÇÃO ATIVO] {symbol}: {seq} ciclo(s) LOSS consecutivo(s).")
    if seq >= BLOQUEIO_ATIVO_CICLOS_LOSS:
        _bloqueios_por_symbol[symbol] = {
            "ate_ts": time.time() + BLOQUEIO_ATIVO_SEGUNDOS,
            "motivo": f"{seq} ciclos LOSS consecutivos",
            "sequencia": seq,
        }
        _gales_pendentes.pop(symbol, None)
        log(f"[BLOQUEIO ATIVO] {symbol}: BLOQUEADO por 1 hora após {seq} ciclos LOSS consecutivos.")


def _tentar_gale_na_proxima_vela(active_id, msg):
    """Executa no máximo 1 Gale de R$6 na vela M5 imediatamente seguinte ao LOSS."""
    codigo, symbol = _symbol_por_active_id(active_id)
    if not symbol:
        return False
    gale = _gales_pendentes.get(symbol)
    if not gale:
        return False
    try:
        candle_from = int(msg.get("from"))
        candle_to = int(msg.get("to") or (candle_from + 300))
        alvo_from = int(gale["candle_from_alvo"])
    except Exception:
        return True
    if candle_from < alvo_from:
        return True
    if candle_from > alvo_from:
        log(f"[GALE] {symbol}: perdeu a vela imediatamente seguinte; Gale cancelado.")
        _gales_pendentes.pop(symbol, None)
        return False
    server_ts, _ = _horario_servidor_atual()
    decorridos = server_ts - candle_from
    if decorridos < INTRAVELA_MIN_SEGUNDOS_DECORRIDOS:
        return True
    if decorridos > INTRAVELA_MAX_SEGUNDOS_DECORRIDOS:
        if not gale.get("tentado"):
            log(f"[GALE] {symbol}: janela 2-8s perdida; Gale cancelado.")
            _gales_pendentes.pop(symbol, None)
        return True
    if gale.get("tentado"):
        return True

    # Gale 1 obrigatório: toda PRIMEIRA entrada que fechar em LOSS agenda
    # exatamente um Gale de R$6 na vela M5 imediatamente seguinte, mantendo
    # a MESMA direção. O KNN/seletor de ciclos NÃO pode cancelar este Gale.
    gale["tentado"] = True
    preco = float(msg.get("close", msg.get("open", gale.get("entrada_anterior", 0.0))))
    resultado = {
        "sinal": gale["sinal"],
        "preco": preco,
        "candle_from": candle_from,
        "candle_to": candle_to,
        "regime": "GALE_OBRIGATORIO_APOS_LOSS",
        "faixa_confianca": "GALE",
        "vela": datetime.fromtimestamp(candle_from, TZ),
    }
    log(f"[GALE OBRIGATORIO] {symbol}: executando Gale 1 R${_valor_gale_atual():.2f} na mesma direção {gale['sinal']}.")
    valor_gale = _valor_gale_atual()
    status = executar_ordem_intravela(symbol, gale["sinal"], resultado, valor_override=valor_gale, tipo_entrada="GALE")
    if status == "CONFIRMADA":
        with _execucao_lock:
            info = _operacoes_ativas_por_symbol.get(symbol, {}).copy()
        chave = f"{symbol}|GALE|{candle_from}"
        _operacoes_pendentes[symbol] = {
            "id": chave, "symbol": symbol, "mercado": _mercado_do_symbol(symbol),
            "sinal": gale["sinal"], "score": 0, "confianca": 0.0,
            "faixa_confianca": "GALE", "ajuste_online": 0.0, "adaptativo": {},
            "estrategia": resultado.get("estrategia", "M1_SR_LTA_LTB_RETRACAO"), "regime": "GALE_OBRIGATORIO_APOS_LOSS",
            "preco_sinal": preco, "vela_sinal": datetime.fromtimestamp(candle_from, TZ),
            "vela_entrada": datetime.fromtimestamp(candle_from, TZ),
            "vela_expiracao": datetime.fromtimestamp(candle_from, TZ),
            "entrada": preco, "saida": None, "resultado": "PENDENTE",
            "ordem_automatica": True, "valor": valor_gale, "balance_id": info.get("balance_id"),
            "payout_percent": info.get("payout_percent", BULLEX_PAYOUT_FALLBACK), "payout_source": info.get("payout_source", "FALLBACK"),
            "produto": info.get("produto", "DIGITAL_INTRAVELA"), "option_id": info.get("option_id"),
            "tipo_entrada": "GALE", "candle_to": candle_to,
        }
        _ultimas_operacoes_registradas[symbol] = chave
        _gales_pendentes.pop(symbol, None)
        log(f"[GALE] {symbol}: Gale 1 registrado {gale['sinal']} R${valor_gale:.2f} | expira={datetime.fromtimestamp(candle_to,TZ).strftime('%H:%M:%S')}")
    else:
        log(f"[GALE] {symbol}: Gale não foi aberto ({status}); ciclo encerrado.")
        _gales_pendentes.pop(symbol, None)
    return True


def _processar_sinal_intravela(active_id, msg):
    codigo, symbol = _symbol_por_active_id(active_id)
    if not codigo or not symbol:
        return
    if _status_bloqueio_ativo(symbol):
        return

    # R30: Martingale desativado. Cada entrada M15 encerra em WIN/LOSS/DOJI.
    _gales_pendentes.clear()

    # R31: permite até 2 operações simultâneas no robô inteiro,
    # mantendo no máximo 1 operação por ativo.
    with _execucao_lock:
        if not _ha_vaga_operacao_global():
            return

    if not dentro_do_horario() or not _historico_pronto_event.is_set():
        return

    # R30 M15: histórico é validado por ativo. Um ativo atrasado não bloqueia os demais.
    if not _historico_m15_pronto_ativo(active_id, 55):
        return

    resultado = _resultado_retracao_intravela(msg, active_id)
    if resultado is None:
        return
    candle_key=(int(active_id),int(resultado['candle_from']))
    with _intravela_lock:
        if candle_key in _intravela_velas_tentadas:
            log(f"[M1 FLUXO] {symbol}: setup repetido na mesma vela M1; já encaminhado anteriormente.")
            return
        _intravela_velas_tentadas.add(candle_key)

    log(f"[M1 CANDIDATO] {symbol} -> {resultado['sinal']} | qualidade={resultado.get('confianca',0)*100:.1f}% | setup={resultado.get('tipo_nivel')} | histórico={resultado.get('amostras_modelo',0)}")

    candle_from = int(resultado['candle_from'])
    with _r24_candidatos_lock:
        # R35: não finaliza a vela inteira após a primeira rodada do seletor.
        # Como a estratégia é intravela, outro ativo pode formar um setup válido
        # minutos depois. A trava por active_id/símbolo continua impedindo duplicidade.
        _r24_candidatos.setdefault(candle_from, []).append((int(active_id), symbol, resultado))
        if candle_from not in _r24_dispatchers:
            _r24_dispatchers.add(candle_from)
            threading.Thread(
                target=_r24_despachar_melhor,
                args=(candle_from,),
                daemon=True,
                name=f"seletor-global-{candle_from}",
            ).start()


def calcular_estatisticas_por_estrategia():
    wins = losses = dojis = 0
    for item in _historico_resultados:
        if item.get("estrategia") != "AUTONOMO_KNN_M5":
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
        "AUTONOMO_KNN_M5": {
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


def calcular_estatisticas_por_par():
    """Resumo por ativo, separando primeira entrada e Gale 1."""
    resumo = {}
    for symbol in ATIVOS.values():
        resumo[symbol] = {
            "symbol": symbol, "total": 0, "wins": 0, "losses": 0, "dojis": 0,
            "primeira_wins": 0, "primeira_losses": 0,
            "gale_wins": 0, "gale_losses": 0, "taxa": 0.0, "lucro": 0.0,
        }
    for op in _historico_resultados:
        symbol = op.get("symbol")
        if symbol not in resumo:
            resumo[symbol] = {
                "symbol": symbol, "total": 0, "wins": 0, "losses": 0, "dojis": 0,
                "primeira_wins": 0, "primeira_losses": 0,
                "gale_wins": 0, "gale_losses": 0, "taxa": 0.0, "lucro": 0.0,
            }
        r = op.get("resultado")
        tipo = op.get("tipo_entrada", "PRIMEIRA")
        x = resumo[symbol]
        x["total"] += 1
        if r == "WIN":
            x["wins"] += 1
            x["gale_wins" if tipo == "GALE" else "primeira_wins"] += 1
        elif r == "LOSS":
            x["losses"] += 1
            x["gale_losses" if tipo == "GALE" else "primeira_losses"] += 1
        elif r == "DOJI":
            x["dojis"] += 1
        x["lucro"] = round(float(x.get("lucro", 0.0)) + float(op.get("lucro_operacao", 0.0) or 0.0), 2)
        decididos = x["wins"] + x["losses"]
        x["taxa"] = round(x["wins"] / decididos * 100 if decididos else 0.0, 2)
    return list(resumo.values())


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
        f"➡️ ENTRADA: INICIO DA NOVA VELA M5 (2-8s)\n"
        f"⏱️ EXPIRACAO: FIM DA VELA M5 ATUAL\n\n"
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
    # Telegram profissional: só avisa quando a Bullex confirmou a ordem.
    # Não exibe valor, score, KNN ou outros detalhes internos.
    if status != "CONFIRMADA":
        return
    with _execucao_lock:
        info = _operacoes_ativas_por_symbol.get(symbol, {})
        tipo = info.get("tipo_entrada", "PRIMEIRA")

    direcao = "🟢 CALL ⬆️" if sinal == "CALL" else "🔴 PUT ⬇️"
    if tipo == "GALE":
        texto = (
            "🔄 GALE 1 CONFIRMADO\n\n"
            f"💱 Ativo: {symbol}\n"
            f"📍 Direção: {direcao}\n"
            "⏱ Expiração: fim da mesma vela de 1 minuto"
        )
    else:
        texto = (
            "🚨 SINAL CONFIRMADO\n\n"
            f"💱 Ativo: {symbol}\n"
            f"📍 Direção: {direcao}\n"
            "⏱ Expiração: fim da mesma vela de 1 minuto\n\n"
            "✅ Entrada confirmada"
        )
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

    # Só agora o dashboard passa a mostrar a entrada: a Bullex confirmou a ordem.
    _atualizar_dashboard_intravela(symbol, resultado)

    with _execucao_lock:
        info = _operacoes_ativas_por_symbol.get(symbol, {}).copy()

    operacao = {
        "id": chave,
        "symbol": symbol,
        "asset_id": info.get("asset_id"),
        "mercado": _mercado_do_symbol(symbol),
        "sinal": sinal,
        "score": resultado.get("score", 0),
        "confianca": float(resultado.get("confianca", 0.0) or 0.0),
        "faixa_confianca": resultado.get("faixa_confianca") or _autonomo_faixa_confianca(resultado.get("confianca", 0.0)),
        "ajuste_online": resultado.get("ajuste_online", 0.0),
        "adaptativo": resultado.get("adaptativo", {}),
        "estrategia": resultado.get("estrategia", "M1_SR_LTA_LTB_RETRACAO"),
        "regime": resultado.get("regime", "AUTONOMO"),
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
        "produto": info.get("produto", "DIGITAL_INTRAVELA"),
        "option_id": info.get("option_id"),
        "payout_percent": info.get("payout_percent", BULLEX_PAYOUT_FALLBACK),
        "payout_source": info.get("payout_source", "FALLBACK"),
        "tipo_entrada": info.get("tipo_entrada", "PRIMEIRA"),
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
    operacao = _operacoes_pendentes.get(symbol)
    if not operacao:
        return

    agora = agora_brt()
    alvo_dt = operacao["vela_expiracao"]

    for candle in ordenar_candles(candles):
        dt = candle["_dt"]
        if dt != alvo_dt:
            continue

        if dt + timedelta(minutes=EXPIRACAO_MINUTOS) > agora:
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
        # R30: sem Martingale; LOSS encerra o ciclo imediatamente.
        payout = float(operacao.get("payout_percent", BULLEX_PAYOUT_FALLBACK) or BULLEX_PAYOUT_FALLBACK)
        valor_op = float(operacao.get("valor", 0.0) or 0.0)
        operacao["lucro_operacao"] = round(valor_op * payout / 100.0 if resultado == "WIN" else -valor_op if resultado == "LOSS" else 0.0, 2)
        _historico_resultados.append(operacao.copy())
        del _operacoes_pendentes[symbol]

        with _execucao_lock:
            _operacoes_ativas_por_symbol.pop(symbol, None)
            _operacoes_em_envio.discard(symbol)

        _registrar_fim_de_ciclo(symbol, operacao, resultado)
        _atualizar_progressao(resultado)
        _atualizar_estado_execucao()

        estatisticas = calcular_estatisticas()

        faixa=operacao.get("faixa_confianca", "-")
        conf=float(operacao.get("confianca",0.0) or 0.0)*100.0
        log(
            f"[RESULTADO INTRAVELA] {symbol} {operacao['sinal']} -> {resultado} | "
            f"entrada={entrada:.5f} | fechamento_mesma_vela={saida:.5f} | "
            f"confiança={conf:.1f}% faixa={faixa} | taxa_total={estatisticas['taxa']:.2f}%"
        )

        enviar_resultado_telegram(operacao, estatisticas)
        return




# ============================================================
# TELEGRAM - RESULTADO
# ============================================================

def enviar_resultado_telegram(operacao, estatisticas):
    resultado = operacao.get("resultado", "-")
    symbol = operacao.get("symbol", "-")
    sinal = operacao.get("sinal", "-")
    tipo = operacao.get("tipo_entrada", "PRIMEIRA")
    direcao = "🟢 CALL ⬆️" if sinal == "CALL" else "🔴 PUT ⬇️" if sinal == "PUT" else sinal

    if resultado == "WIN" and tipo != "GALE":
        texto = (
            "🟢 WIN ✅\n\n"
            f"💱 {symbol}\n"
            f"📍 {direcao}\n\n"
            "🏆 WIN M15!"
        )
    elif resultado == "LOSS" and tipo != "GALE":
        texto = (
            "🔴 LOSS ❌\n\n"
            f"💱 {symbol}\n"
            f"📍 {direcao}\n\n"
            "⛔ Operação encerrada em LOSS"
        )
    elif resultado == "WIN" and tipo == "GALE":
        texto = (
            "🟢 WIN NO GALE 1 ✅\n\n"
            f"💱 {symbol}\n\n"
            "🏆 Ciclo finalizado em WIN"
        )
    elif resultado == "LOSS" and tipo == "GALE":
        texto = (
            "🔴 LOSS NO GALE 1 ❌\n\n"
            f"💱 {symbol}\n\n"
            "⛔ Ciclo finalizado em LOSS"
        )
    else:
        texto = f"➖ {symbol} | {resultado}"

    enviar_telegram(texto)


def _ciclos_telegram_desde(inicio, fim):
    """Conta ciclos concluídos no intervalo, sem contar a LOSS inicial duas vezes."""
    primeira_wins = 0
    gale_wins = 0
    ciclos_loss = 0

    with _execucao_lock:
        historico = list(_historico_resultados)

    for op in historico:
        finalizado = op.get("finalizado_em")
        if not isinstance(finalizado, datetime):
            continue
        if not (inicio <= finalizado < fim):
            continue

        resultado = op.get("resultado")
        tipo = op.get("tipo_entrada", "PRIMEIRA")
        if tipo == "GALE":
            if resultado == "WIN":
                gale_wins += 1
            elif resultado == "LOSS":
                ciclos_loss += 1
        elif resultado == "WIN":
            primeira_wins += 1
        elif resultado == "LOSS":
            ciclos_loss += 1
        # R30: sem Gale; WIN/LOSS da primeira encerra o ciclo.

    total = primeira_wins + gale_wins + ciclos_loss
    wins = primeira_wins + gale_wins
    taxa = (wins / total * 100.0) if total else 0.0
    return primeira_wins, gale_wins, ciclos_loss, total, wins, taxa


def loop_parcial_horaria_telegram():
    """Envia uma parcial a cada 1 hora, contando ciclos encerrados naquela hora."""
    inicio = agora_brt()
    while True:
        time.sleep(3600)
        fim = agora_brt()
        primeira_wins, gale_wins, ciclos_loss, total, wins, taxa = _ciclos_telegram_desde(inicio, fim)

        texto = (
            "📊 PARCIAL — ÚLTIMA HORA\n\n"
            f"🟢 WIN M15: {primeira_wins}\n"
            f"🔴 LOSS M15: {ciclos_loss}\n\n"
            f"📈 Total: {total} ciclos\n"
            f"🏆 {wins} WIN | {ciclos_loss} LOSS\n"
            f"🎯 Assertividade: {taxa:.1f}%\n\n"
            "🤖 Sala de Sinais OTC"
        )
        enviar_telegram(texto)
        inicio = fim


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
    """Na R22 o loop mantém histórico M15 e finaliza operações.

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

            # R34: o ciclo de manutenção também precisa informar ao dashboard
            # qual vela M1 acabou de ser processada. Antes este campo só era
            # preenchido quando surgia um setup aprovado, por isso permanecia "-".
            dt_ultima = ultimo.get("_dt")
            if not isinstance(dt_ultima, datetime):
                dt_ultima = parse_datetime_candle(ultimo.get("datetime"))
            estado["vela"] = (
                dt_ultima.strftime("%Y-%m-%d %H:%M:%S BRT")
                if isinstance(dt_ultima, datetime)
                else "-"
            )

            estado["atualizado"] = agora_brt().strftime("%H:%M:%S BRT")
            estado["atualidade_min"] = f"{idade:.1f} min" if idade is not None else "-"
            if estado.get("sinal") not in ("CALL", "PUT"):
                estado["sinal"] = "AGUARDAR"
                estado["mensagem"] = "Monitorando M15: suporte/resistência + LTA/LTB + retração, sem Gale."
        return None

    except Exception as e:
        log(f"ERRO manutenção {symbol}: {e}")
        return None




# ============================================================
# HORÁRIO
# ============================================================

def dentro_do_horario():
    # R25 OTC 24H: sem restrição de horário.
    # O robô opera sempre que os ativos OTC estiverem disponíveis na Traderoom.
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
            "[OTC] Leitura adiada: active_id dos pares ainda não está pronto. "
            f"Detalhe: {erro_ativos}"
        )
        estado["sinal"] = "AGUARDAR"
        estado["score"] = 0
        estado["mensagem"] = (
            "Aguardando carregamento dos pares OTC na Bullex."
        )
        estado["atualizado"] = agora_brt().strftime("%H:%M:%S BRT")
        return

    # R30 M15: libera análise assim que pelo menos um ativo tiver histórico M15 suficiente.
    if not _historico_pronto_event.is_set():
        _precarregar_historico_r22()
        if not _historico_pronto_event.is_set():
            estado["sinal"] = "AGUARDAR"
            estado["mensagem"] = "R30 aguardando histórico M15 suficiente em pelo menos um ativo."
            estado["atualizado"] = agora_brt().strftime("%H:%M:%S BRT")
            log("[R30 PRELOAD M15] Leitura sem sinais: nenhum ativo M15 pronto ainda.")
            return

    # A R30 gera sinais OTC M15 após o preload individual por ativo.
    # Este ciclo de 5 minutos finaliza/atualiza operações e saúde dos ativos.
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
        f"[MONITOR] ativos mapeados={len(ativos_ciclo)} | "
        f"ABERTO={qtd_aberto} | OTC={qtd_otc} | "
        "sinais=AUTONOMO KNN OTC | entrada 2-8s | 2 operações GLOBAIS | 1 por ativo | melhor OTC Digital | 24H"
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
            microsecond=10000,
        )

    else:
        proxima = agora.replace(
            minute=proximo_bloco,
            second=0,
            microsecond=10000,
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
            log(f"[OTC] Pronto para leitura: {ativos_prontos}")
        else:
            log(
                "[OTC] Inicialização ainda incompleta; "
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

        threading.Thread(
            target=loop_parcial_horaria_telegram,
            daemon=True,
            name="telegram-parcial-horaria",
        ).start()

        log(
            "Thread do robo iniciada."
        )
        log(
            "Telegram: parcial por ciclos programada a cada 1 hora de operação."
        )


@app.before_request
def iniciar_robo():
    garantir_robo_iniciado()


def calcular_bloqueios_por_par():
    agora_ts = time.time()
    saida = {}
    for symbol in ATIVOS.values():
        info = _status_bloqueio_ativo(symbol)
        if info:
            restante = max(0, int(float(info.get("ate_ts", 0)) - agora_ts))
            saida[symbol] = {"bloqueado": True, "minutos_restantes": (restante + 59) // 60, "sequencia": int(info.get("sequencia", 0) or 0)}
        else:
            saida[symbol] = {"bloqueado": False, "minutos_restantes": 0, "sequencia": int(_sequencia_ciclos_loss.get(symbol, 0) or 0)}
    return saida



def _dados_grafico_dashboard():
    """Monta o gráfico M1 usando exatamente os candles e níveis calculados pelo robô."""
    symbol_alvo = estado.get("ativo")
    cfg_alvo = None
    codigo_alvo = None

    with _bullex_assets_lock:
        itens = [(codigo, dict(cfg)) for codigo, cfg in ATIVO_BULLEX.items()]

    # Primeiro tenta mostrar o ativo que o robô analisou/sinalizou por último.
    for codigo, cfg in itens:
        if cfg.get("symbol") == symbol_alvo:
            codigo_alvo, cfg_alvo = codigo, cfg
            break

    # Se ainda não houve sinal, escolhe o primeiro ativo que já tenha M15 suficiente.
    if cfg_alvo is None:
        for codigo, cfg in itens:
            aid = cfg.get("active_id")
            if aid is not None and len(_candles_cache(int(aid), 60)) >= 20:
                codigo_alvo, cfg_alvo = codigo, cfg
                break

    if not cfg_alvo:
        return {"pronto": False, "symbol": symbol_alvo or "-", "candles": [], "niveis": []}

    active_id = int(cfg_alvo["active_id"])
    candles = _candles_cache(active_id, 60)[-55:]
    serie = []
    for c in candles:
        try:
            ts = int(float(c.get("from") or 0))
            if not ts:
                dt = c.get("_dt") or parse_datetime_candle(c.get("datetime"))
                ts = int(dt.timestamp()) if dt else 0
            serie.append({
                "t": ts,
                "o": float(c["open"]),
                "h": float(c["high"]),
                "l": float(c["low"]),
                "c": float(c["close"]),
            })
        except Exception:
            continue

    fechadas = somente_velas_fechadas(candles, 1)
    atr15 = atr(fechadas[-SR_M15_LOOKBACK:], 14) if fechadas else None
    niveis = []

    try:
        sup, res, _ = _niveis_sr_m15(active_id)
        for g in sup[-4:]:
            niveis.append({"tipo": "SUPORTE", "nivel": float(g["nivel"]), "toques": int(g.get("toques", 0))})
        for g in res[-4:]:
            niveis.append({"tipo": "RESISTENCIA", "nivel": float(g["nivel"]), "toques": int(g.get("toques", 0))})
    except Exception:
        pass

    if atr15 and fechadas:
        try:
            lta = _linha_tendencia_m15(fechadas[-SR_M15_LOOKBACK:], "LTA", atr15)
            if lta:
                niveis.append({"tipo": "LTA", "nivel": float(lta["nivel"]), "slope": float(lta["slope"]), "toques": 2})
        except Exception:
            pass
        try:
            ltb = _linha_tendencia_m15(fechadas[-SR_M15_LOOKBACK:], "LTB", atr15)
            if ltb:
                niveis.append({"tipo": "LTB", "nivel": float(ltb["nivel"]), "slope": float(ltb["slope"]), "toques": 2})
        except Exception:
            pass

    return {
        "pronto": bool(serie),
        "symbol": cfg_alvo.get("symbol", codigo_alvo),
        "active_id": active_id,
        "candles": serie,
        "niveis": niveis,
        "sinal": estado.get("sinal", "AGUARDAR"),
        "preco": estado.get("preco", "0"),
        "atualizado": estado.get("atualizado", "-"),
    }


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
Radar OTC M1
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


.tabela-pares { width:100%; border-collapse:collapse; font-size:13px; }
.tabela-pares th,.tabela-pares td { padding:8px 5px; border-bottom:1px solid #444; text-align:center; }
.tabela-pares th:first-child,.tabela-pares td:first-child { text-align:left; }
.tabela-wrap { overflow-x:auto; }

.grafico-m1 { width:100%; margin-top:14px; overflow:hidden; background:#151515; border:1px solid #333; border-radius:12px; }
.grafico-m1 svg { display:block; width:100%; height:auto; min-height:300px; }
.legenda-grafico { display:flex; flex-wrap:wrap; gap:12px; justify-content:center; margin-top:10px; color:#bbb; font-size:12px; }
</style>

</head>

<body>

<div class="container">

<h1>
Radar OTC M1
</h1>

<div class="subtitulo">

M1 • Radar de retração • atualização a cada 1 segundo

</div>

<div class="card">
<h3>Execução automática DEMO</h3>
<div class="linha"><span>Modo</span><span class="valor">{{ estado.execucao.modo }}</span></div>
<div class="linha"><span>Automática</span><span class="valor">{{ "ATIVA" if estado.execucao.automatica else "DESATIVADA" }}</span></div>
<div class="linha"><span>Entrada atual</span><span class="valor">R$ {{ "%.2f"|format(estado.execucao.valor_atual) }}</span></div>
<div class="linha"><span>Entrada</span><span class="valor">R$ 5,00 fixa • sem Martingale</span></div>
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
<h3>Visão do robô — gráfico M1</h3>
<div class="linha"><span>Ativo no gráfico</span><span class="valor">{{ grafico.symbol }}</span></div>
<div class="linha"><span>Timeframe</span><span class="valor">M1</span></div>
<div id="grafico-m1" class="grafico-m15">
<svg id="svg-m1" viewBox="0 0 720 390" role="img" aria-label="Candles M1 com suporte, resistência, LTA e LTB"></svg>
</div>
<div id="legenda-grafico" class="legenda-grafico">
<span>— SUP suporte</span><span>— RES resistência</span><span>／ LTA</span><span>＼ LTB</span>
</div>
<div class="observacao" style="margin-top:10px">
As linhas são calculadas com os mesmos candles M1 usados pela estratégia. Atualização automática a cada 10 segundos.
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
<h3>Financeiro</h3>
<div class="linha"><span>Lucro / perda total</span><span class="valor">R$ {{ '%.2f'|format(financeiro.lucro_total) }}</span></div>
<div class="linha"><span>Entrada atual</span><span class="valor">R$ {{ '%.2f'|format(valor_entrada_atual) }}</span></div>
<div class="linha"><span>Gale atual</span><span class="valor">R$ {{ '%.2f'|format(valor_gale_atual) }}</span></div>
</div>

<div class="card">
<h3>Operação atual</h3>
<div class="linha"><span>Último ativo</span><span class="valor">{{ estado.ativo }}</span></div>
<div class="linha"><span>Direção</span><span class="valor">{{ estado.sinal }}</span></div>
<div class="linha"><span>Preço</span><span class="valor">{{ estado.preco }}</span></div>
<div class="linha"><span>Vela M1</span><span class="valor">{{ estado.vela }}</span></div>
</div>

<div class="card">

<div class="observacao">

{{ estado.mensagem }}

<br><br>

Quando houver sinal:

<br>

<strong>
Entrada: durante a retração da vela M1
</strong>

<br>

<strong>
Expiração: fechamento da mesma vela M1
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
(function () {
    const dados = {{ grafico|tojson }};
    const svg = document.getElementById("svg-m1");
    if (!svg) return;

    const NS = "http://www.w3.org/2000/svg";
    const W = 720, H = 390, left = 58, right = 12, top = 16, bottom = 34;
    const candles = Array.isArray(dados.candles) ? dados.candles : [];
    const niveis = Array.isArray(dados.niveis) ? dados.niveis : [];

    function el(tag, attrs, txt) {
        const n = document.createElementNS(NS, tag);
        Object.entries(attrs || {}).forEach(([k,v]) => n.setAttribute(k, String(v)));
        if (txt !== undefined) n.textContent = txt;
        svg.appendChild(n);
        return n;
    }

    if (!candles.length) {
        el("text", {x: W/2, y: H/2, fill:"#aaa", "text-anchor":"middle", "font-size":"16"}, "Aguardando candles M1...");
        return;
    }

    let minP = Math.min(...candles.map(c => c.l), ...niveis.map(n => n.nivel));
    let maxP = Math.max(...candles.map(c => c.h), ...niveis.map(n => n.nivel));
    let pad = Math.max((maxP-minP)*0.08, Math.abs(maxP)*0.0001);
    minP -= pad; maxP += pad;
    const plotW = W-left-right, plotH = H-top-bottom;
    const y = p => top + (maxP-p)/(maxP-minP)*plotH;
    const step = plotW / candles.length;
    const x = i => left + step*(i+0.5);

    // Grade e escala de preço.
    for (let i=0;i<=4;i++) {
        const yy = top + plotH*i/4;
        const price = maxP - (maxP-minP)*i/4;
        el("line",{x1:left,y1:yy,x2:W-right,y2:yy,stroke:"#2d2d2d","stroke-width":"1"});
        el("text",{x:left-6,y:yy+4,fill:"#888","text-anchor":"end","font-size":"10"}, price.toFixed(price < 10 ? 5 : 3));
    }

    // Candles.
    candles.forEach((c,i) => {
        const xx=x(i), up=c.c>=c.o;
        const stroke = up ? "#8bcf9b" : "#e58b8b";
        el("line",{x1:xx,y1:y(c.h),x2:xx,y2:y(c.l),stroke:stroke,"stroke-width":"1.2"});
        const yo=y(Math.max(c.o,c.c)), yc=y(Math.min(c.o,c.c));
        el("rect",{x:xx-Math.max(2,step*0.28),y:yo,width:Math.max(3,step*0.56),height:Math.max(1,yc-yo),fill:stroke,rx:"0.5"});
    });

    // Níveis horizontais e linhas de tendência projetadas.
    niveis.forEach((n,idx) => {
        const isTrend = n.tipo==="LTA" || n.tipo==="LTB";
        const stroke = n.tipo==="SUPORTE" ? "#78aee8" : n.tipo==="RESISTENCIA" ? "#e5b96f" : n.tipo==="LTA" ? "#75c7a1" : "#d78fa7";
        if (isTrend && Number.isFinite(Number(n.slope))) {
            const base = Number(n.nivel);
            const slope = Number(n.slope);
            const p0 = base - slope*(candles.length-1);
            el("line",{x1:x(0),y1:y(p0),x2:x(candles.length-1),y2:y(base),stroke:stroke,"stroke-width":"1.7","stroke-dasharray":"6 4"});
        } else {
            el("line",{x1:left,y1:y(n.nivel),x2:W-right,y2:y(n.nivel),stroke:stroke,"stroke-width":"1.4","stroke-dasharray":"5 4"});
        }
        el("text",{x:W-right-3,y:y(n.nivel)-3,fill:stroke,"text-anchor":"end","font-size":"10"}, n.tipo+" "+Number(n.nivel).toFixed(Number(n.nivel)<10?5:3));
    });

    // Horários aproximados no eixo X.
    [0, Math.floor((candles.length-1)/2), candles.length-1].forEach(i => {
        const d = new Date(Number(candles[i].t)*1000);
        const label = Number.isFinite(d.getTime()) ? d.toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"}) : "";
        el("text",{x:x(i),y:H-10,fill:"#888","text-anchor":"middle","font-size":"10"},label);
    });
})();
setTimeout(function(){ location.reload(); }, 1000);
</script>


<section id="radar-m1-live" style="margin:18px 0;padding:16px;border:1px solid #333;border-radius:14px;">
  <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap">
    <div>
      <h2 style="margin:0 0 4px">Radar M1 ao vivo</h2>
      <div style="opacity:.75">6 ativos mais próximos da entrada • atualização a cada 1 segundo • operação manual</div>
    </div>
    <div id="radar-clock" style="font-weight:700">Atualizando…</div>
  </div>
  <div id="radar-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin-top:14px"></div>
</section>
<script>
(function(){
  const grid = document.getElementById('radar-grid');
  const clock = document.getElementById('radar-clock');
  let ultimoEntrar = '';
  function esc(v){return String(v == null ? '' : v).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));}
  async function atualizarRadar(){
    try{
      const r = await fetch('/radar-m1', {cache:'no-store'});
      const d = await r.json();
      const ativos = Array.isArray(d.ativos) ? d.ativos : [];
      grid.innerHTML = ativos.map(a=>{
        const entrar = a.status === 'ENTRAR AGORA';
        const atencao = a.status === 'ATENÇÃO';
        const borda = entrar ? '3px solid currentColor' : atencao ? '2px solid currentColor' : '1px solid #444';
        const chamada = entrar ? (a.direcao === 'CALL' ? '🟢 COMPRA AGORA' : '🔴 VENDA AGORA') : esc(a.status);
        return `<div style="padding:14px;border:${borda};border-radius:12px">
          <div style="display:flex;justify-content:space-between;gap:8px"><strong>${esc(a.ativo)}</strong><strong>${a.progresso}%</strong></div>
          <div style="font-size:20px;font-weight:800;margin:9px 0">${chamada}</div>
          <div style="opacity:.8">Preço: ${esc(a.preco)} • fecha em ${esc(a.restantes)}s</div>
          <div style="height:8px;background:#333;border-radius:99px;overflow:hidden;margin-top:10px"><div style="height:100%;width:${a.progresso}%;background:currentColor"></div></div>
        </div>`;
      }).join('') || '<div style="opacity:.7">Aguardando candles M1 suficientes…</div>';
      clock.textContent = 'Ao vivo • ' + new Date().toLocaleTimeString();
      const atual = ativos.filter(a=>a.status==='ENTRAR AGORA').map(a=>a.ativo+':'+a.direcao).join('|');
      if(atual && atual !== ultimoEntrar && 'AudioContext' in window){
        try{
          const ac = new AudioContext(), o=ac.createOscillator(), g=ac.createGain();
          o.connect(g); g.connect(ac.destination); o.frequency.value=880; g.gain.value=.05; o.start(); o.stop(ac.currentTime+.15);
        }catch(e){}
      }
      ultimoEntrar = atual;
    }catch(e){
      clock.textContent = 'Reconectando…';
    }
  }
  atualizarRadar();
  setInterval(atualizarRadar,1000);
})();
</script>

</body>

</html>
"""



# R40_MANUAL_FORCE: este app é radar manual; não envia ordens automaticamente.
BULLEX_AUTO_TRADE = False

# ============================================================
# ROTAS
# ============================================================


def _radar_m1_ao_vivo():
    """R40: até 6 ativos OTC mais próximos da entrada, usando candles M1 reais do WS."""
    itens = []
    agora, _ = _horario_servidor_atual()
    candle_from_atual = int(agora // 60) * 60
    restantes = max(0, int(candle_from_atual + 60 - agora))

    # ATIVO_BULLEX é atualizado pela descoberta dinâmica da Traderoom.
    with _bullex_assets_lock:
        configs = [dict(v) for v in ATIVO_BULLEX.values() if isinstance(v, dict)]

    for cfg in configs:
        try:
            aid = int(cfg.get("active_id"))
            simbolo = cfg.get("symbol") or cfg.get("ticker") or str(aid)
            candles = _candles_cache(aid, 60)
            if not candles or len(candles) < 25:
                continue

            # Localiza a vela M1 corrente recebida pelo candle-generated.
            corrente = None
            for c in reversed(candles):
                try:
                    cf = int(float(c.get("from", c.get("at", c.get("timestamp", 0))) or 0))
                except Exception:
                    cf = 0
                if cf == candle_from_atual:
                    corrente = dict(c)
                    break
            if corrente is None:
                corrente = dict(candles[-1])

            corrente["active_id"] = aid
            corrente["size"] = 60
            if "from" not in corrente:
                corrente["from"] = candle_from_atual
            if "to" not in corrente:
                corrente["to"] = int(corrente["from"]) + 60

            preco = float(corrente.get("close", corrente.get("price", 0)) or 0)
            resultado = _resultado_retracao_intravela(corrente, aid)

            direcao = None
            confianca = 0.0
            detalhe = ""
            if isinstance(resultado, dict):
                direcao = resultado.get("sinal")
                confianca = float(resultado.get("confianca", 0) or 0)
                detalhe = str(resultado.get("pullback") or resultado.get("estrategia") or "")

            # Progresso visual NÃO é probabilidade de vitória.
            # Mede proximidade do preço a um extremo da faixa recente enquanto
            # o setup completo ainda não foi confirmado.
            if confianca <= 0:
                recentes = candles[-20:]
                highs = [float(c.get("high", c.get("max", 0)) or 0) for c in recentes]
                lows = [float(c.get("low", c.get("min", 0)) or 0) for c in recentes]
                highs = [v for v in highs if v > 0]
                lows = [v for v in lows if v > 0]
                if highs and lows and preco > 0:
                    hi, lo = max(highs), min(lows)
                    amplitude = max(hi - lo, 1e-12)
                    pos = max(0.0, min(1.0, (preco - lo) / amplitude))
                    proximidade_extremo = max(pos, 1.0 - pos)
                    confianca = min(0.79, max(0.20, proximidade_extremo * 0.79))
                else:
                    confianca = 0.20

            pct = int(round(confianca * 100 if confianca <= 1 else confianca))
            pct = max(0, min(100, pct))

            if direcao in ("CALL", "PUT"):
                status = "ENTRAR AGORA"
                pct = max(pct, 90)
            elif pct >= 80:
                status = "ATENÇÃO"
            elif pct >= 65:
                status = "APROXIMANDO"
            else:
                status = "AGUARDANDO"

            itens.append({
                "ativo": simbolo,
                "active_id": aid,
                "preco": preco,
                "progresso": pct,
                "status": status,
                "direcao": direcao,
                "restantes": restantes,
                "detalhe": detalhe,
            })
        except Exception as e:
            log(f"[RADAR M1] ativo ignorado: {e}")
            continue

    itens.sort(
        key=lambda x: (
            1 if x["status"] == "ENTRAR AGORA" else 0,
            x["progresso"]
        ),
        reverse=True
    )
    return itens[:6]


@app.route("/radar-m1")
def radar_m1():
    return jsonify({
        "status": "ok",
        "timeframe": "M1",
        "modo": "MANUAL",
        "atualizacao_ms": 1000,
        "ativos": _radar_m1_ao_vivo(),
        "candles_m1_armazenados": sum(len(_candles_cache(int(v.get("active_id")), 60)) for v in ATIVO_BULLEX.values() if isinstance(v, dict) and v.get("active_id") is not None),
    })


@app.route("/")
def index():
    garantir_robo_iniciado()

    estado[
        "estatisticas"
    ] = calcular_estatisticas()

    return render_template_string(
        HTML,
        estado=estado,
        grafico=_dados_grafico_dashboard(),
        financeiro=calcular_financeiro(),
        valor_entrada_atual=_valor_entrada_atual(),
        valor_gale_atual=_valor_gale_atual(),
        bloqueios_pares=calcular_bloqueios_por_par()
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
                "AUTONOMO KNN OTC: aprendizado por ativo + seletor GLOBAL do melhor OTC Digital | 24H"
            ),
        "fonte_candles": "Bullex",
        "execucao_automatica": BULLEX_AUTO_TRADE,
        "modo_execucao": "DEMO",
        "valor_entrada_atual": _valor_entrada_atual(),
        "nivel_progressao": _nivel_progressao,
        "operacoes_ativas_por_symbol": _operacoes_ativas_por_symbol,
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
        "gale_valor": _valor_gale_atual(),
        "entrada_valor": _valor_entrada_atual(),
        "financeiro": calcular_financeiro(),
        "gale_maximo": 1,
        "gales_pendentes": list(_gales_pendentes.keys()),
        "bloqueios_por_ativo": calcular_bloqueios_por_par(),
        "bloqueio_apos_ciclos_loss": BLOQUEIO_ATIVO_CICLOS_LOSS,
        "bloqueio_minutos": BLOQUEIO_ATIVO_SEGUNDOS // 60,
        "progressao_ativa": True,
        "historico_preload_pronto": _historico_pronto_event.is_set(),
        "historico_preload_ultima_tentativa": _historico_preload_ultima_tentativa,
        "historico_preload_status": dict(_historico_preload_status),
        "mercado": "OTC",
        "ativos_otc": {
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
                "1000"
            )
        ),
        debug=False,
    )