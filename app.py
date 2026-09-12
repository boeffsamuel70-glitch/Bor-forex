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

# OTC automático. Os active_id não ficam fixos no código:
# são descobertos automaticamente na lista digital da Traderoom após autenticar.
ATIVO_BULLEX = {}

_bullex_assets_lock = threading.RLock()
_bullex_assets_detected = False
_bullex_assets_last_error = None
_bullex_assets_updated_at = None
_bullex_assets_source = None
_bullex_assets_ready_event = threading.Event()
_bullex_assets_init_lock = threading.Lock()

_BULLEX_CANDLE_SIZES = {"1min": 60, "5min": 300}

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
BULLEX_DIAGNOSTIC_VERSION = "R31-OTC-FIM-M5-M15-ATE1S-MAX2-DIAG-CORRIGIDO"

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
TIMEFRAME_TREND = "5min"

TIMEZONE = "America/Sao_Paulo"
TZ = ZoneInfo(TIMEZONE)

OUTPUTSIZE = 150
OUTPUTSIZE_5M = 100

HORA_INICIO = 22
HORA_FIM = 15

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

VALORES_ENTRADA = [6.00, 7.00, 8.00, 9.00]

# ============================================================
# GERENCIAMENTO AUTÔNOMO DE BANCA
# ============================================================
# Valores padrão podem ser alterados no Render sem editar o código.
ENTRADA_BASE = float(os.getenv("ENTRADA_BASE", "6").replace(",", "."))
ENTRADA_MAXIMA = float(os.getenv("ENTRADA_MAXIMA", "9").replace(",", "."))
META_LUCRO_DIA = float(os.getenv("META_LUCRO_DIA", "60").replace(",", "."))
STOP_LOSS_DIA = float(os.getenv("STOP_LOSS_DIA", "24").replace(",", "."))
TRAVA_LUCRO_ATIVA_APOS = float(
    os.getenv("TRAVA_LUCRO_ATIVA_APOS", "30").replace(",", ".")
)
TRAVA_LUCRO_RECUO = float(
    os.getenv("TRAVA_LUCRO_RECUO", "10").replace(",", ".")
)

# A mão cresce somente com lucro já conquistado:
# +0 a +14,99  -> R$6
# +15 a +29,99 -> R$7
# +30 a +44,99 -> R$8
# +45 ou mais  -> R$9
DEGRAU_LUCRO_PARA_AUMENTO = float(
    os.getenv("DEGRAU_LUCRO_PARA_AUMENTO", "15").replace(",", ".")
)

EXPIRACAO_MINUTOS = 1
# A antiga janela de 3 segundos foi removida.
# Esta estratégia entra DURANTE a vela M1 atual e expira no fechamento da MESMA vela.
INTRAVELA_MIN_SEGUNDOS_DECORRIDOS = 2
INTRAVELA_MIN_SEGUNDOS_RESTANTES = 50

# Filtro de contexto/força no M5. O M5 não executa a entrada; ele apenas
# autoriza operações na direção de uma tendência suficientemente forte.
M5_ADX_PERIODO = 14
M5_ADX_MINIMO = 20.0
M5_SEPARACAO_EMAS_ATR_MIN = 0.12
M5_INCLINACAO_ATR_MIN = 0.03
M5_IMPULSO_CANDLES = 3
M5_IMPULSO_MIN_DIRECIONAIS = 2

# Parâmetros antigos de S/R mantidos apenas por compatibilidade com helpers legados.
SR_M5_LOOKBACK = 100
SR_M5_PIVOT_JANELA = 2
SR_M5_MIN_TOQUES = 2
SR_M5_TOLERANCIA_ATR = 0.18

# A vela M1 precisa vir de uma distância mínima até o nível.
# Se abrir colada no suporte/resistência, não opera.
SR_M1_DISTANCIA_ABERTURA_ATR_MIN = 0.55

# Rejeição/retração depois do toque.
INTRAVELA_RETRACAO_MIN = 0.20
INTRAVELA_RETRACAO_MAX = 0.68
INTRAVELA_REJEICAO_ATR_MIN = 0.10
INTRAVELA_PAVIO_MIN_FRACAO_MOVIMENTO = 0.10

UMA_OPERACAO_GLOBAL = False
MAX_OPERACOES_SIMULTANEAS = 2

# Após um LOSS, somente o ativo que perdeu fica bloqueado por 40 minutos.
BLOQUEIO_LOSS_MINUTOS = 40
_bloqueio_loss_lock = threading.RLock()
_bloqueio_loss_ate = {}

_intravela_lock = threading.RLock()
_intravela_estado = {}
_intravela_velas_tentadas = set()

# ============================================================
# ATIVOS
# ============================================================

ATIVOS = {}

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
        "lucro_total": 0.0,
    },
}

estado["execucao"] = {
    "automatica": BULLEX_AUTO_TRADE,
    "modo": "DEMO",
    "valor_atual": ENTRADA_BASE,
    "nivel_progressao": 0,
    "operacao_ativa": False,
    "ultima_ordem": None,
    "ultimo_erro": None,
    "balance_id_disponivel": bool(BULLEX_USER_BALANCE_ID),
    "balance_source": "ENV" if BULLEX_USER_BALANCE_ID else None,
    "gerenciamento": {
        "meta_lucro": META_LUCRO_DIA,
        "stop_loss": STOP_LOSS_DIA,
        "trava_ativa_apos": TRAVA_LUCRO_ATIVA_APOS,
        "trava_recuo": TRAVA_LUCRO_RECUO,
        "lucro_dia": 0.0,
        "pico_lucro_dia": 0.0,
        "status": "ATIVO",
        "motivo_parada": None,
    },
}

_robo_lock = threading.Lock()
_robo_started = False

_ultimos_sinais_telegram = {}
_operacoes_pendentes = {}
_ultimas_operacoes_registradas = {}
# Nova contagem do dashboard a partir desta versão.
# WIN/LOSS/DOJI e lucro começam zerados após o deploy.
_historico_resultados = []

# Fallback de payout líquido usado SOMENTE quando a Bullex não informar
# o valor financeiro real da liquidação da operação.
# Ex.: entrada de R$6,00 com 87% -> lucro líquido de R$5,22.
# Pode ser ajustado no Render pela variável PAYOUT_LUCRO_PERCENTUAL.
PAYOUT_LUCRO_PERCENTUAL = float(
    os.getenv("PAYOUT_LUCRO_PERCENTUAL", "87").replace(",", ".")
)

# Liquidações financeiras recebidas da Bullex, indexadas por option_id.
# O objetivo é usar o valor REAL devolvido pela corretora quando disponível,
# em vez de presumir um payout fixo para todos os ativos/operações.
_bullex_settlement_lock = threading.RLock()
_bullex_settlements = {}

_execucao_lock = threading.RLock()
_operacao_global_ativa = None
_nivel_progressao = 0
_bullex_balance_id = None
_bullex_balance_source = None
_bullex_instrument_cache = {}

# ============================================================
# HORÁRIO DO SERVIDOR / JANELA DE ENTRADA M1
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


def _janela_execucao_m1():
    server_ts, source = _horario_servidor_atual()

    current = int(server_ts)
    candle_open = current - (current % 60)
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


def _float_seguro(valor):
    try:
        if valor in (None, ""):
            return None
        if isinstance(valor, str):
            valor = valor.strip().replace(",", ".")
        return float(valor)
    except (TypeError, ValueError):
        return None


def _normalizar_resultado_bullex(valor):
    if valor is None:
        return None
    txt = str(valor).strip().lower()
    if txt in ("win", "won", "winner", "success", "profit"):
        return "WIN"
    if txt in ("loss", "lose", "loose", "lost", "losses", "fail", "failed"):
        return "LOSS"
    if txt in ("equal", "draw", "doji", "refund", "tie"):
        return "DOJI"
    return None


def _iter_dicts_liquidacao(obj):
    if isinstance(obj, dict):
        yield obj
        for valor in obj.values():
            yield from _iter_dicts_liquidacao(valor)
    elif isinstance(obj, list):
        for valor in obj:
            yield from _iter_dicts_liquidacao(valor)


def _ids_opcoes_conhecidas():
    ids = {}
    for op in list(_operacoes_pendentes.values()):
        oid = op.get("option_id")
        if oid not in (None, "", 0):
            ids[str(oid)] = float(op.get("valor") or 0.0)
    for op in list(_historico_resultados):
        oid = op.get("option_id")
        if oid not in (None, "", 0):
            ids[str(oid)] = float(op.get("valor") or 0.0)
    return ids


def _extrair_liquidacao_do_item(item, valor_operacao):
    if not isinstance(item, dict):
        return None

    resultado = None
    for chave in ("win", "result", "resultado", "outcome"):
        resultado = _normalizar_resultado_bullex(item.get(chave))
        if resultado:
            break

    valor_ref = _float_seguro(
        item.get("amount", item.get("price", item.get("invest", valor_operacao)))
    )
    if valor_ref is None or valor_ref <= 0:
        valor_ref = float(valor_operacao or 0.0)

    # Campos que normalmente representam lucro/prejuízo LÍQUIDO.
    for chave in ("net_profit", "profit_net", "netProfit", "profit_value"):
        valor = _float_seguro(item.get(chave))
        if valor is not None:
            return {"lucro": round(valor, 2), "resultado": resultado, "campo": chave}

    # Em mensagens do ecossistema IQ/Bullex, profit_amount costuma representar
    # o TOTAL devolvido (entrada + lucro). Por isso subtraímos a entrada.
    for chave in ("profit_amount", "return_amount", "payout_amount", "win_amount"):
        valor = _float_seguro(item.get(chave))
        if valor is not None:
            lucro = valor - valor_ref
            if resultado == "LOSS" and valor <= 0:
                lucro = -valor_ref
            elif resultado == "DOJI":
                lucro = 0.0
            return {"lucro": round(lucro, 2), "resultado": resultado, "campo": chave}

    # Mesmo sem valor financeiro explícito, LOSS e DOJI têm resultado líquido
    # conhecido. Para WIN sem valor, aguardamos o fallback da avaliação.
    if resultado == "LOSS":
        return {"lucro": round(-valor_ref, 2), "resultado": resultado, "campo": "resultado"}
    if resultado == "DOJI":
        return {"lucro": 0.0, "resultado": resultado, "campo": "resultado"}

    return None


def _aplicar_liquidacao_real(option_id, liquidacao, payload=None):
    oid = str(option_id)
    lucro = float(liquidacao["lucro"])
    resultado = liquidacao.get("resultado")

    registro = {
        "option_id": oid,
        "lucro": round(lucro, 2),
        "resultado": resultado,
        "campo": liquidacao.get("campo"),
        "recebido_em": agora_brt().isoformat(),
    }
    with _bullex_settlement_lock:
        _bullex_settlements[oid] = registro

    # Atualiza operação pendente para que a finalização use o valor real.
    for op in list(_operacoes_pendentes.values()):
        if str(op.get("option_id")) == oid:
            op["lucro_real"] = round(lucro, 2)
            op["fonte_lucro"] = "BULLEX_REAL"
            if resultado:
                op["resultado_bullex"] = resultado

    # Se a mensagem chegar depois de a operação já ter sido colocada no
    # histórico, corrige o lucro acumulado sem criar uma segunda operação.
    for op in _historico_resultados:
        if str(op.get("option_id")) == oid:
            op["lucro"] = round(lucro, 2)
            op["lucro_real"] = round(lucro, 2)
            op["fonte_lucro"] = "BULLEX_REAL"
            if resultado:
                op["resultado"] = resultado
                op["resultado_bullex"] = resultado

    log(
        f"[LIQUIDACAO REAL] option_id={oid} | "
        f"resultado={resultado or '-'} | lucro_liquido=R${lucro:.2f} | "
        f"campo={liquidacao.get('campo')}"
    )


def _capturar_liquidacao_bullex(data):
    conhecidos = _ids_opcoes_conhecidas()
    if not conhecidos:
        return

    for item in _iter_dicts_liquidacao(data):
        candidatos = []
        for chave in ("option_id", "optionId", "id", "position_id", "positionId"):
            valor = item.get(chave)
            if valor not in (None, "", 0):
                candidatos.append(str(valor))

        for oid in candidatos:
            if oid not in conhecidos:
                continue
            liquidacao = _extrair_liquidacao_do_item(item, conhecidos[oid])
            if liquidacao is None:
                continue
            _aplicar_liquidacao_real(oid, liquidacao, item)
            return


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

def _historico_hoje():
    hoje = agora_brt().date()
    itens = []
    for item in _historico_resultados:
        dt = item.get("finalizado_em")
        if isinstance(dt, str):
            try:
                dt = datetime.fromisoformat(dt)
            except Exception:
                dt = None
        if isinstance(dt, datetime):
            if dt.astimezone(TZ).date() == hoje:
                itens.append(item)
        else:
            # Registros antigos sem horário pertencem à sessão atual.
            itens.append(item)
    return itens


def _resumo_gerenciamento():
    itens = _historico_hoje()
    acumulado = 0.0
    pico = 0.0

    for item in itens:
        lucro = _float_seguro(item.get("lucro"))
        if lucro is None:
            resultado = item.get("resultado")
            valor = float(item.get("valor") or 0.0)
            if resultado == "WIN":
                lucro = valor * (PAYOUT_LUCRO_PERCENTUAL / 100.0)
            elif resultado == "LOSS":
                lucro = -valor
            else:
                lucro = 0.0

        acumulado += lucro
        pico = max(pico, acumulado)

    acumulado = round(acumulado, 2)
    pico = round(pico, 2)

    status = "ATIVO"
    motivo = None

    if acumulado >= META_LUCRO_DIA:
        status = "PARADO"
        motivo = "META_LUCRO"
    elif acumulado <= -abs(STOP_LOSS_DIA):
        status = "PARADO"
        motivo = "STOP_LOSS"
    elif (
        pico >= TRAVA_LUCRO_ATIVA_APOS
        and acumulado <= (pico - TRAVA_LUCRO_RECUO)
    ):
        status = "PARADO"
        motivo = "TRAVA_LUCRO"

    return {
        "lucro_dia": acumulado,
        "pico_lucro_dia": pico,
        "status": status,
        "motivo_parada": motivo,
        "meta_lucro": META_LUCRO_DIA,
        "stop_loss": STOP_LOSS_DIA,
        "trava_ativa_apos": TRAVA_LUCRO_ATIVA_APOS,
        "trava_recuo": TRAVA_LUCRO_RECUO,
    }


def _gerenciamento_permite_operar():
    resumo = _resumo_gerenciamento()
    return resumo["status"] == "ATIVO", resumo


def _valor_entrada_atual():
    """Define a mão usando apenas lucro já conquistado.

    Qualquer LOSS na última operação do dia força a próxima mão para a base.
    Fora isso, sobe R$1 a cada R$15 de lucro acumulado, limitado a R$9.
    """
    itens = _historico_hoje()
    if itens and itens[-1].get("resultado") == "LOSS":
        return round(float(ENTRADA_BASE), 2)

    lucro = max(0.0, float(_resumo_gerenciamento()["lucro_dia"]))
    if DEGRAU_LUCRO_PARA_AUMENTO <= 0:
        nivel = 0
    else:
        nivel = int(lucro // DEGRAU_LUCRO_PARA_AUMENTO)

    valor = ENTRADA_BASE + nivel
    valor = max(ENTRADA_BASE, min(ENTRADA_MAXIMA, valor))
    return round(float(valor), 2)


def _atualizar_estado_execucao():
    resumo = _resumo_gerenciamento()
    valor = _valor_entrada_atual()
    nivel = int(round(max(0.0, valor - ENTRADA_BASE)))

    estado["execucao"].update({
        "automatica": BULLEX_AUTO_TRADE,
        "modo": "DEMO",
        "valor_atual": valor,
        "nivel_progressao": nivel,
        "payout_lucro_percentual": PAYOUT_LUCRO_PERCENTUAL,
        "operacao_ativa": bool(_operacoes_pendentes) or _operacao_global_ativa is not None,
        "operacoes_abertas": len(_operacoes_pendentes),
        "max_operacoes_simultaneas": MAX_OPERACOES_SIMULTANEAS,
        "balance_id_disponivel": _bullex_balance_id is not None,
        "balance_source": _bullex_balance_source,
        "gerenciamento": resumo,
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
    return agora.replace(second=0, microsecond=0)


def _montar_instrument_id(active_id, dt=None):
    if dt is None:
        dt = _instrument_time()
    return f"do{int(active_id)}{dt.strftime('%Y%m%d')}D{dt.strftime('%H%M')}T1MPSPT"


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


def _instrumento_eh_1m(item, expected_id):
    iid = str(item.get("instrument_id", ""))
    return iid == expected_id or "T1M" in iid.upper()


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
                          if _instrumento_eh_1m(x, expected)]
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

    pode_operar, gerenciamento = _gerenciamento_permite_operar()
    if not pode_operar:
        motivo = gerenciamento.get("motivo_parada") or "GERENCIAMENTO"
        estado["execucao"]["ultimo_erro"] = f"PARADO_{motivo}"
        _atualizar_estado_execucao()
        log(
            f"[GERENCIAMENTO] Nova ordem bloqueada: {motivo} | "
            f"lucro_dia=R${gerenciamento['lucro_dia']:.2f} | "
            f"pico=R${gerenciamento['pico_lucro_dia']:.2f}"
        )
        return f"PARADO_{motivo}"

    if not BULLEX_USER_BALANCE_ID:
        estado["execucao"]["ultimo_erro"] = "SEM_BALANCE_ID"
        _atualizar_estado_execucao()
        return "SEM_BALANCE_ID"

    with _execucao_lock:
        em_confirmacao = 1 if _operacao_global_ativa is not None else 0
        abertas = len(_operacoes_pendentes) + em_confirmacao
        if abertas >= MAX_OPERACOES_SIMULTANEAS:
            log(
                f"[INTRAVELA] {symbol}: sinal ignorado; "
                f"limite de {MAX_OPERACOES_SIMULTANEAS} operações simultâneas atingido."
            )
            return "LIMITE_OPERACOES"

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
    """Compatibilidade: a mão agora é calculada pelo gerenciamento autônomo."""
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
        f"{len(ativos)} ativo(s) OTC reconhecido(s)."
    )

    if ativos:
        try:
            _atualizar_ativos_mercado_aberto(ativos, f"{nome} v{version} IMEDIATO")
            _assinar_candles_mercado_aberto()
            log(
                f"[OTC AUTO] Inicialização imediata concluída com "
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

    # Tenta capturar liquidação financeira real de qualquer evento/resposta
    # da Bullex antes dos returns específicos de autenticação/candles.
    try:
        _capturar_liquidacao_bullex(data)
    except Exception as e:
        log(f"[LIQUIDACAO REAL] Falha ao interpretar evento: {e}")

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

                # Estratégia de reversão: observa a vela de M1 ainda aberta.
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
    """Compatibilidade: agora normaliza SOMENTE ativos OTC disponíveis."""
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
    texto = " ".join(str(x) for x in campos_texto if x not in (None, "")).upper()
    eh_otc = item.get("is_otc") is True or item.get("isOtc") is True or "OTC" in texto
    if not eh_otc:
        return None

    # Ignora ativos explicitamente pausados/invisíveis quando a lista informa isso.
    if item.get("is_paused") is True or item.get("isPaused") is True:
        return None
    if item.get("is_visible") is False or item.get("isVisible") is False:
        return None

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
    ticker = str(ticker or symbol or f"OTC-{active_id}").strip()
    symbol = str(symbol or ticker).strip()

    # Código único e estável no processo, sem limitar a uma lista fixa de pares.
    codigo_base = re.sub(r"[^A-Z0-9]", "", ticker.upper()) or f"OTC{active_id}"
    codigo = f"{codigo_base}_{active_id}"

    return {
        "codigo": codigo,
        "symbol": symbol,
        "active_id": active_id,
        "ticker": ticker,
        "raw": item,
    }


def _extrair_mercado_aberto_da_resposta(resposta):
    """Compatibilidade: extrai todos os OTC elegíveis retornados pela Traderoom."""
    encontrados = {}
    for item in _iter_dicts_recursivo(resposta):
        normalizado = _normalizar_par_mercado_aberto(item)
        if not normalizado:
            continue
        aid = int(normalizado["active_id"])
        atual = encontrados.get(aid)
        if atual is None:
            encontrados[aid] = normalizado
            continue
        raw_novo = normalizado.get("raw") or {}
        raw_atual = atual.get("raw") or {}
        score_novo = int(raw_novo.get("is_visible") is True) + int(raw_novo.get("is_active") is True)
        score_atual = int(raw_atual.get("is_visible") is True) + int(raw_atual.get("is_active") is True)
        if score_novo > score_atual:
            encontrados[aid] = normalizado
    return sorted(encontrados.values(), key=lambda x: (x["ticker"], x["active_id"]))


def _corpo_lista_instrumentos(nome):
    if nome == "digital-option-instruments.get-underlying-list":
        return {"type": "digital-option"}
    return None


def _consultar_lista_mercado_aberto(nome, versoes=("2.0", "1.0")):
    ultimo_erro = None
    body = _corpo_lista_instrumentos(nome)
    for versao in versoes:
        try:
            resposta = _enviar_e_aguardar(nome, versao, body, timeout=12)
            ativos = _extrair_mercado_aberto_da_resposta(resposta)
            log(
                f"[OTC AUTO] {nome} v{versao}: "
                f"{len(ativos)} ativo(s) OTC reconhecido(s)."
            )
            if ativos:
                return resposta, ativos
        except Exception as e:
            ultimo_erro = e
            log(f"[OTC AUTO] Falha em {nome} v{versao}: {e}")
    if ultimo_erro:
        raise ultimo_erro
    return None, []


def _atualizar_ativos_mercado_aberto(ativos, origem):
    global ATIVO_BULLEX
    global ATIVOS
    global _bullex_assets_detected
    global _bullex_assets_last_error
    global _bullex_assets_updated_at
    global _bullex_assets_source

    if not ativos:
        raise RuntimeError("Nenhum ativo OTC disponível foi encontrado na Traderoom.")

    novos_bullex = {}
    novos_ativos = {}
    for item in ativos:
        codigo = item["codigo"]
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
        "[OTC AUTO] Ativos carregados: "
        + ", ".join(
            f"{cfg['ticker']}={cfg['active_id']}"
            for cfg in novos_bullex.values()
        )
    )


def _inicializar_ativos_mercado_aberto():
    """Descobre automaticamente todos os ativos OTC disponíveis.

    A inicialização é serializada para impedir duas descobertas concorrentes
    após reconexões rápidas do WebSocket.
    """
    global _bullex_assets_last_error

    if not _bullex_assets_init_lock.acquire(blocking=False):
        log("[OTC AUTO] Descoberta de ativos já está em andamento.")
        return

    try:
        _bullex_assets_ready_event.clear()
        fonte_digital = "digital-option-instruments.get-underlying-list"

        try:
            _, ativos = _consultar_lista_mercado_aberto(fonte_digital)
            if not ativos:
                raise RuntimeError(
                    "Lista digital não retornou ativos OTC disponíveis."
                )

            _atualizar_ativos_mercado_aberto(ativos, fonte_digital)
            _assinar_candles_mercado_aberto()
            log(
                f"[OTC AUTO] Inicialização concluída com {len(ativos)} ativo(s)."
            )
            return

        except Exception as e:
            _bullex_assets_last_error = str(e)
            log(f"[OTC AUTO] Descoberta digital falhou: {e}")

        # Limpa o mapa enquanto a Traderoom não retornar OTCs válidos.
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
            "[OTC AUTO] Ativos ainda não disponíveis. "
            "A leitura ficará bloqueada até nova autenticação/descoberta; Somente OTC será usado."
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
    """Assina M1 e M5 dos ativos usados pelo robô."""
    assinaturas = set()

    for config in ATIVO_BULLEX.values():
        active_id = int(config["active_id"])
        for size in (60, 300):
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


def adx(candles, period=14):
    """Calcula ADX de Wilder para medir força de tendência, sem direção."""
    if len(candles) < (period * 2) + 1:
        return None

    trs = []
    plus_dm = []
    minus_dm = []
    for i in range(1, len(candles)):
        atual = candles[i]
        anterior = candles[i - 1]
        high = float(atual["high"])
        low = float(atual["low"])
        prev_high = float(anterior["high"])
        prev_low = float(anterior["low"])
        prev_close = float(anterior["close"])

        up = high - prev_high
        down = prev_low - low
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))

    if len(trs) < period * 2:
        return None

    tr_s = sum(trs[:period])
    p_s = sum(plus_dm[:period])
    m_s = sum(minus_dm[:period])
    dxs = []

    def _dx(trv, pv, mv):
        if trv <= 0:
            return None
        pdi = 100.0 * pv / trv
        mdi = 100.0 * mv / trv
        den = pdi + mdi
        if den <= 0:
            return 0.0
        return 100.0 * abs(pdi - mdi) / den

    first = _dx(tr_s, p_s, m_s)
    if first is not None:
        dxs.append(first)

    for i in range(period, len(trs)):
        tr_s = tr_s - (tr_s / period) + trs[i]
        p_s = p_s - (p_s / period) + plus_dm[i]
        m_s = m_s - (m_s / period) + minus_dm[i]
        valor = _dx(tr_s, p_s, m_s)
        if valor is not None:
            dxs.append(valor)

    if len(dxs) < period:
        return None

    valor_adx = sum(dxs[:period]) / period
    for valor in dxs[period:]:
        valor_adx = ((valor_adx * (period - 1)) + valor) / period
    return valor_adx


def _contexto_forca_m5(active_id):
    """Retorna direção M5 somente quando tendência e força são suficientes."""
    candles = somente_velas_fechadas(_candles_cache(active_id, 300), 5)
    if len(candles) < 40:
        return None
    candles = candles[-80:]

    closes = [float(c["close"]) for c in candles]
    ema9s = ema_series(closes, 9)
    ema21s = ema_series(closes, 21)
    ema9 = ema9s[-1]
    ema21 = ema21s[-1]
    ema9_prev = ema9s[-3] if len(ema9s) >= 3 else None
    ema21_prev = ema21s[-3] if len(ema21s) >= 3 else None
    atr5 = atr(candles, 14)
    adx15 = adx(candles, M5_ADX_PERIODO)

    if None in (ema9, ema21, ema9_prev, ema21_prev) or not atr5 or atr5 <= 0 or adx15 is None:
        return None
    if adx15 < M5_ADX_MINIMO:
        return None

    separacao = abs(ema9 - ema21)
    if separacao < atr5 * M5_SEPARACAO_EMAS_ATR_MIN:
        return None

    inclinacao9 = ema9 - ema9_prev
    inclinacao21 = ema21 - ema21_prev
    ultimos = candles[-M5_IMPULSO_CANDLES:]
    altas = sum(float(c["close"]) > float(c["open"]) for c in ultimos)
    baixas = sum(float(c["close"]) < float(c["open"]) for c in ultimos)

    min_inclinacao = atr5 * M5_INCLINACAO_ATR_MIN
    if (
        ema9 > ema21
        and inclinacao9 >= min_inclinacao
        and inclinacao21 > 0
        and altas >= M5_IMPULSO_MIN_DIRECIONAIS
    ):
        return {
            "direcao": "CALL", "ema9": ema9, "ema21": ema21,
            "adx": adx15, "atr": atr5, "separacao": separacao,
            "impulso": altas,
        }

    if (
        ema9 < ema21
        and inclinacao9 <= -min_inclinacao
        and inclinacao21 < 0
        and baixas >= M5_IMPULSO_MIN_DIRECIONAIS
    ):
        return {
            "direcao": "PUT", "ema9": ema9, "ema21": ema21,
            "adx": adx15, "atr": atr5, "separacao": separacao,
            "impulso": baixas,
        }

    return None


# ============================================================
# INFORMAÇÕES DA VELA
# ============================================================


def _contexto_forca_m15(active_id):
    """Retorna direção M15 somente quando tendência e força são suficientes."""
    candles = somente_velas_fechadas(_candles_cache(active_id, 900), 5)
    if len(candles) < 40:
        return None
    candles = candles[-80:]

    closes = [float(c["close"]) for c in candles]
    ema9s = ema_series(closes, 9)
    ema21s = ema_series(closes, 21)
    ema9 = ema9s[-1]
    ema21 = ema21s[-1]
    ema9_prev = ema9s[-3] if len(ema9s) >= 3 else None
    ema21_prev = ema21s[-3] if len(ema21s) >= 3 else None
    atr5 = atr(candles, 14)
    adx15 = adx(candles, M15_ADX_PERIODO)

    if None in (ema9, ema21, ema9_prev, ema21_prev) or not atr5 or atr5 <= 0 or adx15 is None:
        return None
    if adx15 < M15_ADX_MINIMO:
        return None

    separacao = abs(ema9 - ema21)
    if separacao < atr5 * M15_SEPARACAO_EMAS_ATR_MIN:
        return None

    inclinacao9 = ema9 - ema9_prev
    inclinacao21 = ema21 - ema21_prev
    ultimos = candles[-M15_IMPULSO_CANDLES:]
    altas = sum(float(c["close"]) > float(c["open"]) for c in ultimos)
    baixas = sum(float(c["close"]) < float(c["open"]) for c in ultimos)

    min_inclinacao = atr5 * M15_INCLINACAO_ATR_MIN
    if (
        ema9 > ema21
        and inclinacao9 >= min_inclinacao
        and inclinacao21 > 0
        and altas >= M15_IMPULSO_MIN_DIRECIONAIS
    ):
        return {
            "direcao": "CALL", "ema9": ema9, "ema21": ema21,
            "adx": adx15, "atr": atr5, "separacao": separacao,
            "impulso": altas,
        }

    if (
        ema9 < ema21
        and inclinacao9 <= -min_inclinacao
        and inclinacao21 < 0
        and baixas >= M15_IMPULSO_MIN_DIRECIONAIS
    ):
        return {
            "direcao": "PUT", "ema9": ema9, "ema21": ema21,
            "adx": adx15, "atr": atr5, "separacao": separacao,
            "impulso": baixas,
        }

    return None


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
    """Estratégia principal 5M + 5M + pullback + confirmação separada.

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
            "mensagem": "Poucas velas de 5M.",
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
            bloqueio = "5M em alta, mas 5M não confirma."
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
            bloqueio = "5M em baixa, mas 5M não confirma."
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
            "CALL FORTE | 5M ALTA + 5M ALTA | "
            "Pullback real | Confirmação em vela separada | "
            f"Score={score_call}/12 | RSI={rsi14:.2f}"
        )
    elif sinal == "PUT":
        mensagem = (
            "PUT FORTE | 5M BAIXA + 5M BAIXA | "
            "Pullback real | Confirmação em vela separada | "
            f"Score={score_put}/12 | RSI={rsi14:.2f}"
        )
    elif bloqueio:
        mensagem = f"AGUARDAR | {bloqueio}"
    else:
        mensagem = (
            f"AGUARDAR | 5M={tendencia_5m} | 5M={tendencia_15m} | "
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
# ESTRATÉGIA ÚNICA - S/R M5 + REVERSÃO NA MESMA VELA M1
# ============================================================

def _bloqueio_loss_restante(symbol):
    agora = agora_brt()
    with _bloqueio_loss_lock:
        ate = _bloqueio_loss_ate.get(symbol)
        if ate is None:
            return 0.0
        restante = (ate - agora).total_seconds()
        if restante <= 0:
            _bloqueio_loss_ate.pop(symbol, None)
            return 0.0
        return restante


def _aplicar_bloqueio_loss(symbol):
    ate = agora_brt() + timedelta(minutes=BLOQUEIO_LOSS_MINUTOS)
    with _bloqueio_loss_lock:
        _bloqueio_loss_ate[symbol] = ate
    log(
        f"[COOLDOWN] {symbol} bloqueado por {BLOQUEIO_LOSS_MINUTOS} min "
        f"após LOSS, até {ate.strftime('%H:%M:%S BRT')}."
    )
    return ate


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


def _atr_cache_1m(active_id):
    candles = _candles_cache(active_id, 60)
    fechadas = somente_velas_fechadas(candles, 1)
    if len(fechadas) < 15:
        return None
    return atr(fechadas, 14)


def _atr_cache_5m(active_id):
    candles = _candles_cache(active_id, 300)
    fechadas = somente_velas_fechadas(candles, 5)
    if len(fechadas) < 15:
        return None
    return atr(fechadas, 14)


def _pivos_m5(candles):
    """Retorna pivôs de suporte e resistência usando apenas candles M5 fechados."""
    infos = [candle_info(c) for c in candles]
    suportes = []
    resistencias = []
    w = SR_M5_PIVOT_JANELA

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
    sup_pivos, res_pivos = _pivos_m5(fechadas)

    suportes = [
        g for g in _agrupar_niveis(sup_pivos, tolerancia)
        if g["toques"] >= SR_M5_MIN_TOQUES
    ]
    resistencias = [
        g for g in _agrupar_niveis(res_pivos, tolerancia)
        if g["toques"] >= SR_M5_MIN_TOQUES
    ]

    return suportes, resistencias, atr5


def _nivel_mais_proximo(niveis, preco, lado):
    """Escolhe o nível M5 relevante mais próximo do preço atual."""
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



def _log_fim_diagnostico(active_id, symbol, status, **dados):
    """Log compacto para entender por que a FIM entrou ou bloqueou."""
    partes = [f"[FIM-M5][DIAG] {symbol}", status]
    for chave, valor in dados.items():
        if isinstance(valor, float):
            partes.append(f"{chave}={valor:.3f}")
        else:
            partes.append(f"{chave}={valor}")
    log(" | ".join(partes))


def _resultado_retracao_intravela(msg, active_id):
    """FIM EXPERIMENTAL — Fluxo, Impulso e Momento.

    Estratégia autoral para M5:
    1. Fluxo M5: direção + força (ADX).
    2. Estrutura M5: EMA9/EMA21 e inclinação.
    3. Impulso: corpos e fechamentos recentes.
    4. Retração: procura perda de força contra o fluxo.
    5. Rejeição: pavios/posição do fechamento.
    6. Volatilidade: ATR evita mercado morto e vela esticada.
    7. Timing: só entra no começo da nova M5.
    8. Score: exige várias evidências simultâneas.
    """
    if not isinstance(msg, dict):
        return None

    # Resolve o nome do ativo sem depender de ACTIVE_ID_TO_SYMBOL,
    # que não existe nesta base do robô.
    symbol = str(active_id)
    try:
        aid = int(active_id)

        # Procura primeiro nos cadastros/dicionários já existentes.
        for _nome_mapa in (
            "ATIVOS_OTC",
            "ATIVOS",
            "OTC_ATIVOS",
            "BULLEX_ATIVOS",
            "ACTIVE_IDS",
        ):
            _mapa = globals().get(_nome_mapa)
            if not isinstance(_mapa, dict):
                continue

            # Formato active_id -> nome
            if aid in _mapa:
                _valor = _mapa[aid]
                if isinstance(_valor, dict):
                    symbol = str(
                        _valor.get("symbol")
                        or _valor.get("ticker")
                        or _valor.get("codigo")
                        or _valor.get("name")
                        or aid
                    )
                else:
                    symbol = str(_valor)
                break

            # Formato nome -> active_id / dados
            for _chave, _valor in _mapa.items():
                if isinstance(_valor, dict):
                    _id = (
                        _valor.get("active_id")
                        or _valor.get("id")
                        or _valor.get("activeId")
                    )
                    try:
                        if _id is not None and int(_id) == aid:
                            symbol = str(
                                _valor.get("symbol")
                                or _valor.get("ticker")
                                or _valor.get("codigo")
                                or _valor.get("name")
                                or _chave
                            )
                            break
                    except (TypeError, ValueError):
                        pass
                else:
                    try:
                        if int(_valor) == aid:
                            symbol = str(_chave)
                            break
                    except (TypeError, ValueError):
                        pass

            if symbol != str(active_id):
                break
    except (TypeError, ValueError):
        pass

    try:
        abertura = float(msg["open"])
        fechamento = float(msg["close"])
        maxima = float(msg.get("max", msg.get("high")))
        minima = float(msg.get("min", msg.get("low")))
        candle_from = int(float(msg["from"]))
        candle_to = int(float(msg.get("to") or (candle_from + 60)))
    except Exception:
        return None

    server_ts, _ = _horario_servidor_atual()
    decorridos = max(0.0, server_ts - candle_from)
    restantes = max(0.0, candle_to - server_ts)

    _log_fim_diagnostico(
        active_id,
        symbol,
        "M5_RECEBIDA",
        atraso_s=decorridos,
        restantes_s=restantes,
    )

    # Entrada imediata: a análise principal já vem das velas fechadas.
    # Só aceita a oportunidade até 1 segundo após abrir a nova M5.
    if decorridos < 0:
        _log_fim_diagnostico(active_id, symbol, "BLOQUEADA_RELOGIO", atraso_s=decorridos)
        return None
    if decorridos > 1.0 or restantes < 298:
        _log_fim_diagnostico(
            active_id,
            symbol,
            "BLOQUEADA_ATRASO",
            atraso_s=decorridos,
            limite_s=1.0,
        )
        return None

    contexto_m15 = _contexto_forca_m15(active_id)
    if not contexto_m15:
        _log_fim_diagnostico(active_id, symbol, "BLOQUEADA_SEM_CONTEXTO_M55")
        return None

    direcao = contexto_m15.get("direcao")
    adx15 = float(contexto_m15.get("adx") or 0.0)
    if direcao not in ("CALL", "PUT"):
        _log_fim_diagnostico(active_id, symbol, "BLOQUEADA_SEM_DIRECAO_M55", adx=adx15)
        return None
    if adx15 < 20:
        _log_fim_diagnostico(
            active_id, symbol, "BLOQUEADA_ADX_BAIXO",
            direcao=direcao, adx=adx15, minimo=20
        )
        return None

    candles = somente_velas_fechadas(_candles_cache(active_id, 300), 1)
    if len(candles) < 40:
        _log_fim_diagnostico(
            active_id, symbol, "BLOQUEADA_POUCAS_VELAS",
            candles=len(candles), minimo=40
        )
        return None
    candles = candles[-70:]

    infos = [candle_info(c) for c in candles]
    valores = [float(c["close"]) for c in candles]
    ema9s = ema_series(valores, 9)
    ema21s = ema_series(valores, 21)
    ema9 = ema9s[-1]
    ema21 = ema21s[-1]
    atr1 = atr(candles, 14)

    if None in (ema9, ema21) or not atr1 or atr1 <= 0:
        _log_fim_diagnostico(active_id, symbol, "BLOQUEADA_INDICADORES_INVALIDOS")
        return None

    ultimas = infos[-5:]
    ultima = ultimas[-1]
    score = 0
    motivos = []

    # --------------------------------------------------------
    # 1. FLUXO M5
    # --------------------------------------------------------
    if adx15 >= 28:
        score += 3
        motivos.append("M5 muito forte")
    elif adx15 >= 23:
        score += 2
        motivos.append("M5 forte")
    else:
        score += 1
        motivos.append("M5 válido")

    # --------------------------------------------------------
    # 2. ESTRUTURA M5 — EMA + inclinação
    # --------------------------------------------------------
    sep = abs(ema9 - ema21)
    if sep < atr1 * 0.06:
        _log_fim_diagnostico(
            active_id, symbol, "BLOQUEADA_COMPRESSAO",
            separacao=sep, atr=atr1
        )
        return None  # mercado excessivamente comprimido/lateral

    if direcao == "CALL":
        if ema9 >= ema21:
            score += 2
            motivos.append("EMA M5 alta")
        else:
            score -= 2
        inclinacao = ema9s[-1] - ema9s[-4]
        if inclinacao > 0:
            score += 1
            motivos.append("EMA9 inclinada")
    else:
        if ema9 <= ema21:
            score += 2
            motivos.append("EMA M5 baixa")
        else:
            score -= 2
        inclinacao = ema9s[-1] - ema9s[-4]
        if inclinacao < 0:
            score += 1
            motivos.append("EMA9 inclinada")

    # --------------------------------------------------------
    # 3. IMPULSO RECENTE
    # --------------------------------------------------------
    favor = 0
    contra = 0
    for inf in ultimas[-4:]:
        if inf["close"] > inf["open"]:
            if direcao == "CALL":
                favor += 1
            else:
                contra += 1
        elif inf["close"] < inf["open"]:
            if direcao == "PUT":
                favor += 1
            else:
                contra += 1

    if favor >= 2:
        score += 1
        motivos.append("impulso presente")

    # --------------------------------------------------------
    # 4. RETRAÇÃO / PERDA DE FORÇA
    # Aceita 1-2 velas contra o fluxo, desde que não sejam violentas.
    # --------------------------------------------------------
    retracao = False
    duas = ultimas[-2:]
    if direcao == "CALL":
        retracao = any(i["close"] < i["open"] for i in duas)
    else:
        retracao = any(i["close"] > i["open"] for i in duas)

    if retracao:
        amplitude_retracao = max(i["range"] for i in duas)
        if amplitude_retracao <= atr1 * 1.25:
            score += 2
            motivos.append("retração controlada")
        else:
            score -= 2

    # --------------------------------------------------------
    # 5. REJEIÇÃO NA ÚLTIMA VELA FECHADA
    # --------------------------------------------------------
    amp_u = max(ultima["range"], 1e-12)
    if direcao == "CALL":
        pavio_rejeicao = max(ultima["open"] - ultima["low"], 0.0) / amp_u
        fechamento_pos = (ultima["close"] - ultima["low"]) / amp_u
        if pavio_rejeicao >= 0.22 or fechamento_pos >= 0.65:
            score += 2
            motivos.append("rejeição inferior")
    else:
        pavio_rejeicao = max(ultima["high"] - ultima["open"], 0.0) / amp_u
        fechamento_pos = (ultima["high"] - ultima["close"]) / amp_u
        if pavio_rejeicao >= 0.22 or fechamento_pos >= 0.65:
            score += 2
            motivos.append("rejeição superior")

    # --------------------------------------------------------
    # 6. NÃO PERSEGUE MOVIMENTO ESTICADO
    # --------------------------------------------------------
    distancia_ema = abs(ultima["close"] - ema9)
    if distancia_ema > atr1 * 1.15:
        _log_fim_diagnostico(
            active_id, symbol, "BLOQUEADA_ESTICADA_EMA",
            distancia_ema=distancia_ema, atr=atr1
        )
        return None

    if ultima["range"] > atr1 * 1.65:
        _log_fim_diagnostico(
            active_id, symbol, "BLOQUEADA_VELA_ANOMALA",
            range_ultima=ultima["range"], atr=atr1
        )
        return None

    # --------------------------------------------------------
    # 7. TIMING IMEDIATO
    # --------------------------------------------------------
    # Para entrar em até 1 segundo, não aguardamos a formação do corpo
    # da nova vela. A decisão é baseada nas velas M5 já fechadas + M15.
    motivos.append("entrada imediata <=1s")

    # --------------------------------------------------------
    # 8. SCORE FINAL
    # --------------------------------------------------------
    # Exige múltiplas evidências; não existe entrada por um único indicador.
    SCORE_MINIMO_FIM = 6
    if score < SCORE_MINIMO_FIM:
        _log_fim_diagnostico(
            active_id, symbol, "BLOQUEADA_SCORE",
            direcao=direcao,
            score=score,
            minimo=SCORE_MINIMO_FIM,
            adx=adx15,
            motivos=";".join(motivos),
        )
        return None

    qualidade = "FORTE" if score >= 10 else "NORMAL"

    _log_fim_diagnostico(
        active_id,
        symbol,
        "SINAL_APROVADO",
        direcao=direcao,
        score=score,
        qualidade=qualidade,
        atraso_s=decorridos,
        adx=adx15,
    )

    return {
        "sinal": direcao,
        "score": score,
        "score_call": score if direcao == "CALL" else 1,
        "score_put": score if direcao == "PUT" else 1,
        "preco": fechamento,
        "vela": datetime.fromtimestamp(candle_from, TZ),
        "estrategia": "FIM_M5_FLUXO_IMPULSO_MOMENTO",
        "regime": qualidade,
        "pullback": "RETRAÇÃO CONTROLADA" if retracao else "IMPULSO DIRETO",
        "rejeicao": " + ".join(motivos[-3:]),
        "lateral": "NAO",
        "atr": atr1,
        "rsi": None,
        "ema5": None,
        "ema13": ema9,
        "ema21": ema21,
        "tendencia_5m": (
            f"{'ALTA' if direcao == 'CALL' else 'BAIXA'} | ADX {adx15:.1f}"
        ),
        "tendencia_15m": "N/A",
        "zona_fibonacci": f"FIM SCORE {score}",
        "bloqueio": "SINAL",
        "mensagem": (
            f"{direcao} FIM {qualidade} | score={score} | "
            f"M5 ADX={adx15:.1f} | entrada={decorridos:.1f}s | "
            f"{', '.join(motivos)}"
        ),
        "candle_from": candle_from,
        "candle_to": candle_to,
        "segundos_decorridos": decorridos,
        "segundos_restantes": restantes,
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
        "rsi": "-",
        "ema5": "-",
        "ema13": "-",
        "ema21": "-",
        "tendencia_5m": resultado.get("tendencia_5m", "-"),
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
        "estrategia": "FIM_M5_FLUXO_IMPULSO_MOMENTO",
        "zona_fibonacci": "-",
    }


def _processar_sinal_intravela(active_id, msg):
    codigo, symbol = _symbol_por_active_id(active_id)
    if not codigo or not symbol:
        return

    if not dentro_do_horario():
        return

    restante_bloqueio = _bloqueio_loss_restante(symbol)
    if restante_bloqueio > 0:
        # LOSS recente: este ativo fica fora por 40 minutos; os demais seguem normais.
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

    log(
        f"[INTRAVELA] {symbol} -> {resultado['sinal']} | "
        f"score={resultado['score']} | "
        f"estrategia={resultado.get('estrategia')} | "
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
        if item.get("estrategia") != "FIM_M5_FLUXO_IMPULSO_MOMENTO":
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
        "FIM_M5_FLUXO_IMPULSO_MOMENTO": {
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
    total = len(_historico_resultados)

    wins = sum(1 for x in _historico_resultados if x.get("resultado") == "WIN")
    losses = sum(1 for x in _historico_resultados if x.get("resultado") == "LOSS")
    dojis = sum(1 for x in _historico_resultados if x.get("resultado") == "DOJI")

    decididos = wins + losses
    taxa = wins / decididos * 100 if decididos > 0 else 0.0

    # Soma o lucro/prejuízo já registrado em cada operação. Isso permite
    # combinar payouts diferentes e usar a liquidação REAL recebida da Bullex.
    lucro_total = 0.0
    for item in _historico_resultados:
        lucro_item = _float_seguro(item.get("lucro"))
        if lucro_item is not None:
            lucro_total += lucro_item
            continue

        # Compatibilidade com registros antigos que ainda não tenham "lucro".
        resultado = item.get("resultado")
        valor = float(item.get("valor") or 0.0)
        if resultado == "WIN":
            lucro_total += valor * (PAYOUT_LUCRO_PERCENTUAL / 100.0)
        elif resultado == "LOSS":
            lucro_total -= valor

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "dojis": dojis,
        "taxa": round(taxa, 2),
        "lucro_total": round(lucro_total, 2),
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
        f"{emoji} SINAL FIM M5\n\n"
        f"Ativo: {symbol}\n"
        f"Direcao: {sinal}\n"
        f"Score: {resultado.get('score', 0)}\n"
        f"Estrategia: {resultado.get('estrategia', '-')}\n"
        f"Regime: {resultado.get('regime', '-')}\n"
        f"Preco: {fmt(resultado.get('preco'))}\n"
        f"Vela analisada: "
        f"{vela.strftime('%Y-%m-%d %H:%M:%S BRT')}\n\n"
        f"Tendencia 15M: "
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
        f"➡️ ENTRADA: MESMA VELA M1\n"
        f"⏱️ EXPIRACAO: 1 MINUTO (fechamento da vela)\n\n"
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

def registrar_operacao_intravela(symbol, resultado):
    global _operacao_global_ativa

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
        "sinal": sinal,
        "score": resultado.get("score", 0),
        "estrategia": "FIM_M5_FLUXO_IMPULSO_MOMENTO",
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
        "nivel_m5": resultado.get("nivel_m5"),
        "tipo_nivel": resultado.get("tipo_nivel"),
        "toques_nivel": resultado.get("toques_nivel"),
        "distancia_abertura_nivel": resultado.get("distancia_abertura_nivel"),
    }

    _operacoes_pendentes[symbol] = operacao
    _ultimas_operacoes_registradas[symbol] = chave

    # A ordem já foi copiada para as operações pendentes.
    # Libera o slot temporário para permitir a segunda operação.
    with _execucao_lock:
        if (
            _operacao_global_ativa is not None
            and _operacao_global_ativa.get("symbol") == symbol
        ):
            _operacao_global_ativa = None

    log(
        f"[INTRAVELA] {symbol}: operação registrada {sinal} | "
        f"entrada={operacao['entrada']:.5f} | "
        f"expira={datetime.fromtimestamp(int(resultado['candle_to']), TZ).strftime('%H:%M:%S')}"
    )

    threading.Thread(
        target=enviar_sinal_telegram,
        args=(symbol, resultado),
        daemon=True,
        name=f"telegram-intravela-{symbol}-{candle_from}",
    ).start()




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

        if dt + timedelta(minutes=1) > agora:
            return

        info = candle_info(candle)
        entrada = float(operacao.get("entrada") or operacao.get("preco_sinal"))
        saida = info["close"]

        operacao["entrada"] = entrada
        operacao["saida"] = saida

        if operacao["sinal"] == "CALL":
            resultado_candle = "WIN" if saida > entrada else "LOSS" if saida < entrada else "DOJI"
        else:
            resultado_candle = "WIN" if saida < entrada else "LOSS" if saida > entrada else "DOJI"

        # Se a Bullex já informou o resultado oficial da liquidação, ele tem
        # prioridade. Caso contrário, mantém a classificação técnica pelo candle.
        resultado = operacao.get("resultado_bullex") or resultado_candle
        operacao["resultado"] = resultado
        operacao["resultado_candle"] = resultado_candle
        operacao["finalizado_em"] = agora

        valor_operacao = float(operacao.get("valor") or 0.0)
        lucro_real = _float_seguro(operacao.get("lucro_real"))
        if lucro_real is not None:
            operacao["lucro"] = round(lucro_real, 2)
            operacao["fonte_lucro"] = "BULLEX_REAL"
        elif resultado == "WIN":
            operacao["lucro"] = round(
                valor_operacao * (PAYOUT_LUCRO_PERCENTUAL / 100.0), 2
            )
            operacao["fonte_lucro"] = f"FALLBACK_{PAYOUT_LUCRO_PERCENTUAL:.2f}%"
        elif resultado == "LOSS":
            operacao["lucro"] = round(-valor_operacao, 2)
            operacao["fonte_lucro"] = "VALOR_ENTRADA"
        else:
            operacao["lucro"] = 0.0
            operacao["fonte_lucro"] = "DOJI"

        _historico_resultados.append(operacao.copy())
        del _operacoes_pendentes[symbol]

        with _execucao_lock:
            if (
                _operacao_global_ativa is not None
                and _operacao_global_ativa.get("symbol") == symbol
            ):
                _operacao_global_ativa = None

        if resultado == "LOSS":
            bloqueado_ate = _aplicar_bloqueio_loss(symbol)
            operacao["bloqueado_ate"] = bloqueado_ate.isoformat()

        _atualizar_progressao(resultado)
        _atualizar_estado_execucao()

        gerenciamento = _resumo_gerenciamento()
        if gerenciamento["status"] == "PARADO":
            log(
                f"[GERENCIAMENTO] OPERAÇÕES ENCERRADAS | "
                f"motivo={gerenciamento['motivo_parada']} | "
                f"lucro_dia=R${gerenciamento['lucro_dia']:.2f} | "
                f"pico=R${gerenciamento['pico_lucro_dia']:.2f}"
            )

        estatisticas = calcular_estatisticas()

        log(
            f"[RESULTADO INTRAVELA] {symbol} {operacao['sinal']} -> {resultado} | "
            f"entrada={entrada:.5f} | fechamento_mesma_vela={saida:.5f} | "
            f"taxa_total={estatisticas['taxa']:.2f}% | "
            f"lucro_total=R${estatisticas['lucro_total']:.2f}"
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
        f"Direcao: {operacao['sinal']}\n"
        f"Estrategia: {operacao.get('estrategia', '-')}\n"
        f"Regime: {operacao.get('regime', '-')}\n"
        f"Resultado: {resultado}\n"
        + (f"Bloqueio do ativo: {BLOQUEIO_LOSS_MINUTOS} minutos\n" if resultado == "LOSS" else "")
        + f"\nEntrada: {fmt(operacao.get('entrada'))}\n"
        f"Saida: {fmt(operacao.get('saida'))}\n"
        f"Fonte da vela: Bullex\n\n"
        f"📊 ESTATISTICAS\n"
        f"Operacoes: {estatisticas['total']}\n"
        f"Wins: {estatisticas['wins']}\n"
        f"Losses: {estatisticas['losses']}\n"
        f"Dojis: {estatisticas['dojis']}\n"
        f"Taxa: {estatisticas['taxa']:.2f}%\n"
        f"Resultado financeiro: R${float(operacao.get('lucro') or 0.0):.2f}\n"
        f"Fonte do lucro: {operacao.get('fonte_lucro', '-')}\n"
        f"Lucro acumulado: R${estatisticas['lucro_total']:.2f}"
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
            candles_1m = obter_candles(
                symbol,
                TIMEFRAME,
                OUTPUTSIZE
            )
            avaliar_operacao(
                symbol,
                candles_1m
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
    """O ciclo M1 de manutenção não cria sinais por polling.

    Ele mantém histórico atualizado, dashboard e finaliza operações.
    Os sinais surgem exclusivamente do candle-generated M1 em tempo real.
    """
    with _bullex_assets_lock:
        config = ATIVO_BULLEX.get(chave)

    if not config:
        return None

    try:
        candles_1m = obter_candles(symbol, TIMEFRAME, OUTPUTSIZE)
        # Mantém histórico M5 carregado para o filtro de tendência/força.
        obter_candles(symbol, TIMEFRAME_TREND, OUTPUTSIZE_5M)
        avaliar_operacao(symbol, candles_1m)

        ultimo, idade = idade_do_ultimo_candle(candles_1m)
        if ultimo is not None:
            estado["ativo"] = symbol
            estado["preco"] = f"{float(ultimo['close']):.5f}"
            estado["atualizado"] = agora_brt().strftime("%H:%M:%S BRT")
            estado["atualidade_min"] = f"{idade:.1f} min" if idade is not None else "-"
            if estado.get("sinal") not in ("CALL", "PUT"):
                estado["sinal"] = "AGUARDAR"
                estado["mensagem"] = "Monitorando FIM M5: decisão pelas velas M5 fechadas + contexto M15, entrada até 1s da nova M5 e máximo 2 operações simultâneas."
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
            "[OTC AUTO] Leitura adiada: active_id dos pares ainda não está pronto. "
            f"Detalhe: {erro_ativos}"
        )
        estado["sinal"] = "AGUARDAR"
        estado["score"] = 0
        estado["mensagem"] = (
            "Aguardando carregamento dos pares de OTC na Bullex."
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

    # No ciclo M1, sinais NÃO são gerados aqui por polling.
    # A retração é detectada em tempo real no candle-generated.
    finalizar_operacoes_vencidas_antes_da_leitura()

    for chave, symbol in ATIVOS.items():
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
    """Sincroniza a manutenção do robô com cada nova vela M1.

    O gatilho intravela continua vindo em tempo real pelo candle-generated.
    Esta rotina apenas garante que histórico, resultados pendentes, dashboard
    e descoberta/manutenção dos ativos sejam atualizados a cada minuto.
    """
    agora = agora_brt()

    # Próximo fechamento/abertura de vela M1. Pequena folga de 100 ms
    # evita consultar exatamente antes da virada do minuto.
    proxima = (
        agora + timedelta(minutes=1)
    ).replace(
        second=0,
        microsecond=100000,
    )

    segundos = max(
        (proxima - agora).total_seconds(),
        0.2,
    )

    log(
        "[R30][M5] Proxima leitura M1: "
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
    log(
        f"[R30][M5] Scheduler ativo: leitura/manutencao a cada 1 minuto | "
        f"expiracao={EXPIRACAO_MINUTOS} minuto(s) | cooldown_loss={BLOQUEIO_LOSS_MINUTOS} min"
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
            log(f"[OTC AUTO] Pronto para leitura: {ativos_prontos}")
        else:
            log(
                "[OTC AUTO] Inicialização ainda incompleta; "
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
<span>Nível M5</span>
<span class="valor">MESMA VELA M1</span>
</div>

<div class="linha">
<span>Tendência 15M</span>
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

<div class="box">
LUCRO TOTAL
<div class="numero">
R$ {{ '%.2f'|format(estado.estatisticas.lucro_total) }}
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
Entrada: reversão intravela M1 em tempo real
</strong>

<br>

<strong>
Expiração: 1 minuto
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
                "Retracao intravela na mesma vela de 1 minuto"
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
        "entradas": VALORES_ENTRADA,
        "gerenciamento": _resumo_gerenciamento(),
        "gestao_6_9_ativa": True,
        "mercado": "OTC",
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

log(f"AUTO TRADE DEMO={'ATIVO' if BULLEX_AUTO_TRADE else 'DESATIVADO'} | gestao=R$6 -> WIN -> R$9 -> volta R$6 | LOSS -> R$6")
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