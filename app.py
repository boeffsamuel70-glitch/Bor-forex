import os
import time
import math
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, render_template_string

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None

app = Flask(__name__)

# ============================================================
# CONFIGURACAO MT5 - DEMO SOMENTE
# ============================================================
TIMEZONE = "America/Sao_Paulo"
TZ = ZoneInfo(TIMEZONE)

MT5_PATH = os.getenv("MT5_PATH", "").strip()
MT5_LOGIN = os.getenv("MT5_LOGIN", "").strip()
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "").strip()
MT5_SERVER = os.getenv("MT5_SERVER", "").strip()

# Protecao: esta versao foi feita para conta DEMO.
# Para liberar execucao, o nome do servidor precisa parecer DEMO.
MT5_DEMO_ONLY = True
MT5_AUTO_TRADE = os.getenv("MT5_AUTO_TRADE", "true").strip().lower() in (
    "1", "true", "yes", "sim", "on"
)

MT5_LOT = float(os.getenv("MT5_LOT", "0.01"))
MT5_SL_PIPS = float(os.getenv("MT5_SL_PIPS", "10"))
MT5_TP_PIPS = float(os.getenv("MT5_TP_PIPS", "15"))
MT5_DEVIATION = int(os.getenv("MT5_DEVIATION", "20"))
MT5_MAGIC = int(os.getenv("MT5_MAGIC", "260909"))
MT5_COMMENT = os.getenv("MT5_COMMENT", "ROBO_SR_M5_DEMO")[:31]

# Se sua corretora usa sufixos (ex.: EURUSD.a), altere por variavel ambiente:
# MT5_SYMBOLS=EURUSD.a,EURJPY.a,GBPUSD.a,USDJPY.a,GBPJPY.a,EURGBP.a,USDCHF.a
MT5_SYMBOLS = [
    x.strip() for x in os.getenv(
        "MT5_SYMBOLS",
        "EURUSD,EURJPY,GBPUSD,USDJPY,GBPJPY,EURGBP,USDCHF"
    ).split(",") if x.strip()
]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ============================================================
# ESTRATEGIA ATUAL - S/R M5 + RETRACAO NA MESMA VELA
# ============================================================
SR_M5_LOOKBACK = int(os.getenv("SR_M5_LOOKBACK", "100"))
SR_M5_PIVOT_JANELA = int(os.getenv("SR_M5_PIVOT_JANELA", "2"))
SR_M5_MIN_TOQUES = int(os.getenv("SR_M5_MIN_TOQUES", "3"))
SR_M5_TOLERANCIA_ATR = float(os.getenv("SR_M5_TOLERANCIA_ATR", "0.16"))
SR_M5_DISTANCIA_ABERTURA_ATR_MIN = float(
    os.getenv("SR_M5_DISTANCIA_ABERTURA_ATR_MIN", "0.55")
)
INTRAVELA_RETRACAO_MIN = float(os.getenv("INTRAVELA_RETRACAO_MIN", "0.20"))
INTRAVELA_RETRACAO_MAX = float(os.getenv("INTRAVELA_RETRACAO_MAX", "0.68"))
INTRAVELA_REJEICAO_ATR_MIN = float(
    os.getenv("INTRAVELA_REJEICAO_ATR_MIN", "0.10")
)
INTRAVELA_PAVIO_MIN_FRACAO_MOVIMENTO = float(
    os.getenv("INTRAVELA_PAVIO_MIN_FRACAO_MOVIMENTO", "0.10")
)
INTRAVELA_MIN_SEGUNDOS_DECORRIDOS = int(
    os.getenv("INTRAVELA_MIN_SEGUNDOS_DECORRIDOS", "20")
)
INTRAVELA_MIN_SEGUNDOS_RESTANTES = int(
    os.getenv("INTRAVELA_MIN_SEGUNDOS_RESTANTES", "35")
)

# Uma posicao por ativo. False permite ativos diferentes ao mesmo tempo.
UMA_OPERACAO_GLOBAL = os.getenv("UMA_OPERACAO_GLOBAL", "false").strip().lower() in (
    "1", "true", "yes", "sim", "on"
)

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.0"))
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "7"))

# ============================================================
# ESTADO
# ============================================================
_lock = threading.RLock()
_started = False
_mt5_ready = False
_mt5_last_error = None
_intravela_estado = {}
_velas_tentadas = set()
_ordens_robo = {}  # position/ticket -> metadata
_historico = []
_processados_history = set()

estado = {
    "status": "iniciando",
    "mt5_conectado": False,
    "conta": "-",
    "servidor": "-",
    "modo": "DEMO PROTEGIDO",
    "ativo": "-",
    "sinal": "AGUARDAR",
    "score": 0,
    "preco": "-",
    "mensagem": "Aguardando MT5.",
    "atualizado": "-",
    "detalhes": {},
}


def agora_brt():
    return datetime.now(TZ)


def log(msg):
    print(f"[{agora_brt().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def telegram_configurado():
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def enviar_telegram(texto):
    if not telegram_configurado():
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": texto},
            timeout=8,
        )
    except Exception as e:
        log(f"Telegram: {e}")


# ============================================================
# MT5
# ============================================================
def _servidor_parece_demo(server):
    s = (server or "").lower()
    return any(x in s for x in ("demo", "trial", "practice"))


def conectar_mt5():
    global _mt5_ready, _mt5_last_error

    if mt5 is None:
        _mt5_last_error = (
            "Pacote MetaTrader5 nao instalado. Rode: pip install MetaTrader5"
        )
        return False

    kwargs = {}
    if MT5_LOGIN:
        try:
            kwargs["login"] = int(MT5_LOGIN)
        except ValueError:
            _mt5_last_error = "MT5_LOGIN invalido."
            return False
    if MT5_PASSWORD:
        kwargs["password"] = MT5_PASSWORD
    if MT5_SERVER:
        kwargs["server"] = MT5_SERVER

    try:
        if MT5_PATH:
            ok = mt5.initialize(MT5_PATH, **kwargs)
        elif kwargs:
            ok = mt5.initialize(**kwargs)
        else:
            # Usa o terminal MT5 que ja esta aberto/logado no computador.
            ok = mt5.initialize()
    except Exception as e:
        _mt5_last_error = str(e)
        return False

    if not ok:
        _mt5_last_error = f"initialize falhou: {mt5.last_error()}"
        _mt5_ready = False
        return False

    info = mt5.account_info()
    if info is None:
        _mt5_last_error = f"account_info falhou: {mt5.last_error()}"
        _mt5_ready = False
        return False

    server = str(getattr(info, "server", "") or "")
    trade_mode = int(getattr(info, "trade_mode", -1))
    # No MT5, trade_mode=0 corresponde a conta DEMO. Esta versao bloqueia
    # qualquer outro tipo de conta, mesmo que o nome do servidor seja ambiguo.
    if MT5_DEMO_ONLY and trade_mode != 0:
        _mt5_last_error = (
            f"BLOQUEADO: a conta conectada nao e DEMO (trade_mode={trade_mode}, servidor='{server}'). "
            "Esta versao nao envia ordens em conta real."
        )
        _mt5_ready = False
        estado.update({
            "status": "bloqueado",
            "mt5_conectado": True,
            "conta": str(getattr(info, "login", "-")),
            "servidor": server,
            "mensagem": _mt5_last_error,
        })
        return False

    _mt5_ready = True
    _mt5_last_error = None
    estado.update({
        "status": "ok",
        "mt5_conectado": True,
        "conta": str(getattr(info, "login", "-")),
        "servidor": server,
        "mensagem": "MT5 DEMO conectado. Monitorando mercado.",
        "atualizado": agora_brt().strftime("%H:%M:%S BRT"),
    })

    for symbol in MT5_SYMBOLS:
        try:
            mt5.symbol_select(symbol, True)
        except Exception:
            pass

    return True


def garantir_mt5():
    if _mt5_ready:
        try:
            if mt5.terminal_info() is not None and mt5.account_info() is not None:
                return True
        except Exception:
            pass
    return conectar_mt5()


def _pip_size(symbol_info):
    # Forex com 5/3 digitos: 1 pip = 10 points. Com 4/2: 1 pip = 1 point.
    point = float(symbol_info.point)
    digits = int(symbol_info.digits)
    return point * 10.0 if digits in (3, 5) else point


def _normalizar_volume(info, volume):
    vmin = float(getattr(info, "volume_min", 0.01) or 0.01)
    vmax = float(getattr(info, "volume_max", volume) or volume)
    step = float(getattr(info, "volume_step", 0.01) or 0.01)
    v = min(max(float(volume), vmin), vmax)
    n = round((v - vmin) / step)
    v = vmin + n * step
    # quantidade de casas do step
    casas = max(0, -int(math.floor(math.log10(step)))) if step < 1 else 0
    return round(v, min(casas + 1, 8))


def _filling_mode(info):
    # ORDER_FILLING_IOC costuma funcionar em Forex; se o broker exigir outro,
    # tentamos RETURN como fallback em enviar_ordem_mt5().
    return mt5.ORDER_FILLING_IOC


def posicoes_robo(symbol=None):
    if not garantir_mt5():
        return []
    pos = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
    if not pos:
        return []
    return [p for p in pos if int(getattr(p, "magic", 0)) == MT5_MAGIC]


def pode_abrir(symbol):
    if posicoes_robo(symbol):
        return False, "Ja existe posicao do robo neste ativo."
    if UMA_OPERACAO_GLOBAL and posicoes_robo():
        return False, "Ja existe uma posicao global do robo."
    return True, "OK"


def enviar_ordem_mt5(symbol, sinal, resultado):
    if not MT5_AUTO_TRADE:
        return {"ok": False, "motivo": "MT5_AUTO_TRADE desativado"}
    if not garantir_mt5():
        return {"ok": False, "motivo": _mt5_last_error or "MT5 desconectado"}

    permitido, motivo = pode_abrir(symbol)
    if not permitido:
        return {"ok": False, "motivo": motivo}

    info = mt5.symbol_info(symbol)
    if info is None:
        return {"ok": False, "motivo": f"Simbolo {symbol} nao encontrado no MT5"}
    if not info.visible and not mt5.symbol_select(symbol, True):
        return {"ok": False, "motivo": f"Nao consegui habilitar {symbol} no Market Watch"}

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return {"ok": False, "motivo": f"Sem tick de {symbol}"}

    is_buy = sinal == "CALL"
    order_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
    price = float(tick.ask if is_buy else tick.bid)
    pip = _pip_size(info)
    digits = int(info.digits)
    sl_dist = MT5_SL_PIPS * pip
    tp_dist = MT5_TP_PIPS * pip

    sl = price - sl_dist if is_buy else price + sl_dist
    tp = price + tp_dist if is_buy else price - tp_dist
    sl = round(sl, digits)
    tp = round(tp, digits)
    price = round(price, digits)
    volume = _normalizar_volume(info, MT5_LOT)

    base = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "sl": sl,
        "tp": tp,
        "deviation": MT5_DEVIATION,
        "magic": MT5_MAGIC,
        "comment": MT5_COMMENT,
        "type_time": mt5.ORDER_TIME_GTC,
    }

    result = None
    erros = []
    for filling in (mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN):
        request = dict(base)
        request["type_filling"] = filling
        try:
            result = mt5.order_send(request)
        except Exception as e:
            erros.append(str(e))
            continue
        if result is None:
            erros.append(str(mt5.last_error()))
            continue
        if result.retcode == mt5.TRADE_RETCODE_DONE:
            break
        erros.append(f"retcode={result.retcode} comment={getattr(result, 'comment', '')}")

    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        return {
            "ok": False,
            "motivo": "; ".join(erros[-3:]) or "order_send recusado",
        }

    ticket = int(getattr(result, "order", 0) or getattr(result, "deal", 0) or 0)
    meta = {
        "ticket": ticket,
        "symbol": symbol,
        "lado": "BUY" if is_buy else "SELL",
        "sinal": sinal,
        "volume": volume,
        "entrada": price,
        "sl": sl,
        "tp": tp,
        "score": resultado.get("score", 0),
        "nivel_sr": resultado.get("nivel_sr"),
        "toques_nivel": resultado.get("toques_nivel"),
        "aberto_em": datetime.now().isoformat(),
    }
    with _lock:
        _ordens_robo[ticket] = meta

    log(
        f"[MT5 DEMO] {symbol} {meta['lado']} | lote={volume} | "
        f"entrada={price} | SL={sl} | TP={tp} | ticket={ticket}"
    )
    threading.Thread(
        target=enviar_telegram,
        args=((
            f"MT5 DEMO - ORDEM ABERTA\n"
            f"{symbol} {meta['lado']}\n"
            f"Lote: {volume}\nEntrada: {price}\nSL: {sl}\nTP: {tp}\n"
            f"Score: {meta['score']}"
        ),),
        daemon=True,
    ).start()

    return {"ok": True, **meta}


# ============================================================
# CANDLES / INDICADORES
# ============================================================
def obter_rates(symbol, timeframe, count, incluir_atual=True):
    if not garantir_mt5():
        return []
    start = 0 if incluir_atual else 1
    rates = mt5.copy_rates_from_pos(symbol, timeframe, start, count)
    if rates is None:
        return []
    out = []
    for r in rates:
        out.append({
            "time": int(r["time"]),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
            "tick_volume": int(r["tick_volume"]),
        })
    return out


def atr(candles, periodo=14):
    if len(candles) < periodo + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < periodo:
        return None
    return sum(trs[-periodo:]) / periodo


def _pivos_sr(candles, janela):
    suportes, resistencias = [], []
    for i in range(janela, len(candles) - janela):
        lows = [candles[j]["low"] for j in range(i - janela, i + janela + 1)]
        highs = [candles[j]["high"] for j in range(i - janela, i + janela + 1)]
        if candles[i]["low"] <= min(lows):
            suportes.append(candles[i]["low"])
        if candles[i]["high"] >= max(highs):
            resistencias.append(candles[i]["high"])
    return suportes, resistencias


def _agrupar_niveis(valores, tolerancia):
    grupos = []
    for valor in sorted(float(x) for x in valores):
        achou = None
        for g in grupos:
            if abs(valor - g["nivel"]) <= tolerancia:
                achou = g
                break
        if achou is None:
            grupos.append({"nivel": valor, "toques": 1, "valores": [valor]})
        else:
            achou["valores"].append(valor)
            achou["toques"] += 1
            achou["nivel"] = sum(achou["valores"]) / len(achou["valores"])
    return grupos


def niveis_sr_m5(symbol):
    fechadas = obter_rates(symbol, mt5.TIMEFRAME_M5, SR_M5_LOOKBACK + 20, incluir_atual=False)
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


def _nivel_mais_proximo(niveis, preco, lado):
    if not niveis:
        return None
    if lado == "SUPORTE":
        candidatos = [g for g in niveis if g["nivel"] <= preco] or niveis
    else:
        candidatos = [g for g in niveis if g["nivel"] >= preco] or niveis
    return min(candidatos, key=lambda g: abs(preco - g["nivel"]))


def analisar_intravela(symbol):
    rates = obter_rates(symbol, mt5.TIMEFRAME_M5, 2, incluir_atual=True)
    if not rates:
        return None
    atual = rates[-1]
    abertura = atual["open"]
    fechamento = atual["close"]
    maxima = atual["high"]
    minima = atual["low"]
    candle_from = int(atual["time"])
    candle_to = candle_from + 300
    agora = int(time.time())
    decorridos = max(0, agora - candle_from)
    restantes = max(0, candle_to - agora)

    if decorridos < INTRAVELA_MIN_SEGUNDOS_DECORRIDOS:
        return None
    if restantes < INTRAVELA_MIN_SEGUNDOS_RESTANTES:
        return None

    suportes, resistencias, atr5 = niveis_sr_m5(symbol)
    if not atr5:
        return None

    key = (symbol, candle_from)
    with _lock:
        st = _intravela_estado.setdefault(key, {"ultimo_close": fechamento, "eventos": 0})
        ultimo_close = float(st["ultimo_close"])
        st["eventos"] += 1
        st["ultimo_close"] = fechamento
        eventos = st["eventos"]

    if eventos < 2:
        return None

    tolerancia = atr5 * SR_M5_TOLERANCIA_ATR
    dist_min = atr5 * SR_M5_DISTANCIA_ABERTURA_ATR_MIN

    suporte = _nivel_mais_proximo(suportes, minima, "SUPORTE")
    if suporte is not None:
        nivel = float(suporte["nivel"])
        dist_abertura = abertura - nivel
        tocou = minima <= nivel + tolerancia
        movimento = max(0.0, abertura - minima)
        rejeicao = fechamento - minima
        ratio = rejeicao / max(movimento, 1e-12)
        pavio = min(abertura, fechamento) - minima
        confirmou = (
            tocou and dist_abertura >= dist_min and fechamento > ultimo_close
            and INTRAVELA_RETRACAO_MIN <= ratio <= INTRAVELA_RETRACAO_MAX
            and rejeicao >= atr5 * INTRAVELA_REJEICAO_ATR_MIN
            and pavio >= max(movimento * INTRAVELA_PAVIO_MIN_FRACAO_MOVIMENTO, atr5 * 0.04)
        )
        if confirmou:
            score = 10 + (1 if suporte["toques"] >= 4 else 0) + (1 if dist_abertura >= atr5 * 0.90 else 0)
            return {
                "sinal": "CALL", "score": score, "preco": fechamento,
                "atr": atr5, "nivel_sr": nivel, "tipo_nivel": "SUPORTE_M5",
                "toques_nivel": suporte["toques"], "retracao_ratio": ratio,
                "candle_from": candle_from, "candle_to": candle_to,
                "mensagem": f"CALL | suporte M5={nivel:.5f} | toques={suporte['toques']} | retracao={ratio*100:.1f}%",
            }

    resistencia = _nivel_mais_proximo(resistencias, maxima, "RESISTENCIA")
    if resistencia is not None:
        nivel = float(resistencia["nivel"])
        dist_abertura = nivel - abertura
        tocou = maxima >= nivel - tolerancia
        movimento = max(0.0, maxima - abertura)
        rejeicao = maxima - fechamento
        ratio = rejeicao / max(movimento, 1e-12)
        pavio = maxima - max(abertura, fechamento)
        confirmou = (
            tocou and dist_abertura >= dist_min and fechamento < ultimo_close
            and INTRAVELA_RETRACAO_MIN <= ratio <= INTRAVELA_RETRACAO_MAX
            and rejeicao >= atr5 * INTRAVELA_REJEICAO_ATR_MIN
            and pavio >= max(movimento * INTRAVELA_PAVIO_MIN_FRACAO_MOVIMENTO, atr5 * 0.04)
        )
        if confirmou:
            score = 10 + (1 if resistencia["toques"] >= 4 else 0) + (1 if dist_abertura >= atr5 * 0.90 else 0)
            return {
                "sinal": "PUT", "score": score, "preco": fechamento,
                "atr": atr5, "nivel_sr": nivel, "tipo_nivel": "RESISTENCIA_M5",
                "toques_nivel": resistencia["toques"], "retracao_ratio": ratio,
                "candle_from": candle_from, "candle_to": candle_to,
                "mensagem": f"PUT | resistencia M5={nivel:.5f} | toques={resistencia['toques']} | retracao={ratio*100:.1f}%",
            }
    return None


# ============================================================
# RESULTADOS / ESTATISTICAS
# ============================================================
def atualizar_historico_mt5():
    if not garantir_mt5():
        return
    inicio = datetime.now() - timedelta(days=HISTORY_DAYS)
    fim = datetime.now() + timedelta(minutes=1)
    deals = mt5.history_deals_get(inicio, fim)
    if not deals:
        return

    # Considera deals de saida do nosso magic. Agrupa lucro por position_id.
    por_pos = {}
    for d in deals:
        if int(getattr(d, "magic", 0)) != MT5_MAGIC:
            continue
        pid = int(getattr(d, "position_id", 0) or 0)
        if not pid:
            continue
        rec = por_pos.setdefault(pid, {"profit": 0.0, "symbol": getattr(d, "symbol", "-"), "time": 0})
        rec["profit"] += float(getattr(d, "profit", 0) or 0)
        rec["profit"] += float(getattr(d, "swap", 0) or 0)
        rec["profit"] += float(getattr(d, "commission", 0) or 0)
        rec["time"] = max(rec["time"], int(getattr(d, "time", 0) or 0))

    abertas = {int(p.ticket) for p in posicoes_robo()}
    for pid, rec in por_pos.items():
        if pid in abertas or pid in _processados_history:
            continue
        # Evita registrar position antiga que nao foi aberta nesta sessao se nao houver metadata.
        meta = _ordens_robo.get(pid)
        if meta is None:
            continue
        lucro = round(rec["profit"], 2)
        resultado = "WIN" if lucro > 0 else ("LOSS" if lucro < 0 else "ZERO")
        item = {**meta, "position_id": pid, "lucro": lucro, "resultado": resultado,
                "fechado_em": datetime.fromtimestamp(rec["time"]).isoformat() if rec["time"] else datetime.now().isoformat()}
        with _lock:
            _historico.append(item)
            _processados_history.add(pid)
        threading.Thread(
            target=enviar_telegram,
            args=((f"MT5 DEMO - {resultado}\n{rec['symbol']}\nResultado: {lucro:.2f}"),),
            daemon=True,
        ).start()


def estatisticas_por_ativo():
    base = {s: {"wins": 0, "losses": 0, "zero": 0, "total": 0, "lucro": 0.0} for s in MT5_SYMBOLS}
    with _lock:
        hist = list(_historico)
    for x in hist:
        s = x.get("symbol")
        if s not in base:
            base[s] = {"wins": 0, "losses": 0, "zero": 0, "total": 0, "lucro": 0.0}
        base[s]["total"] += 1
        base[s]["lucro"] += float(x.get("lucro", 0) or 0)
        if x.get("resultado") == "WIN": base[s]["wins"] += 1
        elif x.get("resultado") == "LOSS": base[s]["losses"] += 1
        else: base[s]["zero"] += 1
    for s in base:
        d = base[s]
        dec = d["wins"] + d["losses"]
        d["taxa"] = round(d["wins"] / dec * 100, 2) if dec else 0.0
        d["lucro"] = round(d["lucro"], 2)
    return base


def estatisticas():
    p = estatisticas_por_ativo()
    wins = sum(x["wins"] for x in p.values())
    losses = sum(x["losses"] for x in p.values())
    zero = sum(x["zero"] for x in p.values())
    lucro = round(sum(x["lucro"] for x in p.values()), 2)
    dec = wins + losses
    return {"wins": wins, "losses": losses, "zero": zero, "total": wins + losses + zero,
            "taxa": round(wins / dec * 100, 2) if dec else 0.0, "lucro": lucro}


# ============================================================
# LOOP PRINCIPAL
# ============================================================
def atualizar_dashboard(symbol, r, ordem=None):
    lado = "BUY" if r["sinal"] == "CALL" else "SELL"
    estado.update({
        "ativo": symbol,
        "sinal": lado,
        "score": r["score"],
        "preco": f"{r['preco']:.5f}",
        "mensagem": r["mensagem"],
        "atualizado": agora_brt().strftime("%H:%M:%S BRT"),
        "detalhes": {
            "estrategia": "SR M5 + retracao na mesma vela",
            "nivel": f"{r['tipo_nivel']} {r['nivel_sr']:.5f}",
            "toques": r["toques_nivel"],
            "retracao": f"{r['retracao_ratio']*100:.1f}%",
            "lote": ordem.get("volume") if ordem and ordem.get("ok") else MT5_LOT,
            "sl_pips": MT5_SL_PIPS,
            "tp_pips": MT5_TP_PIPS,
            "ordem": "CONFIRMADA" if ordem and ordem.get("ok") else (ordem or {}).get("motivo", "SEM ORDEM"),
        }
    })


def loop_robo():
    log("Iniciando robo MT5 DEMO - SR M5 intravela")
    while True:
        try:
            if not garantir_mt5():
                estado.update({
                    "status": "erro",
                    "mt5_conectado": False,
                    "mensagem": _mt5_last_error or "Falha MT5",
                    "atualizado": agora_brt().strftime("%H:%M:%S BRT"),
                })
                time.sleep(5)
                continue

            for symbol in MT5_SYMBOLS:
                try:
                    r = analisar_intravela(symbol)
                    if not r:
                        continue
                    candle_key = (symbol, int(r["candle_from"]))
                    with _lock:
                        if candle_key in _velas_tentadas:
                            continue
                        _velas_tentadas.add(candle_key)

                    log(f"[SINAL] {symbol} {r['sinal']} score={r['score']} | {r['mensagem']}")
                    ordem = enviar_ordem_mt5(symbol, r["sinal"], r)
                    atualizar_dashboard(symbol, r, ordem)
                    if not ordem.get("ok"):
                        log(f"[ORDEM NAO ENVIADA] {symbol}: {ordem.get('motivo')}")
                except Exception as e:
                    log(f"Erro {symbol}: {e}")

            atualizar_historico_mt5()
            # limpeza de memoria de velas antigas
            cutoff = int(time.time()) - 3600
            with _lock:
                antigos = [k for k in _intravela_estado if k[1] < cutoff]
                for k in antigos:
                    _intravela_estado.pop(k, None)
                _velas_tentadas.intersection_update({k for k in _velas_tentadas if k[1] >= cutoff})
        except Exception as e:
            log(f"Erro loop: {e}")
        time.sleep(max(POLL_SECONDS, 0.25))


def garantir_robo_iniciado():
    global _started
    with _lock:
        if _started:
            return
        _started = True
        threading.Thread(target=loop_robo, daemon=True, name="mt5-bot").start()


# ============================================================
# DASHBOARD
# ============================================================
HTML = r"""
<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Robô Forex MT5 DEMO</title>
<style>
body{font-family:Arial,sans-serif;background:#0f172a;color:#e5e7eb;margin:0;padding:20px}.wrap{max-width:1200px;margin:auto}
h1{margin:0 0 6px}.sub{color:#94a3b8;margin-bottom:20px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}.card{background:#1e293b;border:1px solid #334155;border-radius:14px;padding:16px}.big{font-size:28px;font-weight:700}.ok{color:#4ade80}.bad{color:#f87171}.warn{color:#facc15}table{width:100%;border-collapse:collapse;margin-top:14px;background:#1e293b;border-radius:12px;overflow:hidden}th,td{padding:10px;border-bottom:1px solid #334155;text-align:center}th{color:#94a3b8}.buy{color:#4ade80;font-weight:700}.sell{color:#f87171;font-weight:700}.muted{color:#94a3b8;font-size:13px}
</style></head>
<body><div class="wrap">
<h1>Robô Forex — MT5 DEMO</h1><div class="sub">S/R M5 + retração na mesma vela | BUY/SELL com lote, Stop Loss e Take Profit</div>
<div class="grid">
<div class="card"><div class="muted">MT5</div><div id="conn" class="big">-</div><div id="conta" class="muted"></div></div>
<div class="card"><div class="muted">Último sinal</div><div id="sinal" class="big">AGUARDAR</div><div id="ativo" class="muted">-</div></div>
<div class="card"><div class="muted">WIN / LOSS</div><div class="big"><span id="wins" class="ok">0</span> / <span id="losses" class="bad">0</span></div><div id="taxa" class="muted">0%</div></div>
<div class="card"><div class="muted">Lucro demo</div><div id="lucro" class="big">0.00</div><div class="muted">resultado fechado no MT5</div></div>
</div>
<div class="card" style="margin-top:12px"><div id="msg">Aguardando...</div><div id="det" class="muted" style="margin-top:8px"></div></div>
<table><thead><tr><th>Ativo</th><th>WIN</th><th>LOSS</th><th>Total</th><th>Taxa</th><th>Lucro</th></tr></thead><tbody id="tbody"></tbody></table>
</div>
<script>
async function upd(){try{let r=await fetch('/dados');let d=await r.json();document.getElementById('conn').textContent=d.estado.mt5_conectado?'CONECTADO':'DESCONECTADO';document.getElementById('conn').className='big '+(d.estado.mt5_conectado?'ok':'bad');document.getElementById('conta').textContent='Conta '+d.estado.conta+' | '+d.estado.servidor;let s=d.estado.sinal||'AGUARDAR';document.getElementById('sinal').textContent=s;document.getElementById('sinal').className='big '+(s==='BUY'?'buy':s==='SELL'?'sell':'warn');document.getElementById('ativo').textContent=d.estado.ativo+' | '+d.estado.preco;document.getElementById('wins').textContent=d.estatisticas.wins;document.getElementById('losses').textContent=d.estatisticas.losses;document.getElementById('taxa').textContent=d.estatisticas.taxa.toFixed(2)+'% | '+d.estatisticas.total+' operações';document.getElementById('lucro').textContent=d.estatisticas.lucro.toFixed(2);document.getElementById('msg').textContent=d.estado.mensagem;let x=d.estado.detalhes||{};document.getElementById('det').textContent=Object.entries(x).map(([k,v])=>k+': '+v).join(' | ');let h='';Object.entries(d.por_ativo).forEach(([k,v])=>{h+=`<tr><td>${k}</td><td class="ok">${v.wins}</td><td class="bad">${v.losses}</td><td>${v.total}</td><td>${v.taxa.toFixed(2)}%</td><td>${v.lucro.toFixed(2)}</td></tr>`});document.getElementById('tbody').innerHTML=h}catch(e){}}
upd();setInterval(upd,2000);
</script></body></html>
"""


@app.route("/")
def index():
    garantir_robo_iniciado()
    return render_template_string(HTML)


@app.route("/dados")
def dados():
    garantir_robo_iniciado()
    return jsonify({"estado": estado, "estatisticas": estatisticas(), "por_ativo": estatisticas_por_ativo(), "historico": _historico[-30:]})


@app.route("/health")
def health():
    garantir_robo_iniciado()
    return jsonify({
        "status": estado.get("status"),
        "mt5_conectado": estado.get("mt5_conectado"),
        "conta": estado.get("conta"),
        "servidor": estado.get("servidor"),
        "demo_only": MT5_DEMO_ONLY,
        "auto_trade": MT5_AUTO_TRADE,
        "lote": MT5_LOT,
        "sl_pips": MT5_SL_PIPS,
        "tp_pips": MT5_TP_PIPS,
        "symbols": MT5_SYMBOLS,
        "ultimo_erro": _mt5_last_error,
    })


if __name__ == "__main__":
    garantir_robo_iniciado()
    app.run(host="127.0.0.1", port=int(os.getenv("PORT", "5000")), debug=False)
