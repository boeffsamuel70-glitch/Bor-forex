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
TIMEZONE = "America/Sao_Paulo"
TZ = ZoneInfo(TIMEZONE)

OUTPUTSIZE = 100
HORA_INICIO = 6
HORA_FIM = 22

# Twelve Data pode atrasar alguns minutos.
# Se ultrapassar este limite, o robô não gera sinal.
MAX_ATRASO_MINUTOS = 8

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
        "pullback": "-",
        "rejeicao": "-",
        "tendencia": "-",
        "atr": "-",
    },
}

_robo_lock = threading.Lock()
_robo_started = False

# Guarda a última vela que já gerou Telegram para cada ativo.
# Assim o mesmo sinal não é enviado várias vezes.
_ultimos_sinais_telegram = {}


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


def somente_velas_fechadas(candles):
    candles = ordenar_candles(candles)
    agora = agora_brt()

    return [
        candle
        for candle in candles
        if candle["_dt"] + timedelta(minutes=5) <= agora
    ]


def idade_do_ultimo_candle(candles):
    ordenadas = ordenar_candles(candles)

    if not ordenadas:
        return None, None

    ultimo = ordenadas[-1]
    idade = (agora_brt() - ultimo["_dt"]).total_seconds() / 60

    return ultimo, idade


# ============================================================
# TWELVE DATA
# ============================================================

def obter_candles(symbol):
    if not API_KEY:
        raise RuntimeError(
            "TWELVE_DATA_API_KEY nao configurada no Render."
        )

    resposta = requests.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": symbol,
            "interval": TIMEFRAME,
            "outputsize": OUTPUTSIZE,
            "timezone": TIMEZONE,
            "apikey": API_KEY,
        },
        timeout=20,
    )

    resposta.raise_for_status()
    dados = resposta.json()

    if dados.get("status") == "error":
        raise RuntimeError(
            dados.get("message", "Erro retornado pela Twelve Data.")
        )

    values = dados.get("values")

    if not values:
        raise RuntimeError(
            f"Nenhuma vela recebida para {symbol}: {dados}"
        )

    candles = ordenar_candles(values)

    if not candles:
        raise RuntimeError(
            f"Nao foi possivel interpretar as datas de {symbol}."
        )

    return candles


# ============================================================
# INDICADORES
# ============================================================

def closes(candles):
    return [float(c["close"]) for c in candles]


def ema(values, period):
    if len(values) < period:
        return None

    k = 2 / (period + 1)
    valor = sum(values[:period]) / period

    for preco in values[period:]:
        valor = (preco * k) + (valor * (1 - k))

    return valor


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    ganhos = []
    perdas = []

    for i in range(1, len(values)):
        diferenca = values[i] - values[i - 1]
        ganhos.append(max(diferenca, 0))
        perdas.append(max(-diferenca, 0))

    avg_gain = sum(ganhos[:period]) / period
    avg_loss = sum(perdas[:period]) / period

    for i in range(period, len(ganhos)):
        avg_gain = ((avg_gain * (period - 1)) + ganhos[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + perdas[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        atual = candles[i]
        anterior = candles[i - 1]

        high = float(atual["high"])
        low = float(atual["low"])
        close_anterior = float(anterior["close"])

        trs.append(
            max(
                high - low,
                abs(high - close_anterior),
                abs(low - close_anterior),
            )
        )

    if len(trs) < period:
        return None

    return sum(trs[-period:]) / period


# ============================================================
# ESTRATÉGIA PULLBACK
# ============================================================

def candle_info(candle):
    abertura = float(candle["open"])
    fechamento = float(candle["close"])
    maxima = float(candle["high"])
    minima = float(candle["low"])

    range_vela = max(maxima - minima, 1e-10)
    corpo = abs(fechamento - abertura)

    pavio_superior = maxima - max(abertura, fechamento)
    pavio_inferior = min(abertura, fechamento) - minima

    forca_corpo = corpo / range_vela

    return {
        "open": abertura,
        "close": fechamento,
        "high": maxima,
        "low": minima,
        "range": range_vela,
        "body": corpo,
        "upper_wick": max(pavio_superior, 0),
        "lower_wick": max(pavio_inferior, 0),
        "body_ratio": forca_corpo,
    }


def percentual_distancia(preco, referencia):
    if referencia == 0:
        return 999.0
    return abs(preco - referencia) / abs(referencia)


def analisar_pullback(candles):
    """
    Estratégia:
    1) Identifica tendência pelas EMAs 5/13/21.
    2) Procura pullback recente na EMA 13 ou EMA 21.
    3) Exige vela de rejeição/retomada na direção da tendência.
    4) Filtra RSI extremo.
    5) Filtra ATR muito baixo.
    6) Só libera quando há confluência suficiente.

    A vela analisada é a última vela FECHADA.
    A entrada pretendida é na próxima vela de 5 minutos.
    """

    if len(candles) < 40:
        return {
            "sinal": "AGUARDAR",
            "score": 0,
            "preco": float(candles[-1]["close"]) if candles else 0,
            "vela": candles[-1]["_dt"] if candles else None,
            "mensagem": "Poucas velas para análise do pullback.",
            "score_call": 0,
            "score_put": 0,
        }

    c = closes(candles)

    preco = c[-1]

    ema5 = ema(c, 5)
    ema13 = ema(c, 13)
    ema21 = ema(c, 21)
    rsi14 = rsi(c, 14)
    atr14 = atr(candles, 14)

    ultima = candle_info(candles[-1])
    anterior = candle_info(candles[-2])
    terceira = candle_info(candles[-3])

    # --------------------------------------------------------
    # TENDÊNCIA
    # --------------------------------------------------------

    tendencia = "NEUTRA"

    if ema5 and ema13 and ema21:
        if ema5 > ema13 > ema21:
            tendencia = "ALTA"
        elif ema5 < ema13 < ema21:
            tendencia = "BAIXA"

    # --------------------------------------------------------
    # PULLBACK
    # --------------------------------------------------------

    # Tolerância proporcional para considerar que o preço
    # tocou/chegou perto da EMA 13 ou EMA 21.
    tolerancia_ema = 0.0012  # 0,12%

    pullback_call = False
    pullback_put = False

    # Procuramos o toque nas últimas 3 velas, sem exigir
    # que a vela de sinal seja exatamente o candle do toque.
    ultimas = candles[-3:]

    if ema13 and ema21:
        for candle in ultimas:
            info = candle_info(candle)

            perto_ema13 = (
                percentual_distancia(info["low"], ema13) <= tolerancia_ema
                or (
                    info["low"] <= ema13 <= info["high"]
                )
            )

            perto_ema21 = (
                percentual_distancia(info["low"], ema21) <= tolerancia_ema
                or (
                    info["low"] <= ema21 <= info["high"]
                )
            )

            if perto_ema13 or perto_ema21:
                pullback_call = True

            perto_ema13_put = (
                percentual_distancia(info["high"], ema13) <= tolerancia_ema
                or (
                    info["low"] <= ema13 <= info["high"]
                )
            )

            perto_ema21_put = (
                percentual_distancia(info["high"], ema21) <= tolerancia_ema
                or (
                    info["low"] <= ema21 <= info["high"]
                )
            )

            if perto_ema13_put or perto_ema21_put:
                pullback_put = True

    # --------------------------------------------------------
    # REJEIÇÃO / RETOMADA
    # --------------------------------------------------------

    rejeicao_call = False
    rejeicao_put = False

    # CALL:
    # vela fecha acima da abertura + corpo razoável +
    # rejeição inferior ou fechamento forte.
    if ultima["close"] > ultima["open"]:
        rejeicao_inferior = (
            ultima["lower_wick"] >= ultima["body"] * 0.40
            and ultima["lower_wick"] > ultima["upper_wick"]
        )

        fechamento_forte = (
            ultima["body_ratio"] >= 0.45
            and (
                (ultima["high"] - ultima["close"]) / ultima["range"]
            ) <= 0.30
        )

        retomada_com_anterior = (
            ultima["close"] > anterior["high"]
        )

        if rejeicao_inferior or fechamento_forte or retomada_com_anterior:
            rejeicao_call = True

    # PUT:
    # vela fecha abaixo da abertura + corpo razoável +
    # rejeição superior ou fechamento forte.
    if ultima["close"] < ultima["open"]:
        rejeicao_superior = (
            ultima["upper_wick"] >= ultima["body"] * 0.40
            and ultima["upper_wick"] > ultima["lower_wick"]
        )

        fechamento_forte_put = (
            ultima["body_ratio"] >= 0.45
            and (
                (ultima["close"] - ultima["low"]) / ultima["range"]
            ) <= 0.30
        )

        retomada_com_anterior_put = (
            ultima["close"] < anterior["low"]
        )

        if (
            rejeicao_superior
            or fechamento_forte_put
            or retomada_com_anterior_put
        ):
            rejeicao_put = True

    # --------------------------------------------------------
    # MOVIMENTO / CONTEXTO
    # --------------------------------------------------------

    movimento_4 = c[-1] - c[-4]
    movimento_8 = c[-1] - c[-8]

    contexto_call = movimento_4 > 0 and movimento_8 > 0
    contexto_put = movimento_4 < 0 and movimento_8 < 0

    # Evita considerar uma vela completamente fora do padrão
    # como pullback válido se o corpo for muito pequeno.
    corpo_ok = ultima["body_ratio"] >= 0.25

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    rsi_call_ok = rsi14 is not None and 52 <= rsi14 <= 68
    rsi_put_ok = rsi14 is not None and 32 <= rsi14 <= 48

    # RSI muito extremo bloqueia o sinal.
    rsi_extremo = (
        rsi14 is not None
        and (rsi14 >= 72 or rsi14 <= 28)
    )

    # --------------------------------------------------------
    # ATR
    # --------------------------------------------------------

    atr_ok = True

    if atr14 is not None and preco != 0:
        atr_ratio = atr14 / preco

        # Mercado muito parado: não operar.
        if atr_ratio < 0.00008:
            atr_ok = False

        # Mercado extremamente explosivo: evitar perseguir preço.
        if atr_ratio > 0.0035:
            atr_ok = False

    # --------------------------------------------------------
    # SCORE DE CONFLUÊNCIA
    # --------------------------------------------------------

    score_call = 0
    score_put = 0

    if tendencia == "ALTA":
        score_call += 3

    if tendencia == "BAIXA":
        score_put += 3

    if pullback_call:
        score_call += 2

    if pullback_put:
        score_put += 2

    if rejeicao_call:
        score_call += 2

    if rejeicao_put:
        score_put += 2

    if rsi_call_ok:
        score_call += 1

    if rsi_put_ok:
        score_put += 1

    if contexto_call:
        score_call += 1

    if contexto_put:
        score_put += 1

    if corpo_ok:
        if ultima["close"] > ultima["open"]:
            score_call += 1
        elif ultima["close"] < ultima["open"]:
            score_put += 1

    # --------------------------------------------------------
    # REGRAS FINAIS
    # --------------------------------------------------------

    sinal = "AGUARDAR"
    score = max(score_call, score_put)

    bloqueio = None

    if not atr_ok:
        bloqueio = "ATR fora da faixa ideal."

    elif rsi_extremo:
        bloqueio = f"RSI extremo ({rsi14:.2f})."

    elif tendencia == "ALTA":
        # Exige tendência + pullback + confirmação da vela.
        if (
            score_call >= 7
            and pullback_call
            and rejeicao_call
            and rsi_call_ok
            and contexto_call
        ):
            sinal = "CALL"

    elif tendencia == "BAIXA":
        if (
            score_put >= 7
            and pullback_put
            and rejeicao_put
            and rsi_put_ok
            and contexto_put
        ):
            sinal = "PUT"

    if bloqueio:
        sinal = "AGUARDAR"

    detalhes_pullback = (
        "CONFIRMADO"
        if (pullback_call and tendencia == "ALTA")
        or (pullback_put and tendencia == "BAIXA")
        else "NAO"
    )

    detalhes_rejeicao = (
        "CONFIRMADA"
        if (rejeicao_call and tendencia == "ALTA")
        or (rejeicao_put and tendencia == "BAIXA")
        else "NAO"
    )

    if sinal == "CALL":
        mensagem = (
            "PULLBACK DE ALTA CONFIRMADO | "
            "Tendencia ALTA | "
            "Pullback EMA13/EMA21 | "
            "Rejeicao de alta | "
            f"RSI={rsi14:.2f}"
        )
    elif sinal == "PUT":
        mensagem = (
            "PULLBACK DE BAIXA CONFIRMADO | "
            "Tendencia BAIXA | "
            "Pullback EMA13/EMA21 | "
            "Rejeicao de baixa | "
            f"RSI={rsi14:.2f}"
        )
    elif bloqueio:
        mensagem = f"AGUARDAR | {bloqueio}"
    else:
        mensagem = (
            f"AGUARDAR | Tendencia={tendencia} | "
            f"Pullback={detalhes_pullback} | "
            f"Rejeicao={detalhes_rejeicao} | "
            f"CALL={score_call} | PUT={score_put}"
        )

    return {
        "sinal": sinal,
        "score": score,
        "preco": preco,
        "vela": candles[-1]["_dt"],
        "rsi": rsi14,
        "ema5": ema5,
        "ema13": ema13,
        "ema21": ema21,
        "atr": atr14,
        "score_call": score_call,
        "score_put": score_put,
        "pullback": detalhes_pullback,
        "rejeicao": detalhes_rejeicao,
        "tendencia": tendencia,
        "mensagem": mensagem,
    }


# ============================================================
# TELEGRAM
# ============================================================

def telegram_configurado():
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def enviar_telegram(texto):
    if not telegram_configurado():
        log("Telegram nao configurado. Sinal nao enviado.")
        return False

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
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

        log("Telegram: mensagem enviada com sucesso.")
        return True

    except Exception as e:
        log(f"ERRO ao enviar Telegram: {e}")
        return False


def enviar_sinal_telegram(symbol, resultado):
    sinal = resultado.get("sinal")

    if sinal not in ("CALL", "PUT"):
        return

    vela = resultado.get("vela")

    if not isinstance(vela, datetime):
        return

    chave_duplicata = f"{symbol}|{vela.isoformat()}|{sinal}"

    if _ultimos_sinais_telegram.get(symbol) == chave_duplicata:
        log(
            f"{symbol}: sinal duplicado ignorado "
            f"({sinal} / {vela.strftime('%H:%M')})."
        )
        return

    _ultimos_sinais_telegram[symbol] = chave_duplicata

    emoji = "🟢" if sinal == "CALL" else "🔴"

    rsi = resultado.get("rsi")
    ema5 = resultado.get("ema5")
    ema13 = resultado.get("ema13")
    ema21 = resultado.get("ema21")
    atr14 = resultado.get("atr")

    def fmt(valor, casas=5):
        if isinstance(valor, (int, float)):
            return f"{valor:.{casas}f}"
        return "-"

    texto = (
        f"{emoji} SINAL FOREX 5M\n\n"
        f"Ativo: {symbol}\n"
        f"Direcao: {sinal}\n"
        f"Score: {resultado.get('score', 0)}\n"
        f"Preco: {fmt(resultado.get('preco'))}\n"
        f"Vela analisada: "
        f"{vela.strftime('%Y-%m-%d %H:%M:%S BRT')}\n\n"
        f"Tendencia: {resultado.get('tendencia', '-')}\n"
        f"Pullback: {resultado.get('pullback', '-')}\n"
        f"Rejeicao: {resultado.get('rejeicao', '-')}\n"
        f"RSI 14: {fmt(rsi, 2)}\n"
        f"EMA 5: {fmt(ema5)}\n"
        f"EMA 13: {fmt(ema13)}\n"
        f"EMA 21: {fmt(ema21)}\n"
        f"ATR 14: {fmt(atr14, 6)}\n\n"
        f"➡️ ENTRADA: PROXIMA VELA\n"
        f"⏱️ EXPIRACAO: 5 MINUTOS\n\n"
        f"⚠️ Sinal tecnico experimental. "
        f"Nao ha garantia de resultado."
    )

    enviar_telegram(texto)


# ============================================================
# PROCESSAMENTO
# ============================================================

def processar_ativo(chave, symbol):
    try:
        log(f"Consultando Twelve Data: {symbol}")

        candles = obter_candles(symbol)

        log(f"{symbol}: {len(candles)} candles recebidos.")

        ultimo_raw, idade = idade_do_ultimo_candle(candles)

        if ultimo_raw is None:
            raise RuntimeError(
                "Nao foi possivel identificar o ultimo candle."
            )

        log(
            f"{symbol} | ultimo candle="
            f"{ultimo_raw['_dt'].strftime('%Y-%m-%d %H:%M:%S')} BRT | "
            f"atraso={idade:.2f} min"
        )

        if idade > MAX_ATRASO_MINUTOS:
            resultado = {
                "sinal": "AGUARDAR",
                "score": 0,
                "preco": float(ultimo_raw["close"]),
                "vela": ultimo_raw["_dt"],
                "mensagem": (
                    f"Dado atrasado ({idade:.1f} min). "
                    "Aguardando atualizacao."
                ),
                "score_call": 0,
                "score_put": 0,
            }

        else:
            fechadas = somente_velas_fechadas(candles)

            if len(fechadas) < 40:
                resultado = {
                    "sinal": "AGUARDAR",
                    "score": 0,
                    "preco": float(ultimo_raw["close"]),
                    "vela": ultimo_raw["_dt"],
                    "mensagem": "Velas fechadas insuficientes.",
                    "score_call": 0,
                    "score_put": 0,
                }
            else:
                resultado = analisar_pullback(fechadas)

        estado["ativo"] = symbol
        estado["sinal"] = resultado["sinal"]
        estado["score"] = resultado["score"]

        preco = resultado.get("preco")
        estado["preco"] = (
            f"{preco:.5f}"
            if isinstance(preco, (float, int))
            else (preco or "-")
        )

        vela = resultado.get("vela")

        estado["vela"] = (
            vela.strftime("%Y-%m-%d %H:%M:%S BRT")
            if isinstance(vela, datetime)
            else "-"
        )

        estado["atualizado"] = agora_brt().strftime("%H:%M:%S BRT")
        estado["atualidade_min"] = f"{idade:.1f} min"
        estado["mensagem"] = resultado.get("mensagem", "")

        estado["detalhes"] = {
            "score_call": resultado.get("score_call", "-"),
            "score_put": resultado.get("score_put", "-"),
            "rsi": (
                f"{resultado['rsi']:.2f}"
                if isinstance(resultado.get("rsi"), (float, int))
                else "-"
            ),
            "ema5": (
                f"{resultado['ema5']:.5f}"
                if isinstance(resultado.get("ema5"), (float, int))
                else "-"
            ),
            "ema13": (
                f"{resultado['ema13']:.5f}"
                if isinstance(resultado.get("ema13"), (float, int))
                else "-"
            ),
            "ema21": (
                f"{resultado['ema21']:.5f}"
                if isinstance(resultado.get("ema21"), (float, int))
                else "-"
            ),
            "pullback": resultado.get("pullback", "-"),
            "rejeicao": resultado.get("rejeicao", "-"),
            "tendencia": resultado.get("tendencia", "-"),
            "atr": (
                f"{resultado['atr']:.6f}"
                if isinstance(resultado.get("atr"), (float, int))
                else "-"
            ),
        }

        log(
            f"{symbol} -> {resultado['sinal']} | "
            f"score={resultado['score']} | "
            f"CALL={resultado.get('score_call', 0)} | "
            f"PUT={resultado.get('score_put', 0)} | "
            f"tendencia={resultado.get('tendencia', '-')} | "
            f"pullback={resultado.get('pullback', '-')} | "
            f"rejeicao={resultado.get('rejeicao', '-')} | "
            f"preco={estado['preco']} | "
            f"vela={estado['vela']} | "
            f"dado={idade:.1f} min"
        )

        if resultado["sinal"] in ("CALL", "PUT"):
            enviar_sinal_telegram(symbol, resultado)

        return resultado

    except Exception as e:
        log(f"ERRO em {symbol}: {e}")

        estado["ativo"] = symbol
        estado["sinal"] = "AGUARDAR"
        estado["score"] = 0
        estado["preco"] = "-"
        estado["vela"] = "-"
        estado["atualizado"] = agora_brt().strftime("%H:%M:%S BRT")
        estado["atualidade_min"] = "-"
        estado["mensagem"] = f"Erro: {e}"

        return None


# ============================================================
# HORÁRIO / LOOP
# ============================================================

def dentro_do_horario():
    hora = agora_brt().hour
    return HORA_INICIO <= hora < HORA_FIM


def executar_leitura():
    log("Iniciando leitura dos ativos.")

    if not API_KEY:
        log("ERRO: TWELVE_DATA_API_KEY nao configurada.")
        estado["sinal"] = "AGUARDAR"
        estado["mensagem"] = (
            "Configure TWELVE_DATA_API_KEY no Render."
        )
        return

    if not dentro_do_horario():
        agora = agora_brt()

        log(
            f"Fora do horario configurado "
            f"({HORA_INICIO:02d}:00 as {HORA_FIM:02d}:00 BRT)."
        )

        estado["sinal"] = "AGUARDAR"
        estado["mensagem"] = "Fora do horario configurado."
        estado["atualizado"] = agora.strftime("%H:%M:%S BRT")
        return

    for chave, symbol in ATIVOS.items():
        processar_ativo(chave, symbol)

    log(
        "Leitura concluida as "
        f"{agora_brt().strftime('%Y-%m-%d %H:%M:%S BRT')}."
    )


def esperar_ate_proxima_leitura():
    agora = agora_brt()

    proximo_bloco = ((agora.minute // 5) + 1) * 5

    if proximo_bloco >= 60:
        proxima = (agora + timedelta(hours=1)).replace(
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

    segundos = max((proxima - agora).total_seconds(), 1)

    log(
        f"Proxima leitura: "
        f"{proxima.strftime('%H:%M:%S BRT')}."
    )

    time.sleep(segundos)


def loop_robo():
    log("Loop do robo iniciado.")
    log("Executando primeira leitura imediatamente.")

    executar_leitura()

    while True:
        try:
            esperar_ate_proxima_leitura()
            executar_leitura()

        except Exception as e:
            log(f"Erro no loop principal: {e}")
            time.sleep(10)


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

        log("Thread do robo iniciada no worker web.")


@app.before_request
def iniciar_robo():
    garantir_robo_iniciado()


# ============================================================
# INTERFACE
# ============================================================

HTML = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>Robo Forex Pullback 5M</title>

<style>
body {
    font-family: Arial, sans-serif;
    background: #111;
    color: #fff;
    margin: 0;
    padding: 20px;
}

.container {
    max-width: 700px;
    margin: auto;
}

h1 {
    text-align: center;
    margin-bottom: 20px;
}

.subtitulo {
    text-align: center;
    color: #aaa;
    margin-top: -10px;
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

.observacao {
    text-align: center;
    color: #bbb;
    font-size: 14px;
    line-height: 1.5;
}

.atualizacao {
    text-align: center;
    color: #aaa;
    font-size: 13px;
    margin-top: 15px;
}
</style>
</head>

<body>

<div class="container">

<h1>Robo Forex Pullback 5M</h1>

<div class="subtitulo">
Tendência + Pullback + Rejeição + RSI + ATR
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

<div class="linha">
<span>Tendência</span>
<span class="valor">
{{ estado.detalhes.tendencia }}
</span>
</div>

<div class="linha">
<span>Pullback</span>
<span class="valor">
{{ estado.detalhes.pullback }}
</span>
</div>

<div class="linha">
<span>Rejeição</span>
<span class="valor">
{{ estado.detalhes.rejeicao }}
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

<div class="observacao">

{{ estado.mensagem }}

<br><br>

Quando houver CALL/PUT:
<br>
<strong>Entrada: próxima vela de 5 minutos</strong>
<br>
<strong>Expiração: 5 minutos</strong>

<br><br>

O sinal é uma análise técnica experimental
e não garante resultado.

<br>
Use primeiro em conta demo e faça backtest.

</div>

</div>

<div class="atualizacao">
A página atualiza automaticamente a cada 10 segundos.
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
    return render_template_string(HTML, estado=estado)


@app.route("/dados")
def dados():
    garantir_robo_iniciado()
    return jsonify(estado)


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "bot_iniciado": _robo_started,
        "horario_brt": agora_brt().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "estrategia": "pullback",
        "telegram_configurado": telegram_configurado(),
    })


if __name__ == "__main__":
    garantir_robo_iniciado()

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        debug=False,
    )
