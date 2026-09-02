import os
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ============================================================
# CONFIGURAÇÕES
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

MAX_ATRASO_MINUTOS = 8

ATIVOS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "EURJPY": "EUR/JPY",
}


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
    },
}


# ============================================================
# CONTROLES DE THREAD
# ============================================================

_robo_lock = threading.Lock()
_robo_started = False


# ============================================================
# CONTROLE DE SINAIS DUPLICADOS
# ============================================================

_telegram_lock = threading.Lock()

# Guarda o último sinal enviado para cada ativo.
# Exemplo:
# {
#     "EUR/USD": ("2026-09-02 10:35:00", "CALL")
# }
_ultimo_sinal_enviado = {}


# ============================================================
# LOG
# ============================================================

def log(msg):
    agora = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[BOT] {msg}", flush=True)


# ============================================================
# DATA / HORA
# ============================================================

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


# ============================================================
# ORGANIZAÇÃO DAS VELAS
# ============================================================

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

    idade = (
        agora_brt() - ultimo["_dt"]
    ).total_seconds() / 60

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
            dados.get(
                "message",
                "Erro retornado pela Twelve Data."
            )
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
    return [
        float(c["close"])
        for c in candles
    ]


def ema(values, period):
    if len(values) < period:
        return None

    k = 2 / (period + 1)

    valor = sum(
        values[:period]
    ) / period

    for preco in values[period:]:
        valor = (
            preco * k
        ) + (
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
                avg_gain * (period - 1)
            )
            + ganhos[i]
        ) / period

        avg_loss = (
            (
                avg_loss * (period - 1)
            )
            + perdas[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (
        100 / (1 + rs)
    )


def atr(candles, period=14):
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
                    high - close_anterior
                ),
                abs(
                    low - close_anterior
                ),
            )
        )

    if len(trs) < period:
        return None

    return sum(
        trs[-period:]
    ) / period


# ============================================================
# ESTRATÉGIA
# ============================================================

def analisar(candles):

    if len(candles) < 30:
        return {
            "sinal": "AGUARDAR",
            "score": 0,
            "preco": (
                float(candles[-1]["close"])
                if candles
                else 0
            ),
            "vela": (
                candles[-1]["_dt"]
                if candles
                else None
            ),
            "mensagem": (
                "Poucas velas para analise."
            ),
        }

    c = closes(candles)

    preco = c[-1]

    ema5 = ema(c, 5)
    ema13 = ema(c, 13)
    ema21 = ema(c, 21)

    rsi14 = rsi(c, 14)

    atr14 = atr(
        candles,
        14
    )

    ultima = candles[-1]

    abertura = float(
        ultima["open"]
    )

    fechamento = float(
        ultima["close"]
    )

    maxima = float(
        ultima["high"]
    )

    minima = float(
        ultima["low"]
    )

    corpo = abs(
        fechamento - abertura
    )

    range_vela = max(
        maxima - minima,
        1e-10
    )

    forca_corpo = (
        corpo / range_vela
    )

    score_call = 0
    score_put = 0

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    if (
        ema5 is not None
        and ema13 is not None
        and ema21 is not None
    ):

        if ema5 > ema13 > ema21:
            score_call += 2

        elif ema5 < ema13 < ema21:
            score_put += 2

        elif ema5 > ema13:
            score_call += 1

        elif ema5 < ema13:
            score_put += 1

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if rsi14 is not None:

        if 50 <= rsi14 <= 70:
            score_call += 1

        elif 30 <= rsi14 < 50:
            score_put += 1

        if rsi14 > 75:
            score_call -= 1

        if rsi14 < 25:
            score_put -= 1

    # --------------------------------------------------------
    # FORÇA DA VELA
    # --------------------------------------------------------

    if (
        fechamento > abertura
        and forca_corpo >= 0.50
    ):
        score_call += 1

    elif (
        fechamento < abertura
        and forca_corpo >= 0.50
    ):
        score_put += 1

    # --------------------------------------------------------
    # MOVIMENTO
    # --------------------------------------------------------

    if len(c) >= 4:

        movimento = (
            c[-1] - c[-4]
        )

        if movimento > 0:
            score_call += 1

        elif movimento < 0:
            score_put += 1

    # --------------------------------------------------------
    # FILTRO ATR
    # --------------------------------------------------------

    if (
        atr14 is not None
        and preco != 0
    ):

        if (
            atr14 / preco
        ) < 0.00008:

            score_call = min(
                score_call,
                2
            )

            score_put = min(
                score_put,
                2
            )

    # --------------------------------------------------------
    # DECISÃO
    # --------------------------------------------------------

    diferenca = abs(
        score_call - score_put
    )

    if (
        score_call >= 4
        and diferenca >= 2
    ):

        sinal = "CALL"
        score = score_call

    elif (
        score_put >= 4
        and diferenca >= 2
    ):

        sinal = "PUT"
        score = score_put

    else:

        sinal = "AGUARDAR"
        score = max(
            score_call,
            score_put
        )

    mensagem = (
        f"CALL={score_call} | "
        f"PUT={score_put}"
    )

    if rsi14 is not None:
        mensagem += (
            f" | RSI={rsi14:.2f}"
        )

    return {
        "sinal": sinal,
        "score": score,
        "preco": preco,
        "vela": ultima["_dt"],
        "rsi": rsi14,
        "ema5": ema5,
        "ema13": ema13,
        "ema21": ema21,
        "atr": atr14,
        "score_call": score_call,
        "score_put": score_put,
        "mensagem": mensagem,
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
            "Telegram nao configurado. "
            "Configure TELEGRAM_BOT_TOKEN e "
            "TELEGRAM_CHAT_ID."
        )

        return False

    url = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    try:

        resposta = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": texto,
                "parse_mode": "HTML",
            },
            timeout=15,
        )

        # Nao usar raise_for_status() aqui.
        # Assim o log mostra exatamente o motivo
        # informado pelo Telegram em caso de erro.
        try:
            dados = resposta.json()
        except Exception:
            dados = {}

        if resposta.ok and dados.get("ok"):
            log("Mensagem enviada para o Telegram.")
            return True

        descricao = dados.get(
            "description",
            resposta.text or "Resposta vazia do Telegram."
        )

        log(
            f"ERRO Telegram HTTP {resposta.status_code}: "
            f"{descricao}"
        )

        # Diagnostico adicional para facilitar a correcao.
        if resposta.status_code == 400:
            log(
                "Verifique principalmente TELEGRAM_CHAT_ID "
                "e se o usuario/grupo iniciou uma conversa com o bot."
            )

        return False

    except requests.RequestException as e:

        log(
            f"ERRO de conexao com Telegram: {e}"
        )

        return False

    except Exception as e:

        log(
            f"ERRO inesperado ao enviar Telegram: {e}"
        )

        return False


def enviar_sinal_telegram(
    ativo,
    resultado,
    idade_dado
):

    if not telegram_configurado():
        return

    sinal = resultado.get("sinal")

    # Só envia CALL ou PUT.
    if sinal not in (
        "CALL",
        "PUT"
    ):
        return

    vela = resultado.get("vela")

    if isinstance(vela, datetime):

        vela_txt = vela.strftime(
            "%Y-%m-%d %H:%M:%S BRT"
        )

        chave_vela = vela.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    else:

        vela_txt = "-"
        chave_vela = "-"

    # --------------------------------------------------------
    # PROTEÇÃO CONTRA DUPLICAÇÃO
    # --------------------------------------------------------

    chave = (
        ativo,
        chave_vela,
        sinal
    )

    with _telegram_lock:

        ultimo = _ultimo_sinal_enviado.get(
            ativo
        )

        if ultimo == (
            chave_vela,
            sinal
        ):

            log(
                f"{ativo}: sinal {sinal} "
                "ja enviado para esta vela."
            )

            return

        # Marca antes do envio para evitar
        # duplicações em chamadas simultâneas.
        _ultimo_sinal_enviado[
            ativo
        ] = (
            chave_vela,
            sinal
        )

    # --------------------------------------------------------
    # DADOS DO SINAL
    # --------------------------------------------------------

    preco = resultado.get(
        "preco"
    )

    score = resultado.get(
        "score",
        0
    )

    score_call = resultado.get(
        "score_call",
        "-"
    )

    score_put = resultado.get(
        "score_put",
        "-"
    )

    rsi14 = resultado.get(
        "rsi"
    )

    ema5 = resultado.get(
        "ema5"
    )

    ema13 = resultado.get(
        "ema13"
    )

    ema21 = resultado.get(
        "ema21"
    )

    preco_txt = (
        f"{preco:.5f}"
        if isinstance(
            preco,
            (float, int)
        )
        else "-"
    )

    rsi_txt = (
        f"{rsi14:.2f}"
        if isinstance(
            rsi14,
            (float, int)
        )
        else "-"
    )

    ema5_txt = (
        f"{ema5:.5f}"
        if isinstance(
            ema5,
            (float, int)
        )
        else "-"
    )

    ema13_txt = (
        f"{ema13:.5f}"
        if isinstance(
            ema13,
            (float, int)
        )
        else "-"
    )

    ema21_txt = (
        f"{ema21:.5f}"
        if isinstance(
            ema21,
            (float, int)
        )
        else "-"
    )

    idade_txt = (
        f"{idade_dado:.1f} min"
        if isinstance(
            idade_dado,
            (float, int)
        )
        else "-"
    )

    emoji = (
        "🟢"
        if sinal == "CALL"
        else "🔴"
    )

    # --------------------------------------------------------
    # MENSAGEM
    # --------------------------------------------------------

    mensagem = f"""
{emoji} <b>SINAL FOREX 5M</b>

<b>Ativo:</b> {ativo}
<b>Direção:</b> {sinal}
<b>Score:</b> {score}

<b>Preço:</b> {preco_txt}
<b>Vela:</b> {vela_txt}
<b>Idade do dado:</b> {idade_txt}

<b>Score CALL:</b> {score_call}
<b>Score PUT:</b> {score_put}

<b>RSI 14:</b> {rsi_txt}
<b>EMA 5:</b> {ema5_txt}
<b>EMA 13:</b> {ema13_txt}
<b>EMA 21:</b> {ema21_txt}

⚠️ <i>Sinal baseado em análise técnica.
Não há garantia de resultado. Utilize gerenciamento de risco.</i>
""".strip()

    sucesso = enviar_telegram(
        mensagem
    )

    # Se falhar, remove a marcação
    # para permitir nova tentativa.
    if not sucesso:

        with _telegram_lock:

            atual = _ultimo_sinal_enviado.get(
                ativo
            )

            if atual == (
                chave_vela,
                sinal
            ):
                del _ultimo_sinal_enviado[
                    ativo
                ]

        log(
            f"{ativo}: sinal {sinal} "
            "liberado para nova tentativa."
        )


# ============================================================
# PROCESSAMENTO DOS ATIVOS
# ============================================================

def processar_ativo(
    chave,
    symbol
):

    try:

        log(
            f"Consultando Twelve Data: {symbol}"
        )

        candles = obter_candles(
            symbol
        )

        log(
            f"{symbol}: "
            f"{len(candles)} candles recebidos."
        )

        ultimo_raw, idade = (
            idade_do_ultimo_candle(
                candles
            )
        )

        if ultimo_raw is None:

            raise RuntimeError(
                "Nao foi possivel identificar "
                "o ultimo candle."
            )

        log(
            f"{symbol} | ultimo candle="
            f"{ultimo_raw['_dt'].strftime('%Y-%m-%d %H:%M:%S')} BRT | "
            f"atraso={idade:.2f} min"
        )

        # ----------------------------------------------------
        # FILTRO DE DADO ATRASADO
        # ----------------------------------------------------

        if idade > MAX_ATRASO_MINUTOS:

            resultado = {
                "sinal": "AGUARDAR",
                "score": 0,
                "preco": float(
                    ultimo_raw["close"]
                ),
                "vela": ultimo_raw["_dt"],
                "mensagem": (
                    f"Dado atrasado "
                    f"({idade:.1f} min). "
                    "Aguardando atualizacao."
                ),
            }

        else:

            fechadas = (
                somente_velas_fechadas(
                    candles
                )
            )

            if len(fechadas) < 30:

                resultado = {
                    "sinal": "AGUARDAR",
                    "score": 0,
                    "preco": float(
                        ultimo_raw["close"]
                    ),
                    "vela": ultimo_raw["_dt"],
                    "mensagem": (
                        "Velas fechadas "
                        "insuficientes."
                    ),
                }

            else:

                resultado = analisar(
                    fechadas
                )

        # ----------------------------------------------------
        # ATUALIZA ESTADO
        # ----------------------------------------------------

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
            else (
                preco
                or "-"
            )
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

            "rsi":
                (
                    f"{resultado['rsi']:.2f}"
                    if isinstance(
                        resultado.get("rsi"),
                        (float, int)
                    )
                    else "-"
                ),

            "ema5":
                (
                    f"{resultado['ema5']:.5f}"
                    if isinstance(
                        resultado.get("ema5"),
                        (float, int)
                    )
                    else "-"
                ),

            "ema13":
                (
                    f"{resultado['ema13']:.5f}"
                    if isinstance(
                        resultado.get("ema13"),
                        (float, int)
                    )
                    else "-"
                ),

            "ema21":
                (
                    f"{resultado['ema21']:.5f}"
                    if isinstance(
                        resultado.get("ema21"),
                        (float, int)
                    )
                    else "-"
                ),
        }

        # ----------------------------------------------------
        # LOG
        # ----------------------------------------------------

        log(
            f"{symbol} -> "
            f"{resultado['sinal']} | "
            f"score={resultado['score']} | "
            f"preco={estado['preco']} | "
            f"vela={estado['vela']} | "
            f"dado={idade:.1f} min"
        )

        # ----------------------------------------------------
        # TELEGRAM
        # ----------------------------------------------------

        if resultado.get(
            "sinal"
        ) in (
            "CALL",
            "PUT"
        ):

            enviar_sinal_telegram(
                symbol,
                resultado,
                idade
            )

        return resultado

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

        return None


# ============================================================
# HORÁRIO DE OPERAÇÃO
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
# LEITURA DOS ATIVOS
# ============================================================

def executar_leitura():

    log(
        "Iniciando leitura dos ativos."
    )

    if not API_KEY:

        log(
            "ERRO: TWELVE_DATA_API_KEY "
            "nao configurada."
        )

        estado["sinal"] = "AGUARDAR"

        estado["mensagem"] = (
            "Configure "
            "TWELVE_DATA_API_KEY "
            "no Render."
        )

        return

    if not dentro_do_horario():

        agora = agora_brt()

        log(
            f"Fora do horario configurado "
            f"({HORA_INICIO:02d}:00 as "
            f"{HORA_FIM:02d}:00 BRT)."
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

    log(
        "Leitura concluida as "
        f"{agora_brt().strftime('%Y-%m-%d %H:%M:%S BRT')}."
    )


# ============================================================
# ESPERA ATÉ A PRÓXIMA VELA
# ============================================================

def esperar_ate_proxima_leitura():

    agora = agora_brt()

    proximo_bloco = (
        (agora.minute // 5) + 1
    ) * 5

    if proximo_bloco >= 60:

        proxima = (
            agora
            + timedelta(hours=1)
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
        f"{proxima.strftime('%H:%M:%S BRT')}."
    )

    time.sleep(segundos)


# ============================================================
# LOOP DO ROBÔ
# ============================================================

def loop_robo():

    log(
        "Loop do robo iniciado."
    )

    log(
        "Executando primeira leitura "
        "imediatamente."
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
# INICIALIZAÇÃO
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
            "Thread do robo iniciada "
            "no worker web."
        )


@app.before_request
def iniciar_robo():

    garantir_robo_iniciado()


# ============================================================
# INTERFACE WEB
# ============================================================

HTML = """
<!DOCTYPE html>

<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width, initial-scale=1.0">

<title>Robo Forex 5M</title>

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

<h1>Robo Forex 5M</h1>

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

<span>Preco</span>

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

</div>


<div class="card">

<div class="observacao">

{{ estado.mensagem }}

<br><br>

O sinal e uma analise tecnica auxiliar
e nao garante resultado.

<br>

Use primeiro em conta demo
e/ou backtest.

</div>

</div>


<div class="atualizacao">

A pagina atualiza automaticamente
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

    return render_template_string(
        HTML,
        estado=estado
    )


@app.route("/dados")
def dados():

    garantir_robo_iniciado()

    return jsonify(
        estado
    )


@app.route("/health")
def health():

    return jsonify({

        "status": "ok",

        "bot_iniciado":
            _robo_started,

        "telegram_configurado":
            telegram_configurado(),

        "horario_brt":
            agora_brt().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

    })


# ============================================================
# EXECUÇÃO LOCAL
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