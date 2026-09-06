import os
import time
import json
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import websocket

from flask import Flask, jsonify, render_template_string, request


app = Flask(__name__)


# ============================================================
# CONFIGURAÇÃO
# SOMENTE CONTA DEMO
# ============================================================

BULLEX_WS_URL = os.getenv(
    "BULLEX_WS_URL",
    "wss://ws.trade.bull-ex.com/echo/websocket"
).strip()


BULLEX_ORIGIN = os.getenv(
    "BULLEX_ORIGIN",
    "https://bull-ex.com"
).strip()


BULLEX_SSID = os.getenv(
    "BULLEX_SSID",
    ""
).strip()


BULLEX_COOKIE = os.getenv(
    "BULLEX_COOKIE",
    ""
).strip()


BULLEX_PROTOCOL = int(
    os.getenv(
        "BULLEX_PROTOCOL",
        "3"
    ).strip()
    or "3"
)


BULLEX_USER_AGENT = os.getenv(
    "BULLEX_USER_AGENT",
    "Mozilla/5.0 (Linux; Android 10; K) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/152.0.0.0 Mobile Safari/537.36"
).strip()


BULLEX_USER_BALANCE_ID = os.getenv(
    "BULLEX_USER_BALANCE_ID",
    ""
).strip()


VALOR_PADRAO = float(
    os.getenv(
        "TESTE_VALOR",
        "5.00"
    )
)


# ============================================================
# ATIVOS
# ============================================================

ATIVOS = {

    "EUR/USD": {
        "active_id": 76,
        "ticker": "EURUSD-OTC",
    },

    "EUR/JPY": {
        "active_id": 79,
        "ticker": "EURJPY-OTC",
    },

    "GBP/USD": {
        "active_id": 81,
        "ticker": "GBPUSD-OTC",
    },

    "USD/JPY": {
        "active_id": 85,
        "ticker": "USDJPY-OTC",
    },

    "GBP/JPY": {
        "active_id": 84,
        "ticker": "GBPJPY-OTC",
    },

}


# ============================================================
# TIMEZONE
# ============================================================

TZ = ZoneInfo(
    "America/Sao_Paulo"
)


# ============================================================
# ESTADO WEBSOCKET
# ============================================================

_ws = None

_ws_lock = threading.RLock()

_request_lock = threading.Lock()

_cv = threading.Condition(
    _ws_lock
)

_request_counter = 1000

_responses = {}

_connected = False

_authenticated = False

_auth_event = threading.Event()

_auth_request_id = None

_client_session_id = None

_ws_thread_started = False

_last_error = None


# ============================================================
# BALANCE
# ============================================================

_balance_id = None

_balance_source = None


# ============================================================
# TESTE
# ============================================================

_test_lock = threading.Lock()

_last_test = None


# ============================================================
# LOG
# ============================================================

def log(msg):

    print(
        "[TESTE] "
        f"{datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')} "
        f"{msg}",
        flush=True
    )


# ============================================================
# REQUEST ID
# ============================================================

def next_request_id():

    global _request_counter

    with _request_lock:

        _request_counter += 1

        contador = _request_counter

    return (
        f"{int(time.time())}_"
        f"{contador}"
    )


# ============================================================
# AUTENTICAÇÃO
# ============================================================

def auth_message():

    if not BULLEX_SSID:

        raise RuntimeError(
            "BULLEX_SSID não configurado."
        )

    return {

        "name": "authenticate",

        "request_id":
            next_request_id(),

        "local_time":
            9087,

        "msg": {

            "ssid":
                BULLEX_SSID,

            "protocol":
                BULLEX_PROTOCOL,

            "session_id":
                "",

            "client_session_id":
                "",

        },

    }


# ============================================================
# SEND MESSAGE
# ============================================================

def send_message(
    name,
    version,
    body=None
):

    payload = {

        "name":
            "sendMessage",

        "request_id":
            next_request_id(),

        "local_time":
            int(
                time.time() * 1000
            ) % 1000000,

        "msg": {

            "name":
                name,

            "version":
                version,

        },

    }

    if body is not None:

        payload[
            "msg"
        ][
            "body"
        ] = body

    return payload


# ============================================================
# WEBSOCKET - OPEN
# ============================================================

def on_open(ws):

    global _auth_request_id

    try:

        msg = auth_message()

        _auth_request_id = (
            msg["request_id"]
        )

        ws.send(
            json.dumps(
                msg,
                separators=(
                    ",",
                    ":"
                )
            )
        )

        log(
            "Autenticação enviada "
            f"request_id={_auth_request_id}"
        )

    except Exception as e:

        log(
            f"Erro ao autenticar: {e}"
        )


# ============================================================
# WEBSOCKET - MESSAGE
# ============================================================

def on_message(
    ws,
    raw
):

    global \
        _authenticated, \
        _connected, \
        _client_session_id, \
        _last_error

    try:

        data = json.loads(
            raw
        )

    except Exception:

        return

    if not isinstance(
        data,
        dict
    ):

        return

    name = data.get(
        "name"
    )

    request_id = data.get(
        "request_id"
    )


    # ========================================================
    # AUTENTICAÇÃO
    # ========================================================

    if (
        name == "authenticated"
        and
        data.get("msg") is True
    ):

        _authenticated = True

        _connected = True

        msg = data.get(
            "msg"
        )

        if isinstance(
            msg,
            dict
        ):

            _client_session_id = (
                msg.get(
                    "client_session_id"
                )
            )

        else:

            _client_session_id = (
                data.get(
                    "client_session_id"
                )
            )

        _auth_event.set()

        log(
            "AUTENTICADO com sucesso."
        )

        return


    # ========================================================
    # ERRO DE AUTENTICAÇÃO
    # ========================================================

    if (
        name in (
            "authentication-failed",
            "authentication_failed",
            "error",
        )
        and
        not _authenticated
    ):

        _last_error = str(
            data
        )

        _auth_event.set()

        log(
            "Erro de autenticação: "
            f"{data}"
        )

        return


    # ========================================================
    # RESPOSTAS
    # ========================================================

    if request_id:

        with _cv:

            _responses[
                str(request_id)
            ] = data

            _cv.notify_all()


# ============================================================
# WEBSOCKET - ERROR
# ============================================================

def on_error(
    ws,
    error
):

    global _last_error

    _last_error = str(
        error
    )

    log(
        f"WebSocket erro: {error}"
    )


# ============================================================
# WEBSOCKET - CLOSE
# ============================================================

def on_close(
    ws,
    code,
    reason
):

    global \
        _connected, \
        _authenticated

    _connected = False

    _authenticated = False

    log(
        "WebSocket fechado "
        f"code={code} "
        f"reason={reason}"
    )


# ============================================================
# LOOP WEBSOCKET
# ============================================================

def ws_loop():

    global _ws

    while True:

        try:

            headers = [
                f"User-Agent: "
                f"{BULLEX_USER_AGENT}"
            ]

            if BULLEX_COOKIE:

                headers.append(
                    f"Cookie: "
                    f"{BULLEX_COOKIE}"
                )


            _ws = websocket.WebSocketApp(

                BULLEX_WS_URL,

                header=headers,

                on_open=on_open,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close,

            )


            log(
                "Conectando em "
                f"{BULLEX_WS_URL}"
            )


            _ws.run_forever(

                ping_interval=20,

                ping_timeout=10,

                origin=BULLEX_ORIGIN,

            )


        except Exception as e:

            log(
                f"Falha no WebSocket: {e}"
            )


        time.sleep(
            3
        )


# ============================================================
# INICIAR WS
# ============================================================

def start_ws():

    global _ws_thread_started

    if _ws_thread_started:

        return

    with _ws_lock:

        if _ws_thread_started:

            return

        _ws_thread_started = True

        threading.Thread(

            target=ws_loop,

            daemon=True,

            name="bullex-test-ws"

        ).start()


# ============================================================
# AGUARDAR AUTENTICAÇÃO
# ============================================================

def wait_auth(
    timeout=15
):

    start_ws()

    if _authenticated:

        return True

    _auth_event.clear()

    deadline = (
        time.time()
        +
        timeout
    )

    while (
        time.time()
        <
        deadline
    ):

        if _authenticated:

            return True

        _auth_event.wait(
            0.5
        )

    return _authenticated


# ============================================================
# ENVIAR E AGUARDAR
# ============================================================

def request_wait(
    name,
    version,
    body=None,
    timeout=15
):

    if not wait_auth(
        timeout
    ):

        raise RuntimeError(
            "WebSocket não autenticado. "
            f"erro={_last_error}"
        )


    payload = send_message(
        name,
        version,
        body
    )


    request_id = str(
        payload[
            "request_id"
        ]
    )


    with _cv:

        _responses.pop(
            request_id,
            None
        )


        if (
            _ws is None
            or
            not _connected
        ):

            raise RuntimeError(
                "WebSocket desconectado."
            )


        _ws.send(
            json.dumps(
                payload,
                separators=(
                    ",",
                    ":"
                )
            )
        )


        log(
            f"Enviado {name} "
            f"v{version} "
            f"request_id={request_id}"
        )

        if body is not None:

            log(
                "BODY="
                + json.dumps(
                    body,
                    ensure_ascii=False
                )
            )


        end = (
            time.time()
            +
            timeout
        )


        while (
            time.time()
            <
            end
        ):

            if request_id in _responses:

                resposta = (
                    _responses.pop(
                        request_id
                    )
                )

                log(
                    f"Resposta {name}: "
                    f"{resposta}"
                )

                return resposta


            _cv.wait(
                min(
                    0.5,
                    end - time.time()
                )
            )


    raise TimeoutError(
        f"Timeout aguardando "
        f"{name} "
        f"request_id={request_id}"
    )


# ============================================================
# BALANCE DEMO
# ============================================================

def extract_demo_balance(
    obj
):

    if BULLEX_USER_BALANCE_ID:

        return (
            BULLEX_USER_BALANCE_ID,
            "ENV"
        )


    found = []


    def walk(x):

        if isinstance(
            x,
            dict
        ):

            if (
                x.get("id")
                not in (
                    None,
                    ""
                )
                and
                str(
                    x.get("type")
                ) == "4"
            ):

                found.append(
                    str(
                        x["id"]
                    )
                )


            for v in x.values():

                walk(v)


        elif isinstance(
            x,
            list
        ):

            for v in x:

                walk(v)


    walk(obj)


    if found:

        return (
            found[0],
            "DEMO_TYPE_4"
        )


    return (
        None,
        None
    )


# ============================================================
# OBTER BALANCE
# ============================================================

def get_demo_balance():

    global \
        _balance_id, \
        _balance_source


    if _balance_id:

        return _balance_id


    if BULLEX_USER_BALANCE_ID:

        _balance_id = (
            BULLEX_USER_BALANCE_ID
        )

        _balance_source = (
            "ENV"
        )

        log(
            "Balance DEMO via ENV: "
            f"{_balance_id}"
        )

        return _balance_id


    resposta = request_wait(

        "get-balances",

        "1.0",

        None,

        15

    )


    (
        _balance_id,
        _balance_source
    ) = extract_demo_balance(
        resposta
    )


    if not _balance_id:

        raise RuntimeError(
            "Nenhum balance DEMO "
            "type=4 encontrado."
        )


    log(
        "Balance DEMO encontrado: "
        f"id={_balance_id} "
        f"fonte={_balance_source}"
    )


    return _balance_id


# ============================================================
# HORÁRIO DO INSTRUMENTO
# ============================================================

def instrument_time():

    agora = datetime.now(
        TZ
    )

    minuto = (
        agora.minute // 5
    ) * 5

    return agora.replace(

        minute=minuto,

        second=0,

        microsecond=0

    )


# ============================================================
# ID ESPERADO
# ============================================================

def expected_instrument_id(
    active_id,
    dt=None
):

    dt = (
        dt
        or
        instrument_time()
    )

    return (
        f"do{int(active_id)}"
        f"{dt.strftime('%Y%m%d')}"
        f"D{dt.strftime('%H%M')}"
        f"T5MPSPT"
    )


# ============================================================
# EXTRAÇÃO RECURSIVA
# ============================================================

def extract_instruments(
    obj,
    active_id,
    out=None
):

    if out is None:

        out = []


    if isinstance(
        obj,
        dict
    ):

        aid = obj.get(
            "asset_id",
            obj.get(
                "active_id",
                obj.get(
                    "underlying_id"
                )
            )
        )


        iid = obj.get(
            "instrument_id",
            obj.get(
                "instrumentId",
                obj.get(
                    "id"
                )
            )
        )


        idx = obj.get(
            "instrument_index",
            obj.get(
                "instrumentIndex",
                obj.get(
                    "index"
                )
            )
        )


        if (
            iid is not None
            and
            (
                aid is None
                or
                str(aid)
                ==
                str(active_id)
            )
        ):

            out.append({

                "instrument_id":
                    str(iid),

                "instrument_index":
                    idx,

                "asset_id":
                    aid
                    or
                    active_id,

                "raw":
                    obj,

            })


        for v in obj.values():

            extract_instruments(
                v,
                active_id,
                out
            )


    elif isinstance(
        obj,
        list
    ):

        for v in obj:

            extract_instruments(
                v,
                active_id,
                out
            )


    return out


# ============================================================
# BUSCAR INSTRUMENTO 5M
# ============================================================

def find_5m_instrument(
    active_id
):

    expected = (
        expected_instrument_id(
            active_id
        )
    )


    log(
        "================================"
    )

    log(
        "PROCURANDO INSTRUMENTO"
    )

    log(
        f"active_id={active_id}"
    )

    log(
        f"expected={expected}"
    )

    log(
        "================================"
    )


    respostas = []


    consultas = [

        (
            "3.0",
            {
                "asset_id":
                    int(active_id),

                "instrument_type":
                    "digital",
            }
        ),

        (
            "2.0",
            {
                "asset_id":
                    int(active_id)
            }
        ),

        (
            "3.0",
            {
                "asset_id":
                    int(active_id)
            }
        ),

    ]


    for version, body in consultas:

        try:

            resposta = request_wait(

                "digital-options.get-instruments",

                version,

                body,

                15

            )


            respostas.append(
                resposta
            )


            candidatos = (
                extract_instruments(
                    resposta,
                    active_id
                )
            )


            log(
                f"get-instruments "
                f"v{version}: "
                f"{len(candidatos)} "
                f"candidatos encontrados"
            )


            for candidato in candidatos:

                iid = (
                    candidato[
                        "instrument_id"
                    ]
                )


                # PRIMEIRA PRIORIDADE:
                # ID EXATO

                if iid == expected:

                    log(
                        "INSTRUMENTO EXATO "
                        "ENCONTRADO: "
                        f"{iid}"
                    )

                    return candidato


                # SEGUNDA PRIORIDADE:
                # QUALQUER ID 5M

                if "T5M" in iid.upper():

                    log(
                        "INSTRUMENTO 5M "
                        "ENCONTRADO: "
                        f"{iid}"
                    )

                    return candidato


        except Exception as e:

            log(
                f"Falha "
                f"get-instruments "
                f"v{version}: "
                f"{e}"
            )


    # ========================================================
    # DIAGNÓSTICO FINAL
    # ========================================================

    todos = []


    for resposta in respostas:

        candidatos = (
            extract_instruments(
                resposta,
                active_id
            )
        )


        for candidato in candidatos:

            iid = (
                candidato[
                    "instrument_id"
                ]
            )


            if iid not in [
                x["instrument_id"]
                for x in todos
            ]:

                todos.append(
                    candidato
                )


    if todos:

        log(
            "================================"
        )

        log(
            "INSTRUMENTOS RETORNADOS"
        )

        log(
            "================================"
        )


        for candidato in todos[:100]:

            log(
                f"ID="
                f"{candidato['instrument_id']} "
                f"INDEX="
                f"{candidato.get('instrument_index')}"
            )


    else:

        log(
            "A Bullex não retornou "
            "nenhum instrument_id "
            "identificável."
        )


    return None


# ============================================================
# FORÇAR ORDEM DEMO
# ============================================================

def force_order(
    symbol,
    direction,
    amount
):

    global _last_test


    if symbol not in ATIVOS:

        raise ValueError(
            "Ativo inválido."
        )


    if direction not in (
        "CALL",
        "PUT"
    ):

        raise ValueError(
            "Direção deve ser "
            "CALL ou PUT."
        )


    amount = float(
        amount
    )


    if (
        amount <= 0
        or
        amount > 100
    ):

        raise ValueError(
            "Valor deve estar "
            "entre R$0,01 "
            "e R$100,00."
        )


    if not _test_lock.acquire(
        blocking=False
    ):

        raise RuntimeError(
            "Já existe um teste "
            "em andamento."
        )


    try:

        # ====================================================
        # BALANCE DEMO
        # ====================================================

        balance = (
            get_demo_balance()
        )


        # ====================================================
        # ATIVO
        # ====================================================

        active_id = int(
            ATIVOS[
                symbol
            ][
                "active_id"
            ]
        )


        # ====================================================
        # INSTRUMENTO
        # ====================================================

        instrument = (
            find_5m_instrument(
                active_id
            )
        )


        if not instrument:

            raise RuntimeError(
                "Instrumento 5M "
                f"não encontrado "
                f"para {symbol}. "
                "Veja os IDs retornados "
                "no log."
            )


        instrument_id = (
            instrument[
                "instrument_id"
            ]
        )


        instrument_index = (
            instrument.get(
                "instrument_index"
            )
        )


        # ====================================================
        # BODY DA ORDEM
        # ====================================================

        body = {

            "user_balance_id":
                str(balance),

            "instrument_id":
                instrument_id,

            "amount":
                str(amount),

            "instrument_index":
                instrument_index,

            "asset_id":
                active_id,

            "instrument_dir":
                (
                    "call"
                    if direction == "CALL"
                    else
                    "put"
                ),

        }


        log(
            "================================"
        )

        log(
            "FORÇANDO ENTRADA DEMO"
        )

        log(
            f"Ativo={symbol}"
        )

        log(
            f"Direção={direction}"
        )

        log(
            f"Valor=R${amount:.2f}"
        )

        log(
            f"Balance={balance}"
        )

        log(
            f"Instrument ID="
            f"{instrument_id}"
        )

        log(
            f"Instrument index="
            f"{instrument_index}"
        )

        log(
            "================================"
        )


        # ====================================================
        # ENVIO
        # ====================================================

        resposta = request_wait(

            "digital-options.place-digital-option",

            "3.0",

            body,

            15

        )


        bruto = json.dumps(

            resposta,

            ensure_ascii=False

        ).lower()


        sucesso = (

            resposta.get(
                "msg"
            )
            is True

            or

            resposta.get(
                "success"
            )
            is True

            or

            (
                isinstance(
                    resposta.get(
                        "msg"
                    ),
                    dict
                )
                and
                resposta[
                    "msg"
                ].get(
                    "success"
                )
                is True
            )

            or

            "digital-option-placed"
            in bruto

            or

            '"success":true'
            in bruto

        )


        resultado = {

            "confirmada":
                bool(sucesso),

            "modo":
                "DEMO_ONLY",

            "symbol":
                symbol,

            "direction":
                direction,

            "amount":
                amount,

            "active_id":
                active_id,

            "instrument_id":
                instrument_id,

            "instrument_index":
                instrument_index,

            "balance_source":
                _balance_source,

            "response":
                resposta,

        }


        _last_test = (
            resultado
        )


        if sucesso:

            log(
                "################################"
            )

            log(
                "ORDEM DEMO CONFIRMADA!"
            )

            log(
                "################################"
            )

        else:

            log(
                "ORDEM NÃO CONFIRMADA:"
            )

            log(
                str(resposta)
            )


        return resultado


    finally:

        _test_lock.release()


# ============================================================
# HTML
# ============================================================

HTML = """

<!doctype html>

<html lang="pt-BR">

<head>

<meta charset="utf-8">

<meta
name="viewport"
content="width=device-width,initial-scale=1"
>

<title>
Teste Bullex DEMO
</title>

<style>

body {

    font-family: Arial;

    background: #111;

    color: #fff;

    margin: 0;

    padding: 20px;
}

.box {

    max-width: 650px;

    margin: auto;

    background: #1d1d1d;

    padding: 22px;

    border-radius: 15px;
}

select,
input,
button {

    width: 100%;

    padding: 14px;

    margin: 7px 0;

    border-radius: 9px;

    border: 0;

    font-size: 16px;

    box-sizing: border-box;
}

button {

    cursor: pointer;

    font-weight: bold;
}

.row {

    display: flex;

    gap: 10px;
}

.row button {

    width: 50%;
}

.call {

    background: #16834b;

    color: #fff;
}

.put {

    background: #b52b35;

    color: #fff;
}

.status {

    background: #292929;

    padding: 15px;

    border-radius: 10px;

    margin: 15px 0;

    line-height: 1.7;
}

pre {

    white-space: pre-wrap;

    word-break: break-word;

    background: #090909;

    padding: 12px;

    border-radius: 8px;

    font-size: 12px;
}

.aviso {

    background: #3a2f12;

    padding: 12px;

    border-radius: 8px;

    margin-bottom: 15px;
}

</style>

</head>

<body>

<div class="box">

<h1>
Teste de Entrada DEMO
</h1>

<div class="aviso">

<strong>
ATENÇÃO:
</strong>

Este aplicativo ignora a estratégia
e existe somente para testar o caminho
de execução da conta DEMO.

</div>


<div
class="status"
id="status"
>

Carregando...

</div>


<label>
Ativo
</label>


<select
id="symbol"
>

<option>
EUR/USD
</option>

<option>
EUR/JPY
</option>

<option>
GBP/USD
</option>

<option>
USD/JPY
</option>

<option>
GBP/JPY
</option>

</select>


<label>
Valor da entrada
</label>


<input
id="amount"
type="number"
min="0.01"
max="100"
step="0.01"
value="5.00"
>


<div class="row">


<button
class="call"
onclick="test('CALL')"
>

FORÇAR CALL DEMO

</button>


<button
class="put"
onclick="test('PUT')"
>

FORÇAR PUT DEMO

</button>


</div>


<div
class="status"
id="result"
>

Aguardando teste.

</div>


</div>


<script>


async function status() {

    try {

        let r =
            await fetch(
                '/status'
            );

        let d =
            await r.json();


        document
            .getElementById(
                'status'
            )
            .innerHTML =

            'Modo: <b>'
            + d.mode
            + '</b><br>'

            + 'WebSocket: <b>'
            + d.websocket
            + '</b><br>'

            + 'Autenticado: <b>'
            + d.authenticated
            + '</b><br>'

            + 'Balance DEMO: <b>'
            + d.balance
            + '</b><br>'

            + 'Fonte: <b>'
            + d.balance_source
            + '</b><br>'

            + 'Último erro: <b>'
            + (
                d.error
                || '-'
            )
            + '</b>';

    }

    catch(e) {

        document
            .getElementById(
                'status'
            )
            .textContent = e;

    }

}


async function test(
    direction
) {

    let symbol =
        document
            .getElementById(
                'symbol'
            )
            .value;


    let amount =
        document
            .getElementById(
                'amount'
            )
            .value;


    document
        .getElementById(
            'result'
        )
        .textContent =
            'Enviando teste '
            + symbol
            + ' '
            + direction
            + '...';


    try {

        let r =
            await fetch(
                '/teste/entrada',
                {

                    method:
                        'POST',

                    headers: {
                        'Content-Type':
                            'application/json'
                    },

                    body:
                        JSON.stringify({

                            symbol:
                                symbol,

                            direction:
                                direction,

                            amount:
                                amount

                        })

                }
            );


        let d =
            await r.json();


        document
            .getElementById(
                'result'
            )
            .innerHTML =

            '<pre>'
            +
            JSON.stringify(
                d,
                null,
                2
            )
            +
            '</pre>';


        status();


    }

    catch(e) {

        document
            .getElementById(
                'result'
            )
            .textContent =
                e;

    }

}


status();

setInterval(
    status,
    5000
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

    start_ws()

    return render_template_string(
        HTML
    )


@app.route("/status")
def status():

    return jsonify({

        "mode":
            "DEMO_ONLY",

        "websocket":
            (
                "CONECTADO"
                if _connected
                else
                "DESCONECTADO"
            ),

        "authenticated":
            _authenticated,

        "balance":
            (
                "ENCONTRADO"
                if _balance_id
                else
                "AGUARDANDO"
            ),

        "balance_source":
            _balance_source,

        "last_test":
            _last_test,

        "error":
            _last_error,

    })


# ============================================================
# FORÇAR ENTRADA
# ============================================================

@app.route(
    "/teste/entrada",
    methods=["POST"]
)
def teste_entrada():

    try:

        data = (
            request.get_json(
                silent=True
            )
            or {}
        )


        symbol = str(
            data.get(
                "symbol",
                "EUR/JPY"
            )
        )


        direction = str(
            data.get(
                "direction",
                "CALL"
            )
        ).upper()


        amount = float(
            data.get(
                "amount",
                VALOR_PADRAO
            )
        )


        resultado = force_order(

            symbol,

            direction,

            amount

        )


        return jsonify(
            resultado
        ), (
            200
            if resultado[
                "confirmada"
            ]
            else
            502
        )


    except Exception as e:

        log(
            f"ERRO NO TESTE: {e}"
        )


        return jsonify({

            "confirmada":
                False,

            "modo":
                "DEMO_ONLY",

            "erro":
                str(e),

        }), 400


# ============================================================
# HEALTH
# ============================================================

@app.route(
    "/health"
)
def health():

    return jsonify({

        "status":
            "ok",

        "mode":
            "DEMO_ONLY",

        "websocket":
            _connected,

        "authenticated":
            _authenticated,

        "balance":
            bool(_balance_id),

        "last_test":
            _last_test,

        "error":
            _last_error,

    })


# ============================================================
# INICIALIZAÇÃO
# ============================================================

log(
    "================================"
)

log(
    "APP TESTE BULLEX DEMO"
)

log(
    "SOMENTE DEMO"
)

log(
    "Estratégia: DESATIVADA"
)

log(
    "================================"
)


if __name__ == "__main__":

    start_ws()

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
