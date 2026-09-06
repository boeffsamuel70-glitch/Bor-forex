import os, time, json, threading
from datetime import datetime
from zoneinfo import ZoneInfo
import websocket
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)

# ============================================================
# CONFIGURAÇÃO - SOMENTE DEMO
# ============================================================
BULLEX_WS_URL = os.getenv('BULLEX_WS_URL', 'wss://ws.trade.bull-ex.com/echo/websocket').strip()
BULLEX_ORIGIN = os.getenv('BULLEX_ORIGIN', 'https://bull-ex.com').strip()
BULLEX_SSID = os.getenv('BULLEX_SSID', '').strip()
BULLEX_COOKIE = os.getenv('BULLEX_COOKIE', '').strip()
BULLEX_PROTOCOL = int(os.getenv('BULLEX_PROTOCOL', '3').strip() or '3')
BULLEX_USER_AGENT = os.getenv(
    'BULLEX_USER_AGENT',
    'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/152.0.0.0 Mobile Safari/537.36'
).strip()
BULLEX_USER_BALANCE_ID = os.getenv('BULLEX_USER_BALANCE_ID', '').strip()
VALOR_PADRAO = float(os.getenv('TESTE_VALOR', '5.00'))
MAX_ATRASO_ENTRADA_SEGUNDOS = int(os.getenv('MAX_ATRASO_ENTRADA_SEGUNDOS', '3'))

ATIVOS = {
    'EUR/USD': {'active_id': 76, 'ticker': 'EURUSD-OTC'},
    'EUR/JPY': {'active_id': 79, 'ticker': 'EURJPY-OTC'},
    'GBP/USD': {'active_id': 81, 'ticker': 'GBPUSD-OTC'},
    'USD/JPY': {'active_id': 85, 'ticker': 'USDJPY-OTC'},
    'GBP/JPY': {'active_id': 84, 'ticker': 'GBPJPY-OTC'},
}

TZ = ZoneInfo('America/Sao_Paulo')

_ws = None
_ws_lock = threading.RLock()
_request_lock = threading.Lock()
_cv = threading.Condition(_ws_lock)
_request_counter = 1000
_responses = {}
_connected = False
_authenticated = False
_auth_event = threading.Event()
_auth_request_id = None
_client_session_id = None
_ws_thread_started = False
_last_error = None
_balance_id = None
_balance_source = None
_test_lock = threading.Lock()
_last_test = None
_test_running = False

# ============================================================
# V4 - HORÁRIO DO SERVIDOR / EXPIRAÇÃO
# ============================================================
_server_timestamp = None
_server_timestamp_received_at = None
_server_time_lock = threading.Lock()


def _normalize_server_timestamp(value):
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None

    # Normaliza ns/us/ms para segundos.
    if ts > 10**18:
        ts /= 1e9
    elif ts > 10**15:
        ts /= 1e6
    elif ts > 10**12:
        ts /= 1e3

    if 1_000_000_000 <= ts <= 5_000_000_000:
        return ts
    return None


def _update_server_time_from_payload(data):
    global _server_timestamp, _server_timestamp_received_at

    if not isinstance(data, dict):
        return

    candidates = []
    msg = data.get("msg")

    if isinstance(msg, (int, float, str)):
        candidates.append(msg)
    elif isinstance(msg, dict):
        for key in ("server_timestamp", "serverTime", "timestamp", "time", "ts"):
            if key in msg:
                candidates.append(msg.get(key))

    for key in ("server_timestamp", "serverTime", "timestamp", "time", "ts"):
        if key in data:
            candidates.append(data.get(key))

    for value in candidates:
        ts = _normalize_server_timestamp(value)
        if ts is not None:
            with _server_time_lock:
                _server_timestamp = ts
                _server_timestamp_received_at = time.time()
            return


def _get_server_timestamp():
    with _server_time_lock:
        ts = _server_timestamp
        received_at = _server_timestamp_received_at

    if ts is not None and received_at is not None:
        return ts + max(0.0, time.time() - received_at), "TIMESYNC"

    return time.time(), "LOCAL_FALLBACK"


def _janela_5m():
    """
    Retorna dados da vela de 5 minutos atual, usando horário do servidor
    quando disponível.

    Regra:
    - vela abre em múltiplos de 5 minutos;
    - entrada só é permitida nos primeiros 3 segundos;
    - expiração é exatamente no fechamento da mesma vela de 5M.
    """
    server_ts, source = _get_server_timestamp()
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




def log(msg):
    print(f'[TESTE] {datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")} {msg}', flush=True)


def next_request_id():
    global _request_counter
    with _request_lock:
        _request_counter += 1
        return f'{int(time.time())}_{_request_counter}'


def auth_message():
    if not BULLEX_SSID:
        raise RuntimeError('BULLEX_SSID não configurado.')
    return {
        'name': 'authenticate',
        'request_id': next_request_id(),
        'local_time': 9087,
        'msg': {
            'ssid': BULLEX_SSID,
            'protocol': BULLEX_PROTOCOL,
            'session_id': '',
            'client_session_id': '',
        },
    }


def send_message(name, version, body=None):
    payload = {
        'name': 'sendMessage',
        'request_id': next_request_id(),
        'local_time': int(time.time() * 1000) % 1000000,
        'msg': {'name': name, 'version': version},
    }
    if body is not None:
        payload['msg']['body'] = body
    return payload


def auth_ok(data):
    return data.get('name') == 'authenticated' and data.get('msg') is True


def on_open(ws):
    global _auth_request_id
    try:
        msg = auth_message()
        _auth_request_id = msg['request_id']
        ws.send(json.dumps(msg, separators=(',', ':')))
        log(f'Autenticação enviada request_id={_auth_request_id}')
    except Exception as e:
        log(f'Erro ao autenticar: {e}')


def on_message(ws, raw):
    global _authenticated, _connected, _client_session_id, _last_error
    try:
        data = json.loads(raw)
    except Exception:
        return
    if not isinstance(data, dict):
        return

    name = data.get('name')
    req = data.get('request_id')

    if name == 'authenticated' and data.get('msg') is True:
        _authenticated = True
        _connected = True
        _client_session_id = data.get('client_session_id')
        if not _client_session_id and isinstance(data.get('msg'), dict):
            _client_session_id = data['msg'].get('client_session_id')
        _auth_event.set()
        log('AUTENTICADO com sucesso.')
        return

    if name in ('authentication-failed', 'authentication_failed', 'error') and not _authenticated:
        _last_error = str(data)
        _auth_event.set()
        log(f'Erro de autenticação: {data}')
        return

    if req:
        with _cv:
            _responses[str(req)] = data
            _cv.notify_all()


def on_error(ws, error):
    global _last_error
    _last_error = str(error)
    log(f'WebSocket erro: {error}')


def on_close(ws, code, reason):
    global _connected, _authenticated
    _connected = False
    _authenticated = False
    log(f'WebSocket fechado code={code} reason={reason}')


def ws_loop():
    global _ws
    while True:
        try:
            headers = [f'User-Agent: {BULLEX_USER_AGENT}']
            if BULLEX_COOKIE:
                headers.append(f'Cookie: {BULLEX_COOKIE}')
            _ws = websocket.WebSocketApp(
                BULLEX_WS_URL,
                header=headers,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            log(f'Conectando em {BULLEX_WS_URL}')
            _ws.run_forever(ping_interval=20, ping_timeout=10, origin=BULLEX_ORIGIN)
        except Exception as e:
            log(f'Falha no WebSocket: {e}')
        time.sleep(3)


def start_ws():
    global _ws_thread_started
    if _ws_thread_started:
        return
    with _ws_lock:
        if _ws_thread_started:
            return
        _ws_thread_started = True
        threading.Thread(target=ws_loop, daemon=True, name='bullex-test-ws').start()


def wait_auth(timeout=15):
    start_ws()
    if _authenticated:
        return True
    _auth_event.clear()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _authenticated:
            return True
        _auth_event.wait(0.5)
    return _authenticated


def request_wait(name, version, body=None, timeout=15):
    if not wait_auth(timeout):
        raise RuntimeError(f'WebSocket não autenticado. erro={_last_error}')
    payload = send_message(name, version, body)
    rid = str(payload['request_id'])
    with _cv:
        _responses.pop(rid, None)
        if _ws is None or not _connected:
            raise RuntimeError('WebSocket desconectado.')
        _ws.send(json.dumps(payload, separators=(',', ':')))
        log(f'Enviado {name} v{version} request_id={rid} body={body}')
        end = time.time() + timeout
        while time.time() < end:
            if rid in _responses:
                resposta = _responses.pop(rid)
                log(f'Resposta {name}: {resposta}')
                return resposta
            _cv.wait(min(0.5, end - time.time()))
    raise TimeoutError(f'Timeout aguardando {name} request_id={rid}')


def extract_demo_balance(obj):
    if BULLEX_USER_BALANCE_ID:
        return BULLEX_USER_BALANCE_ID, 'ENV'
    found = []
    def walk(x):
        if isinstance(x, dict):
            if x.get('id') not in (None, '') and str(x.get('type')) == '4':
                found.append(str(x['id']))
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(obj)
    return (found[0], 'DEMO_TYPE_4') if found else (None, None)


def get_demo_balance():
    global _balance_id, _balance_source
    if _balance_id:
        return _balance_id
    if BULLEX_USER_BALANCE_ID:
        _balance_id, _balance_source = int(BULLEX_USER_BALANCE_ID), 'ENV'
        log(f'Balance DEMO via ENV: {_balance_id}')
        return _balance_id
    r = request_wait('get-balances', '1.0', None, 15)
    _balance_id, _balance_source = extract_demo_balance(r)
    if not _balance_id:
        raise RuntimeError('Nenhum balance DEMO type=4 encontrado.')
    log(f'Balance DEMO encontrado: id={_balance_id} fonte={_balance_source}')
    return _balance_id


def instrument_time():
    now = datetime.now(TZ)
    minute = (now.minute // 5) * 5
    return now.replace(minute=minute, second=0, microsecond=0)


def expected_instrument_id(active_id, dt=None):
    dt = dt or instrument_time()
    return f'do{int(active_id)}{dt.strftime("%Y%m%d")}D{dt.strftime("%H%M")}T5MPSPT'


def extract_instruments(obj, active_id, out=None):
    if out is None:
        out = []
    if isinstance(obj, dict):
        aid = obj.get('asset_id', obj.get('active_id', obj.get('underlying_id')))
        iid = obj.get('instrument_id', obj.get('instrumentId', obj.get('id')))
        idx = obj.get('instrument_index', obj.get('instrumentIndex', obj.get('index')))
        if iid is not None and (aid is None or str(aid) == str(active_id)):
            out.append({'instrument_id': str(iid), 'instrument_index': idx, 'asset_id': aid or active_id, 'raw': obj})
        for v in obj.values():
            extract_instruments(v, active_id, out)
    elif isinstance(obj, list):
        for v in obj:
            extract_instruments(v, active_id, out)
    return out


def find_5m_instrument(active_id):
    expected = expected_instrument_id(active_id)
    log(
        f'Procurando instrumento OTC: active_id={active_id} '
        f'expected={expected}'
    )

    # ========================================================
    # IMPORTANTE:
    # Os três get-instruments já demonstraram timeout.
    # Não vamos mais gastar 45 segundos neles.
    #
    # Agora vamos diretamente ao endpoint que o Traderoom
    # já foi observado usando após a autenticação:
    #
    # digital-option-instruments.get-underlying-list
    #
    # Esta etapa é APENAS descoberta/diagnóstico. Nenhuma
    # ordem é enviada aqui.
    # ========================================================

    tentativas = [
        # Formatos simples
        {'asset_id': int(active_id)},
        {'active_id': int(active_id)},
        {'underlying_id': int(active_id)},

        # Possíveis filtros de tipo
        {
            'asset_id': int(active_id),
            'instrument_type': 'digital',
        },
        {
            'active_id': int(active_id),
            'instrument_type': 'digital',
        },
        {
            'underlying_id': int(active_id),
            'instrument_type': 'digital',
        },

        # Possíveis formatos usados por APIs de instrumentos
        {
            'asset_id': int(active_id),
            'type': 'digital',
        },
        {
            'active_id': int(active_id),
            'type': 'digital',
        },
        {
            'underlying_id': int(active_id),
            'type': 'digital',
        },
    ]

    respostas = []

    for body in tentativas:
        try:
            log(
                'DIAGNÓSTICO OTC: enviando '
                'digital-option-instruments.get-underlying-list '
                f'v1.0 body={body}'
            )

            r = request_wait(
                'digital-option-instruments.get-underlying-list',
                '1.0',
                body,
                8,
            )
            respostas.append(r)

            log(
                'DIAGNÓSTICO OTC: resposta: '
                + json.dumps(r, ensure_ascii=False)
            )

            # Procura qualquer referência ao ativo na resposta.
            bruto = json.dumps(r, ensure_ascii=False)
            upper = bruto.upper()

            termos = [
                'EURUSD-OTC',
                'EURUSD',
                str(active_id),
                'INSTRUMENT_ID',
                'INSTRUMENTINDEX',
                'INSTRUMENT_INDEX',
                'T5M',
            ]

            encontrados = [
                termo for termo in termos
                if termo.upper() in upper
            ]

            if encontrados:
                log(
                    'DIAGNÓSTICO OTC: termos encontrados: '
                    f'{encontrados}'
                )

            # Tentativa genérica de extrair instrument_id.
            candidatos = extract_instruments(r, active_id)

            log(
                'DIAGNÓSTICO OTC: candidatos extraídos='
                f'{len(candidatos)}'
            )

            for c in candidatos[:50]:
                log(
                    f'  CANDIDATO: id={c["instrument_id"]} '
                    f'index={c.get("instrument_index")} '
                    f'asset_id={c.get("asset_id")} '
                    f'raw={json.dumps(c.get("raw"), ensure_ascii=False)}'
                )

            # Se aparecer um instrumento 5M, podemos parar.
            for c in candidatos:
                iid = str(c['instrument_id'])

                if iid == expected or 'T5M' in iid.upper():
                    log(
                        'INSTRUMENTO 5M ENCONTRADO: '
                        f'{iid} index={c.get("instrument_index")}'
                    )
                    return c

        except Exception as e:
            log(
                'DIAGNÓSTICO OTC: falha body='
                f'{body}: {e}'
            )

    # ========================================================
    # Segunda etapa: tentar por ticker.
    # ========================================================
    ticker = next(
        (
            item['ticker']
            for item in ATIVOS.values()
            if item['active_id'] == active_id
        ),
        None,
    )

    if ticker:
        ticker_tentativas = [
            {'ticker': ticker},
            {'name': ticker},
            {'asset': ticker},
            {'underlying': ticker},
            {'symbol': ticker},
        ]

        for body in ticker_tentativas:
            try:
                log(
                    'DIAGNÓSTICO OTC: testando ticker '
                    f'body={body}'
                )

                r = request_wait(
                    'digital-option-instruments.get-underlying-list',
                    '1.0',
                    body,
                    8,
                )

                log(
                    'DIAGNÓSTICO OTC: resposta ticker: '
                    + json.dumps(r, ensure_ascii=False)
                )

                candidatos = extract_instruments(r, active_id)

                for c in candidatos:
                    iid = str(c['instrument_id'])

                    if iid == expected or 'T5M' in iid.upper():
                        log(
                            'INSTRUMENTO 5M ENCONTRADO POR TICKER: '
                            f'{iid} index={c.get("instrument_index")}'
                        )
                        return c

            except Exception as e:
                log(
                    f'DIAGNÓSTICO OTC: falha ticker body={body}: {e}'
                )

    # ========================================================
    # Se a resposta tiver underlying-list mas não tiver
    # instrument_id no formato esperado, imprime os campos
    # relevantes encontrados. Isso é justamente o que
    # precisamos para descobrir a próxima chamada do Traderoom.
    # ========================================================
    for idx, r in enumerate(respostas, 1):
        log(
            f'DIAGNÓSTICO OTC: resumo resposta #{idx}: '
            + json.dumps(r, ensure_ascii=False)
        )

    log(
        'Nenhum instrument_id 5M encontrado. '
        'A descoberta agora depende da estrutura real retornada '
        'pelo underlying-list.'
    )

    return None

def _extract_error_message(response):
    """Extrai uma mensagem textual útil de uma resposta Bullex."""
    if not isinstance(response, dict):
        return str(response)

    msg = response.get('msg')

    if isinstance(msg, dict):
        for key in ('message', 'error', 'reason', 'msg'):
            value = msg.get(key)
            if value not in (None, ''):
                return str(value)

    for key in ('message', 'error', 'reason'):
        value = response.get(key)
        if value not in (None, ''):
            return str(value)

    return ''


def _is_success_response(response):
    """Detecta de forma conservadora uma abertura aceita."""
    if not isinstance(response, dict):
        return False

    msg = response.get('msg')

    if isinstance(msg, dict):
        if msg.get('success') is True:
            return True
        if msg.get('id') not in (None, ''):
            return True

    if response.get('success') is True:
        return True

    bruto = json.dumps(response, ensure_ascii=False).lower()
    return (
        '"success":true' in bruto
        or 'digital-option-placed' in bruto
        or 'option-placed' in bruto
    )


def _build_option_bodies(balance, active_id, direction, amount):
    """
    Gera as variações mínimas de expiração que vamos testar.

    V3 NÃO muda de ativo nem de saldo. O objetivo é descobrir se o
    4104 está ligado especificamente ao campo 'expired' / à forma
    como a expiração é informada.

    Primeira tentativa: exatamente o formato que já chegou ao servidor.
    Segunda/terceira: variações de expiração comuns no protocolo.
    """
    side = 'call' if direction == 'CALL' else 'put'

    return [
        {
            'label': 'EXPIRED_1',
            'body': {
                'user_balance_id': int(balance),
                'active_id': int(active_id),
                'option_type_id': 3,
                'direction': side,
                'expired': 1,
                'price': amount,
                'refund_value': 0,
            },
        },
        {
            'label': 'EXPIRED_60',
            'body': {
                'user_balance_id': int(balance),
                'active_id': int(active_id),
                'option_type_id': 3,
                'direction': side,
                'expired': 60,
                'price': amount,
                'refund_value': 0,
            },
        },
        {
            'label': 'EXPIRED_300',
            'body': {
                'user_balance_id': int(balance),
                'active_id': int(active_id),
                'option_type_id': 3,
                'direction': side,
                'expired': 300,
                'price': amount,
                'refund_value': 0,
            },
        },
    ]


def force_order(symbol, direction, amount):
    """
    TESTE ISOLADO 5M - DEMO.

    Regras:
    - só envia ordem nos primeiros 3 segundos da vela de 5 minutos;
    - exemplo: abriu 17:30:00 -> aceita até 17:30:03;
    - expiração obrigatória em 17:35:00;
    - passou do limite, NÃO envia ordem e retorna ATRASADA;
    - nunca empurra a mesma entrada para a vela seguinte.
    """
    global _last_test

    if symbol not in ATIVOS:
        raise ValueError("Ativo inválido.")

    direction = str(direction).upper()
    if direction not in ("CALL", "PUT"):
        raise ValueError("Direção deve ser CALL ou PUT.")

    amount = float(amount)
    if amount <= 0 or amount > 100:
        raise ValueError("Valor deve estar entre R$0,01 e R$100,00.")

    if not _test_lock.acquire(blocking=False):
        raise RuntimeError("Já existe um teste de entrada em andamento.")

    try:
        balance = get_demo_balance()
        active_id = int(ATIVOS[symbol]["active_id"])
        ticker = ATIVOS[symbol]["ticker"]

        janela = _janela_5m()

        server_ts = janela["server_ts"]
        time_source = janela["source"]
        candle_open = janela["candle_open"]
        candle_close = janela["candle_close"]
        atraso = janela["atraso_segundos"]

        local_now = datetime.now(TZ)
        server_dt = datetime.fromtimestamp(server_ts, TZ)
        open_dt = datetime.fromtimestamp(candle_open, TZ)
        close_dt = datetime.fromtimestamp(candle_close, TZ)

        # Se já passou dos 3 segundos, não envia nada.
        if not janela["permitida"]:
            result = {
                "diagnostico": "TESTE_5M_JANELA_3S",
                "mode": "DEMO_ONLY_5M",
                "confirmada": False,
                "em_andamento": False,
                "status_execucao": "ATRASADA",
                "ordem_enviada": False,
                "motivo": (
                    f"Entrada bloqueada: atraso de {atraso:.3f}s. "
                    f"Máximo permitido: {MAX_ATRASO_ENTRADA_SEGUNDOS}s."
                ),
                "symbol": symbol,
                "ticker": ticker,
                "direction": direction,
                "amount": amount,
                "active_id": active_id,
                "balance_id": str(balance),
                "balance_source": _balance_source,
                "fonte_horario": time_source,
                "horario_local": local_now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "server_time_local": server_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "candle_open_timestamp": candle_open,
                "candle_open_local": open_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "expired_timestamp": candle_close,
                "expired_time_local": close_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "atraso_segundos": round(atraso, 3),
                "max_atraso_segundos": MAX_ATRASO_ENTRADA_SEGUNDOS,
                "response": None,
            }

            _last_test = result

            log("============================================================")
            log(
                f"[TESTE 5M] ENTRADA BLOQUEADA POR ATRASO | "
                f"{symbol} {direction} | atraso={atraso:.3f}s"
            )
            log(
                f"Vela: {open_dt.strftime('%H:%M:%S')} -> "
                f"{close_dt.strftime('%H:%M:%S')}"
            )
            log("Nenhuma ordem foi enviada.")
            log("============================================================")

            return result

        body = {
            "user_balance_id": int(balance),
            "active_id": active_id,
            "option_type_id": 3,
            "direction": "call" if direction == "CALL" else "put",
            "expired": int(candle_close),
            "price": amount,
            "refund_value": 0,
        }

        log("============================================================")
        log(f"[TESTE 5M] ENVIANDO ORDEM DEMO: {symbol} ({ticker})")
        log(f"Direção={direction} valor=R${amount:.2f}")
        log(f"atraso={atraso:.3f}s limite={MAX_ATRASO_ENTRADA_SEGUNDOS}s")
        log(
            f"vela={open_dt.strftime('%H:%M:%S')} -> "
            f"{close_dt.strftime('%H:%M:%S')}"
        )
        log(f"expired_timestamp={candle_close}")
        log(f"fonte_horario={time_source}")
        log("body=" + json.dumps(body, ensure_ascii=False))
        log("============================================================")

        resposta = request_wait(
            "binary-options.open-option",
            "1.0",
            body,
            20,
        )

        msg = resposta.get("msg") if isinstance(resposta, dict) else None
        bruto = json.dumps(resposta, ensure_ascii=False).lower()

        sucesso = False
        option = msg if isinstance(msg, dict) else None

        if isinstance(msg, dict):
            if msg.get("success") is True:
                sucesso = True
            if msg.get("id") not in (None, "", 0):
                sucesso = True

        if isinstance(resposta, dict) and resposta.get("success") is True:
            sucesso = True

        if '"success":true' in bruto or "digital-option-placed" in bruto:
            sucesso = True

        mensagem = _extract_error_message(resposta)

        result = {
            "diagnostico": "TESTE_5M_JANELA_3S",
            "mode": "DEMO_ONLY_5M",
            "confirmada": bool(sucesso),
            "em_andamento": False,
            "status_execucao": "CONFIRMADA" if sucesso else "NAO_CONFIRMADA",
            "ordem_enviada": True,
            "symbol": symbol,
            "ticker": ticker,
            "direction": direction,
            "amount": amount,
            "active_id": active_id,
            "balance_id": str(balance),
            "balance_source": _balance_source,
            "endpoint": "binary-options.open-option",
            "version": "1.0",
            "option_type_id": 3,
            "fonte_horario": time_source,
            "horario_local": local_now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "server_time_local": server_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "candle_open_timestamp": candle_open,
            "candle_open_local": open_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "expired_timestamp": candle_close,
            "expired_time_local": close_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "atraso_segundos": round(atraso, 3),
            "max_atraso_segundos": MAX_ATRASO_ENTRADA_SEGUNDOS,
            "request_body": body,
            "response": resposta,
            "option": option,
            "mensagem": mensagem or None,
        }

        _last_test = result

        if sucesso:
            log("[TESTE 5M] ORDEM DEMO ACEITA!")
        else:
            log("[TESTE 5M] ORDEM NÃO CONFIRMADA.")
            if mensagem:
                log("MENSAGEM: " + mensagem)
            log("RESPOSTA: " + json.dumps(resposta, ensure_ascii=False))

        return result

    finally:
        _test_lock.release()


HTML = '''<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Teste Bullex DEMO</title><style>body{font-family:Arial;background:#111;color:#fff;margin:0;padding:20px}.box{max-width:650px;margin:auto;background:#1d1d1d;padding:22px;border-radius:15px}select,input,button{width:100%;padding:14px;margin:7px 0;border-radius:9px;border:0;font-size:16px;box-sizing:border-box}button{cursor:pointer;font-weight:bold}.row{display:flex;gap:10px}.row button{width:50%}.call{background:#16834b;color:#fff}.put{background:#b52b35;color:#fff}.test{background:#ddd;color:#111}.status{background:#292929;padding:15px;border-radius:10px;margin:15px 0;line-height:1.7}pre{white-space:pre-wrap;word-break:break-word;background:#090909;padding:12px;border-radius:8px;font-size:12px}</style></head><body><div class="box"><h1>Teste de Entrada DEMO</h1><p>V4: este app ignora completamente a estratégia. Ele testa SOMENTE a abertura de uma ordem DEMO e investiga o erro 4104 variando a expiração.</p><div class="status" id="status">Carregando...</div><label>Ativo</label><select id="symbol"><option>EUR/USD</option><option>EUR/JPY</option><option>GBP/USD</option><option>USD/JPY</option><option>GBP/JPY</option></select><label>Valor</label><input id="amount" type="number" min="0.01" max="100" step="0.01" value="5.00"><div class="row"><button class="call" onclick="test('CALL')">FORÇAR CALL</button><button class="put" onclick="test('PUT')">FORÇAR PUT</button></div><div class="status" id="result">Aguardando teste.</div></div><script>async function status(){try{let r=await fetch('/status');let d=await r.json();document.getElementById('status').innerHTML='WebSocket: <b>'+d.websocket+'</b><br>Autenticado: <b>'+d.authenticated+'</b><br>Balance DEMO: <b>'+d.balance+'</b><br>Fonte: <b>'+d.balance_source+'</b><br>Último erro: <b>'+((d.error)||'-')+'</b>';}catch(e){document.getElementById('status').textContent=e}}async function test(direction){let symbol=document.getElementById('symbol').value;let amount=document.getElementById('amount').value;document.getElementById('result').textContent='Iniciando teste '+symbol+' '+direction+'...';try{let r=await fetch('/teste/entrada',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol,direction,amount})});let d=await r.json();document.getElementById('result').innerHTML='<pre>'+JSON.stringify(d,null,2)+'</pre>';status();if(r.status===202){poll();}}catch(e){document.getElementById('result').textContent=e}}async function poll(){for(let i=0;i<90;i++){await new Promise(x=>setTimeout(x,1000));try{let r=await fetch('/status');let d=await r.json();if(d.last_test){document.getElementById('result').innerHTML='<pre>'+JSON.stringify(d.last_test,null,2)+'</pre>';}status();if(!d.test_running)return;}catch(e){}}}status();setInterval(status,5000)</script></body></html>'''


@app.route('/')
def index():
    start_ws()
    return render_template_string(HTML)


@app.route('/status')
def status():
    return jsonify({
        'mode': 'DEMO_ONLY_5M',
        'diagnostico': 'EXPIRACAO_4104',
        'websocket': 'CONECTADO' if _connected else 'DESCONECTADO',
        'authenticated': _authenticated,
        'balance': 'ENCONTRADO' if _balance_id else 'AGUARDANDO',
        'balance_source': _balance_source,
        'last_test': _last_test,
        'test_running': _test_running,
        'error': _last_error,
    })


@app.route('/teste/entrada', methods=['POST'])
def teste_entrada():
    # IMPORTANTE: não bloqueamos o worker do Gunicorn esperando a Bullex.
    # A operação é executada em uma thread e o navegador acompanha por /status.
    global _test_running, _last_test, _last_error
    try:
        data = request.get_json(silent=True) or {}
        symbol = str(data.get('symbol', 'EUR/JPY'))
        direction = str(data.get('direction', 'CALL')).upper()
        amount = float(data.get('amount', VALOR_PADRAO))

        if symbol not in ATIVOS:
            return jsonify({'confirmada': False, 'erro': 'Ativo inválido.', 'modo': 'DEMO_ONLY'}), 400
        if direction not in ('CALL', 'PUT'):
            return jsonify({'confirmada': False, 'erro': 'Direção deve ser CALL ou PUT.', 'modo': 'DEMO_ONLY'}), 400
        if amount <= 0 or amount > 100:
            return jsonify({'confirmada': False, 'erro': 'Valor deve estar entre R$0,01 e R$100,00.', 'modo': 'DEMO_ONLY'}), 400

        if _test_running:
            return jsonify({'confirmada': False, 'em_andamento': True, 'erro': 'Já existe um teste em andamento.', 'modo': 'DEMO_ONLY'}), 409

        _test_running = True
        _last_error = None
        _last_test = {
            'em_andamento': True,
            'confirmada': False,
            'symbol': symbol,
            'direction': direction,
            'amount': amount,
            'modo': 'DEMO_ONLY',
        }

        def worker():
            global _test_running, _last_test, _last_error
            try:
                result = force_order(symbol, direction, amount)
                _last_test = result
            except Exception as e:
                _last_error = str(e)
                _last_test = {
                    'em_andamento': False,
                    'confirmada': False,
                    'symbol': symbol,
                    'direction': direction,
                    'amount': amount,
                    'modo': 'DEMO_ONLY',
                    'erro': str(e),
                }
                log(f'ERRO NO TESTE: {e}')
            finally:
                _test_running = False
                if isinstance(_last_test, dict):
                    _last_test['em_andamento'] = False

        threading.Thread(target=worker, daemon=True, name='bullex-force-order').start()
        return jsonify(_last_test), 202
    except Exception as e:
        _test_running = False
        _last_error = str(e)
        log(f'ERRO AO INICIAR TESTE: {e}')
        return jsonify({'confirmada': False, 'erro': str(e), 'modo': 'DEMO_ONLY'}), 400


@app.route('/health')
def health():
    return jsonify({'status':'ok','mode':'DEMO_ONLY_5M','diagnostico':'EXPIRACAO_4104','websocket':_connected,'authenticated':_authenticated,'balance':bool(_balance_id),'last_test':_last_test,'test_running':_test_running,'error':_last_error})


if __name__ == '__main__':
    start_ws()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT','10000')), debug=False)
