import requests
import hmac
import hashlib
import time
import logging
from flask import Flask, jsonify

# Вставьте здесь свои API ключи
API_KEYS = {
    "binance": {
        "api_key": "8VDR1MTbmBJ7YwFNFECLx09X8sYXylC9xLZDsJ8Ttycd4pEWTPN0Cp1LObowUFMq",
        "secret_key": "waiHbMBDjNMpTkSkSOdtQ6QU10K8JnYaSI0JWnOgsBJqzblzfY2bIQhI8kw8NJZo"
    },
    "bybit": {
        "api_key": "y2SaGwKlrZLLLbUn9L",
        "secret_key": "FDJ694XZ5TGq7QXUPSvXtDuUARwVBoSIEcZh"
    },
    "mexc": {
        "api_key": "mx0vgldWYinbJMpC4g",
        "secret_key": "7f35ddbdef714a0aa08f7835bb518b33"
    },
    "htx": {
        "api_key": "79229925-vftwcr5tnh-4b1d9994-18b2a",
        "secret_key": "cabb7272-6e25b1a4-4918ad3e-b9dc5"
    },
    "kucoin": {
        "api_key": "66a62049fef1a30001b8d97c",
        "secret_key": "7c4ea82e-7595-49ce-8024-a503731c1cbd"
    }
}

BASE_URL_BINANCE = 'https://api.binance.com'
BASE_URL_HTX = 'https://api.huobi.pro'
BASE_URL_KUCOIN = 'https://api.kucoin.com'

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Функции для получения данных с бирж
def get_server_time_binance():
    url = f'{BASE_URL_BINANCE}/api/v3/time'
    response = requests.get(url)
    return response.json()['serverTime']

def get_signature(api_secret: str, params: dict) -> str:
    query_string = '&'.join([f"{key}={value}" for key, value in sorted(params.items())])
    return hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

def get_signature_binance(query_string: str) -> str:
    return hmac.new(
        API_KEYS["binance"]["secret_key"].encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

def get_withdraw_fees_binance() -> dict:
    endpoint = '/sapi/v1/capital/config/getall'
    timestamp = get_server_time_binance()
    query_string = f'timestamp={timestamp}'
    signature = get_signature_binance(query_string)
    headers = {
        'X-MBX-APIKEY': API_KEYS["binance"]["api_key"]
    }
    url = f'{BASE_URL_BINANCE}{endpoint}?{query_string}&signature={signature}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return {}

def get_binance_prices() -> dict:
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching Binance prices: {e}")
        return {}

    prices = {}
    for ticker in data:
        if ticker['symbol'].endswith('USDT'):
            symbol = ticker['symbol']
            prices[symbol] = {
                'price': float(ticker['lastPrice']),
                'volume': float(ticker['quoteVolume'])
            }
            logging.info(f"Binance {symbol}: Price={ticker['lastPrice']}, Volume={ticker['quoteVolume']}")

    return prices

def get_bybit_prices_with_retry(max_retries=3, delay=5) -> dict:
    url = "https://api.bybit.com/v5/market/tickers"
    params = {
        'category': 'spot'
    }
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            prices = {}
            if data.get("result") and "list" in data["result"]:
                for ticker in data["result"]["list"]:
                    if ticker['symbol'].endswith('USDT'):
                        symbol = ticker['symbol']
                        prices[symbol] = {
                            'price': float(ticker['lastPrice']),
                            'volume': float(ticker['volume24h'])
                        }
                        logging.info(f"Bybit {symbol}: Price={ticker['lastPrice']}, Volume={ticker['volume24h']}")
            return prices
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("Error fetching Bybit prices: Too Many Requests. Retrying...")
                time.sleep(delay)
            else:
                print(f"Error fetching Bybit prices: {e}")
                return {}
        except (requests.RequestException, ValueError) as e:
            print(f"Error fetching Bybit prices: {e}")
            return {}
    print("Failed to fetch Bybit prices after several retries.")
    return {}

def get_bybit_withdraw_fees() -> dict:
    endpoint = '/v5/asset/coin/query-info'
    params = {
        'api_key': API_KEYS["bybit"]["api_key"],
        'timestamp': str(int(time.time() * 1000)),
        'recv_window': '5000'
    }
    params['sign'] = get_signature(API_KEYS["bybit"]["secret_key"], params)

    url = f'https://api.bybit.com{endpoint}'
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        fees = {}
        if data and 'result' in data and 'rows' in data['result']:
            for coin in data['result']['rows']:
                currency = coin['coin']
                for chain_info in coin['chains']:
                    symbol = f"{currency}USDT"
                    if symbol not in fees:
                        fees[symbol] = []
                    chain_withdraw = chain_info.get('chainWithdraw', 0)
                    withdraw_enabled = int(chain_withdraw) == 1 if chain_withdraw and chain_withdraw.isdigit() else False
                    chain_deposit = chain_info.get('chainDeposit', 0)
                    deposit_enabled = int(chain_deposit) == 1 if chain_deposit and chain_deposit.isdigit() else False
                    fees[symbol].append({
                        "chainType": chain_info.get('chainType', 'N/A'),  # Изменено с 'chain' на 'chainType'
                        "fee": chain_info.get('withdrawFee', 'N/A'),
                        "withdraw_enabled": withdraw_enabled,
                        "deposit_enabled": deposit_enabled
                    })
        return fees
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching Bybit withdrawal fees: {e}")
        return {}

def get_mexc_prices() -> dict:
    try:
        url = "https://www.mexc.com/open/api/v2/market/ticker"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching MEXC prices: {e}")
        return {}

    prices = {}
    if data.get("data"):
        for ticker in data["data"]:
            if ticker['symbol'].endswith('USDT'):
                symbol = ticker['symbol']
                prices[symbol] = {
                    'price': float(ticker['last']),
                    'volume': float(ticker['volume'])
                }
                logging.info(f"MEXC {symbol}: Price={ticker['last']}, Volume={ticker['volume']}")

    return prices

def get_mexc_withdraw_fees() -> dict:
    url = "https://www.mexc.com/open/api/v2/market/coin/list"
    headers = {
        'Content-Type': 'application/json',
        'ApiKey': API_KEYS["mexc"]["api_key"]
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching MEXC withdrawal fees: {e}")
        return {}

    fees = {}
    if data.get("data"):
        for coin in data["data"]:
            currency = coin["currency"]
            for chain_info in coin["coins"]:
                chain = chain_info["chain"]
                fee = chain_info["fee"]
                withdraw_enabled = int(chain_info.get("is_withdraw_enabled", 0)) == 1
                deposit_enabled = int(chain_info.get("is_deposit_enabled", 0)) == 1
                if currency not in fees:
                    fees[currency] = []
                fees[currency].append({
                    "chain": chain,
                    "fee": fee,
                    "withdraw_enabled": withdraw_enabled,
                    "deposit_enabled": deposit_enabled
                })
    return fees

def get_htx_prices() -> dict:
    try:
        url = f"{BASE_URL_HTX}/market/tickers"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching HTX prices: {e}")
        return {}

    prices = {}
    if data.get("data"):
        for ticker in data["data"]:
            if ticker['symbol'].endswith('usdt'):
                symbol = ticker['symbol'].upper()
                prices[symbol] = {
                    'price': float(ticker['close']),
                    'volume': float(ticker['vol'])
                }
                logging.info(f"HTX {symbol}: Price={ticker['close']}, Volume={ticker['vol']}")

    return prices

def get_htx_withdraw_fees() -> dict:
    endpoint = '/v2/reference/currencies'
    params = {
        'currency': '',  # Для получения информации по всем валютам
        'authorizedUser': 'true'
    }
    url = f"{BASE_URL_HTX}{endpoint}"
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching HTX withdrawal fees: {e}")
        return {}
    fees = {}
    if data.get('data'):
        for fee_info in data['data']:
            currency = fee_info['currency'].upper()
            for chain_info in fee_info['chains']:
                symbol = f"{currency}USDT"
                if symbol not in fees:
                    fees[symbol] = []
                fees[symbol].append({
                    "chain": chain_info.get('chain', 'N/A'),
                    "fee": chain_info.get('transactFeeWithdraw', 'N/A'),
                    "withdraw_enabled": chain_info.get('withdrawStatus') == 'allowed',
                    "deposit_enabled": chain_info.get('depositStatus') == 'allowed'
                })
    return fees

def get_signature_kucoin(endpoint: str, method: str, params: str) -> dict:
    now = int(time.time() * 1000)
    str_to_sign = str(now) + method + endpoint + params
    signature = hmac.new(API_KEYS["kucoin"]["secret_key"].encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

    headers = {
        "KC-API-KEY": API_KEYS["kucoin"]["api_key"],
        "KC-API-SIGN": signature,
        "KC-API-TIMESTAMP": str(now),
        "KC-API-KEY-VERSION": "2"
    }
    return headers

def get_kucoin_prices() -> dict:
    try:
        url = f"{BASE_URL_KUCOIN}/api/v1/market/allTickers"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Error fetching KuCoin prices: {e}")
        return {}

    prices = {}
    if data.get("data") and data["data"].get("ticker"):
        for ticker in data["data"]["ticker"]:
            if ticker['symbol'].endswith('-USDT'):
                symbol = ticker['symbol'].replace('-', '')
                prices[symbol] = {
                    'price': float(ticker['last']) if ticker['last'] else 0.0,
                    'volume': float(ticker['volValue']) if ticker['volValue'] else 0.0
                }
                logging.info(f"KuCoin {symbol}: Price={ticker['last']}, Volume={ticker['volValue']}")

    return prices

def get_kucoin_withdraw_fees() -> dict:
    url = f"{BASE_URL_KUCOIN}/api/v3/currencies"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Error fetching KuCoin withdrawal fees: {e}")
        return {}

    fees = {}
    if data.get('data'):
        for coin in data['data']:
            currency = coin['currency']
            if 'chains' in coin and coin['chains']:
                fees[currency] = []
                for chain_info in coin['chains']:
                    fees[currency].append({
                        "chain": chain_info.get('chainName', 'N/A'),
                        "fee": chain_info.get('withdrawalMinFee', 'N/A'),
                        "withdraw_enabled": chain_info.get('isWithdrawEnabled', False),
                        "deposit_enabled": chain_info.get('isDepositEnabled', False)
                    })
            else:
                logging.info(f"No chains info for currency: {currency}")
    return fees

# Создаем отдельные Flask серверы для каждой биржи
def create_binance_app():
    app = Flask(__name__)

    @app.route('/prices', methods=['GET'])
    def get_prices_route_binance():
        prices = {}
        withdraw_fees = {}

        # Получение цен для Binance
        prices['binance'] = get_binance_prices()
        withdraw_fees['binance'] = get_withdraw_fees_binance()

        # Добавление комиссий и проверки возможности вывода для Binance
        for symbol in prices['binance']:
            currency = symbol.replace("USDT", "")
            fees_info = []
            withdraw_possible = False
            deposit_possible = False
            for fee_info in withdraw_fees['binance']:
                if fee_info['coin'] == currency:
                    for network in fee_info['networkList']:
                        if network['coin'] == currency:
                            fees_info.append({
                                "chain": network['name'],
                                "fee": network['withdrawFee'],
                                "withdraw_enabled": network['withdrawEnable'],
                                "deposit_enabled": network['depositEnable']
                            })
                            if network['withdrawEnable']:
                                withdraw_possible = True
                            if network['depositEnable']:
                                deposit_possible = True
                    break
            prices['binance'][symbol] = {
                "price": prices['binance'][symbol]['price'],
                "volume": prices['binance'][symbol]['volume'],
                "withdraw_fees": fees_info,
                "withdraw_possible": withdraw_possible,
                "deposit_possible": deposit_possible,
                "exchange": "binance"
            }

        return jsonify(prices) if prices else (jsonify({}), 500)

    return app

def create_bybit_app():
    app = Flask(__name__)

    @app.route('/prices', methods=['GET'])
    def get_prices_route_bybit():
        prices = {}
        withdraw_fees = {}

        # Получение цен для Bybit
        prices['bybit'] = get_bybit_prices_with_retry()
        withdraw_fees['bybit'] = get_bybit_withdraw_fees()

        # Добавление комиссий и проверки возможности вывода для Bybit
        for symbol in prices['bybit']:
            if symbol in withdraw_fees['bybit']:
                fee_info = withdraw_fees['bybit'][symbol]
                if isinstance(fee_info, list):
                    withdraw_possible = any(fee['withdraw_enabled'] for fee in fee_info)
                    deposit_possible = any(fee['deposit_enabled'] for fee in fee_info)
                    prices['bybit'][symbol] = {
                        "price": prices['bybit'][symbol]['price'],
                        "volume": prices['bybit'][symbol]['volume'],
                        "withdraw_fees": fee_info,
                        "withdraw_possible": withdraw_possible,
                        "deposit_possible": deposit_possible,
                        "exchange": "bybit"
                    }
                else:
                    prices['bybit'][symbol] = {
                        "price": prices['bybit'][symbol]['price'],
                        "volume": prices['bybit'][symbol]['volume'],
                        "withdraw_fees": "N/A",
                        "withdraw_possible": False,
                        "deposit_possible": False,
                        "exchange": "bybit"
                    }
            else:
                prices['bybit'][symbol] = {
                    "price": prices['bybit'][symbol]['price'],
                    "volume": prices['bybit'][symbol]['volume'],
                    "withdraw_fees": "N/A",
                    "withdraw_possible": False,
                    "deposit_possible": False,
                    "exchange": "bybit"
                }

        return jsonify(prices) if prices else (jsonify({}), 500)

    return app

def create_mexc_app():
    app = Flask(__name__)

    @app.route('/prices', methods=['GET'])
    def get_prices_route_mexc():
        prices = {}
        withdraw_fees = {}

        # Получение цен для MEXC
        prices['mexc'] = get_mexc_prices()
        withdraw_fees['mexc'] = get_mexc_withdraw_fees()

        # Добавление комиссий и проверки возможности вывода для MEXC
        for symbol in prices['mexc']:
            currency = symbol.replace("_USDT", "")
            if currency in withdraw_fees['mexc']:
                fee_info = withdraw_fees['mexc'][currency]
                if isinstance(fee_info, list):
                    withdraw_possible = any(fee['withdraw_enabled'] for fee in fee_info)
                    deposit_possible = any(fee['deposit_enabled'] for fee in fee_info)
                    prices['mexc'][symbol] = {
                        "price": prices['mexc'][symbol]['price'],
                        "volume": prices['mexc'][symbol]['volume'],
                        "withdraw_fees": fee_info,
                        "withdraw_possible": withdraw_possible,
                        "deposit_possible": deposit_possible,
                        "exchange": "mexc"
                    }
                else:
                    prices['mexc'][symbol] = {
                        "price": prices['mexc'][symbol]['price'],
                        "volume": prices['mexc'][symbol]['volume'],
                        "withdraw_fees": "N/A",
                        "withdraw_possible": False,
                        "deposit_possible": False,
                        "exchange": "mexc"
                    }
            else:
                prices['mexc'][symbol] = {
                    "price": prices['mexc'][symbol]['price'],
                    "volume": prices['mexc'][symbol]['volume'],
                    "withdraw_fees": "N/A",
                    "withdraw_possible": False,
                    "deposit_possible": False,
                    "exchange": "mexc"
                }

        return jsonify(prices) if prices else (jsonify({}), 500)

    return app

def create_htx_app():
    app = Flask(__name__)

    @app.route('/prices', methods=['GET'])
    def get_prices_route_htx():
        prices = {}
        withdraw_fees = {}

        # Получение цен для HTX
        prices['htx'] = get_htx_prices()
        withdraw_fees['htx'] = get_htx_withdraw_fees()

        # Добавление комиссий и проверки возможности вывода для HTX
        for symbol in prices['htx']:
            if symbol in withdraw_fees['htx']:
                fee_info = withdraw_fees['htx'][symbol]
                if isinstance(fee_info, list):
                    withdraw_possible = any(fee['withdraw_enabled'] for fee in fee_info)
                    deposit_possible = any(fee['deposit_enabled'] for fee in fee_info)
                    prices['htx'][symbol] = {
                        "price": prices['htx'][symbol]['price'],
                        "volume": prices['htx'][symbol]['volume'],
                        "withdraw_fees": fee_info,
                        "withdraw_possible": withdraw_possible,
                        "deposit_possible": deposit_possible,
                        "exchange": "htx"
                    }
                else:
                    prices['htx'][symbol] = {
                        "price": prices['htx'][symbol]['price'],
                        "volume": prices['htx'][symbol]['volume'],
                        "withdraw_fees": "N/A",
                        "withdraw_possible": False,
                        "deposit_possible": False,
                        "exchange": "htx"
                    }
            else:
                prices['htx'][symbol] = {
                    "price": prices['htx'][symbol]['price'],
                    "volume": prices['htx'][symbol]['volume'],
                    "withdraw_fees": "N/A",
                    "withdraw_possible": False,
                    "deposit_possible": False,
                    "exchange": "htx"
                }

        return jsonify(prices) if prices else (jsonify({}), 500)

    return app

def create_kucoin_app():
    app = Flask(__name__)

    @app.route('/prices', methods=['GET'])
    def get_prices_route_kucoin():
        prices = {}
        withdraw_fees = {}

        # Получение цен для KuCoin
        prices['kucoin'] = get_kucoin_prices()
        withdraw_fees['kucoin'] = get_kucoin_withdraw_fees()

        # Добавление комиссий и проверки возможности вывода для KuCoin
        for symbol in prices['kucoin']:
            currency = symbol.replace("USDT", "")
            if currency in withdraw_fees['kucoin']:
                fee_info = withdraw_fees['kucoin'][currency]
                withdraw_possible = any(fee['withdraw_enabled'] for fee in fee_info)
                deposit_possible = any(fee['deposit_enabled'] for fee in fee_info)
                prices['kucoin'][symbol] = {
                    "price": prices['kucoin'][symbol]['price'],
                    "volume": prices['kucoin'][symbol]['volume'],
                    "withdraw_fees": fee_info,
                    "withdraw_possible": withdraw_possible,
                    "deposit_possible": deposit_possible,
                    "exchange": "kucoin"
                }
            else:
                prices['kucoin'][symbol] = {
                    "price": prices['kucoin'][symbol]['price'],
                    "volume": prices['kucoin'][symbol]['volume'],
                    "withdraw_fees": [],
                    "withdraw_possible": False,
                    "deposit_possible": False,
                    "exchange": "kucoin"
                }

        return jsonify(prices) if prices else (jsonify({}), 500)

    return app

if __name__ == '__main__':
    # Запускаем серверы на разных портах
    binance_app = create_binance_app()
    bybit_app = create_bybit_app()
    mexc_app = create_mexc_app()
    htx_app = create_htx_app()
    kucoin_app = create_kucoin_app()

    from threading import Thread

    # Создаем отдельные потоки для каждого приложения
    Thread(target=lambda: binance_app.run(host='0.0.0.0', port=8001)).start()
    Thread(target=lambda: bybit_app.run(host='0.0.0.0', port=8002)).start()
    Thread(target=lambda: mexc_app.run(host='0.0.0.0', port=8000)).start()
    Thread(target=lambda: htx_app.run(host='0.0.0.0', port=8004)).start()
    Thread(target=lambda: kucoin_app.run(host='0.0.0.0', port=8005)).start()