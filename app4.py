import numpy as np
import requests
import logging
import aiohttp
import asyncio
import time
from telegram import Bot
import ccxt.pro as ccxtpro

# Telegram настройки
TELEGRAM_BOT_TOKEN = '7196660803:AAGg8-LGgDqrCLbYSfo8xrII008mRV7GFZY'
CHANNEL_ID = '-1002174617061'

# Параметры
MIN_SPREAD = 0.9  # Минимальный спред в процентах
MAX_SPREAD = 8.0  # Максимальный спред в процентах
MIN_TRADE_AMOUNT = 500  # Минимальный торговый объем и сумма для проверки ликвидности
MIN_VOLUME = 300000  # Минимальный суточный объем
SPREAD_THRESHOLD = 0.01  # Пороговое значение для изменения спреда
PRICE_RANGE_PERCENT = 0.30  # Диапазон цены в процентах для проверки ликвидности
MIN_PROFIT = 0.00  # Минимальная прибыль для отправки сигнала
LIQUIDITY_CHECK_TIMES = 1  # Количество проверок ликвидности
LIQUIDITY_CHECK_SLEEP = 0.3  # Пауза между проверками ликвидности
TASK_BATCH_SIZE = 15  # Определяет, сколько сигналов будет обрабатываться за один блок


# Список URL Flask-серверов
FLASK_SERVER_URLS = [
    "http://localhost:8001/prices",  # Binance сервер
    "http://localhost:8002/prices",  # Bybit сервер
    "http://localhost:8000/prices",  # MEXC сервер
    "http://localhost:8004/prices",  # HTX сервер
    "http://localhost:8005/prices"  # KuCoin сервер
]

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', encoding='utf-8')

# Инициализация клиентов для бирж
exchanges = {
    "binance": ccxtpro.binance(),
    "bybit": ccxtpro.bybit(),
    "mexc": ccxtpro.mexc(),
    "kucoin": ccxtpro.kucoin(),
    "htx": ccxtpro.huobi()
}

# Глобальные переменные для хранения ордербуков
order_books = {
    "binance": None,
    "bybit": None,
    "mexc": None,
    "kucoin": None,
    "htx": None
}


async def process_opportunity(opp):
    """Проверка ликвидности, актуализация спреда и отправка сигнала для одной возможности"""
    # Подключение и проверка ликвидности
    has_liquidity, dynamic_trade_amount = await check_liquidity_custom(
        opp['buy_exchange'], opp['sell_exchange'], opp['symbol'], PRICE_RANGE_PERCENT, None, None
    )

    # Если ликвидность достаточна
    if has_liquidity:
        opp['dynamic_trade_amount'] = dynamic_trade_amount

        # Актуализация спреда
        updated_opportunity = await update_single_spread(opp, SPREAD_THRESHOLD, MIN_SPREAD)

        # Если спред после актуализации достаточен, отправка сигнала
        if updated_opportunity:
            bot = create_bot(TELEGRAM_BOT_TOKEN)
            await send_signal(bot, [updated_opportunity])
            logging.info(
                f"Сигнал отправлен для {updated_opportunity['symbol']} с {updated_opportunity['buy_exchange']} на {updated_opportunity['sell_exchange']}."
            )
        else:
            logging.info(f"Спред для {opp['symbol']} недостаточен после актуализации, сигнал не отправляется.")
    else:
        logging.info(
            f"Ликвидность недостаточна для {opp['symbol']} между {opp['buy_exchange']} и {opp['sell_exchange']}.")


# Создаем объект Bot без указания кастомного Request
def create_bot(token):
    return Bot(token=token)


# Функция для отправки сообщений в Telegram с задержкой
async def send_signal(bot, opportunities):
    logging.info("Отправка сигналов в Telegram.")
    for opp in opportunities:
        await send_single_signal(bot, opp)
        await asyncio.sleep(0.5)  # Задержка в полсекунды между отправками


async def send_single_signal(bot, opp):
    buy_url = generate_exchange_url(opp['buy_exchange'], opp['symbol'])
    sell_url = generate_exchange_url(opp['sell_exchange'], opp['symbol'])
    buy_price = opp.get('buy_price', 0)
    sell_price = opp.get('sell_price', 0)
    dynamic_trade_amount = opp.get('dynamic_trade_amount', MIN_TRADE_AMOUNT)

    if buy_price == 0 or sell_price == 0:
        logging.warning(
            f"Пропуск сигнала с некорректными ценами для {opp['symbol']} на {opp['buy_exchange']} и {opp['sell_exchange']}")
        return

    withdraw_fee_usd = float(opp.get('withdraw_fee', 0)) * buy_price
    profit_amount_usdt = ((dynamic_trade_amount / buy_price) * sell_price) - dynamic_trade_amount - withdraw_fee_usd

    if profit_amount_usdt <= MIN_PROFIT:
        logging.info(
            f"Прибыль {profit_amount_usdt:.2f} USDT ниже минимальной {MIN_PROFIT:.2f} USDT для {opp['symbol']}, сигнал не отправляется.")
        return

    spread_display = opp['spread_display']

    message = (
        f"💵 Покупка <b>{opp['symbol']}</b>\n"
        f"Объём: {dynamic_trade_amount} USDT -> {(dynamic_trade_amount / buy_price):.2f} {opp['symbol']}\n"
        f"Цена: {buy_price:.8f} USDT\n"
        f"Биржа: <a href='{buy_url}'>{opp['buy_exchange'].upper()}</a>\n\n"
        f"💰 Продажа\n"
        f"Объём: {(dynamic_trade_amount / buy_price):.2f} {opp['symbol']} -> {((dynamic_trade_amount / buy_price) * sell_price):.2f} USDT\n"
        f"Цена: {sell_price:.8f} USDT\n"
        f"Биржа: <a href='{sell_url}'>{opp['sell_exchange'].upper()}</a>\n\n"
        f"📈 Профит: {profit_amount_usdt:.3f} USDT\n"
        f"Спред: {spread_display:.2f}%\n\n"
        f"💸 Комиссия за вывод: {withdraw_fee_usd:.3f} USDT\n"
        f"🔗 Сеть: {opp['network']}\n"
    )

    try:
        await bot.send_message(chat_id=CHANNEL_ID, text=message, parse_mode='HTML', disable_web_page_preview=True)
        logging.info(f"Сообщение отправлено для {opp['symbol']} с {opp['buy_exchange']} на {opp['sell_exchange']}.")
    except Exception as e:
        logging.error(f"Не удалось отправить сообщение в канал: {CHANNEL_ID}, ошибка: {e}")


# Функция подключения к веб-сокетам для получения ордербуков
async def connect_selected_websockets(symbol, buy_exchange, sell_exchange):
    """Подключение только к выбранным веб-сокетам бирж и получение ордербуков для проверки"""
    formatted_symbol_buy = format_symbol(buy_exchange, symbol)
    formatted_symbol_sell = format_symbol(sell_exchange, symbol)

    buy_order_book, sell_order_book = await asyncio.gather(
        fetch_order_book(buy_exchange, formatted_symbol_buy),
        fetch_order_book(sell_exchange, formatted_symbol_sell)
    )

    return buy_order_book, sell_order_book


async def fetch_order_book(exchange_name, formatted_symbol):
    """Функция для получения ордербука через WebSocket"""
    exchange = exchanges[exchange_name]
    try:
        order_book = await exchange.watch_order_book(formatted_symbol)
        if order_book is None or 'bids' not in order_book or 'asks' not in order_book:
            logging.error(f"Ордербук пуст или отсутствуют данные для {formatted_symbol} на {exchange_name}")
            return {'bids': [], 'asks': []}
        else:
            logging.info(f"Получены данные ордербука для {formatted_symbol} на {exchange_name}")
            return order_book
    except Exception as e:
        logging.error(f"Ошибка при получении данных с {exchange_name}: {e}")
        return {'bids': [], 'asks': []}


def format_symbol(exchange, symbol):
    """Форматирует символы в зависимости от биржи на этапе проверки ликвидности."""
    if exchange in ["kucoin", "htx"]:
        # Убедимся, что символ корректно форматируется с одним слэшем
        return symbol.replace('USDT', '/USDT').replace('//', '/')  # Исправление
    else:
        # Для всех остальных бирж символы остаются в формате 'BTCUSDT'
        return symbol.replace('/', '').replace('_', '')

async def fetch_current_price_and_range(exchange_name, symbol, price_range_percent, is_buy):
    """
    Функция для получения текущей цены через WebSocket и построения диапазона.
    """
    # Нормализация символа в формат "BASE/QUOTE" (например, "BTC/USDT")
    if symbol[-4:] == "USDT":
        symbol = f"{symbol[:-4]}/USDT"  # Удаляем последние 4 символа и добавляем '/USDT'
    else:
        logging.error(f"Некорректный символ: {symbol}. Символ должен заканчиваться на 'USDT'.")
        return None, None, None

    exchange = exchanges[exchange_name]  # Выбор нужной биржи из инициализированного списка

    try:
        if exchange_name == "mexc":
            # Для MEXC используем K-line (свечные данные) для получения цены
            ohlcv = await exchange.watch_ohlcv(symbol, timeframe='1m')
            current_price = ohlcv[-1][4]  # Цена закрытия последней свечи
        else:
            # Для всех остальных бирж используем тикер
            ticker = await exchange.watch_ticker(symbol)
            current_price = ticker['last']

        # Проверка успешного получения текущей цены
        if current_price is None:
            logging.error(f"Не удалось получить текущую цену для {symbol} на {exchange_name}.")
            return None, None, None

        logging.info(f"Текущая цена для {symbol} на {exchange_name}: {current_price:.8f} USDT")

        # Построение диапазона на основе типа сделки (покупка или продажа)
        if is_buy:
            lower_bound = current_price
            upper_bound = current_price * (1 + price_range_percent / 100)
        else:
            lower_bound = current_price * (1 - price_range_percent / 100)
            upper_bound = current_price

        logging.info(
            f"Диапазон цены для {exchange_name} ({'покупка' if is_buy else 'продажа'}): {lower_bound:.8f} - {upper_bound:.8f} USDT")

        return current_price, lower_bound, upper_bound

    except Exception as e:
        logging.error(f"Ошибка при получении данных с {exchange_name}: {e}")
        return None, None, None


async def fetch_liquidity(exchange, symbol, current_price, liquidity_amount, is_buy, price_range_percent, order_book):
    """Функция для проверки ликвидности на бирже с использованием переданного ордербука."""
    lower_bound, upper_bound = (
        (current_price * (1 - price_range_percent / 100), current_price) if not is_buy
        else (current_price, current_price * (1 + price_range_percent / 100))
    )

    total_volume = 0
    highest_price = lowest_price = 0

    if not is_buy:
        bids = [bid for bid in order_book['bids'] if lower_bound <= float(bid[0]) <= upper_bound]
        total_volume = sum(float(bid[1]) * float(bid[0]) for bid in bids)
        highest_price = float(bids[0][0]) if bids else 0.0
        result = total_volume >= liquidity_amount
    else:
        asks = [ask for ask in order_book['asks'] if lower_bound <= float(ask[0]) <= upper_bound]
        total_volume = sum(float(ask[1]) * float(ask[0]) for ask in asks)
        lowest_price = float(asks[0][0]) if asks else 0.0
        result = total_volume >= liquidity_amount

    return result, total_volume

# Новая функция для проверки ликвидности
async def check_liquidity_custom(buy_exchange, sell_exchange, symbol, price_range_percent, buy_order_book, sell_order_book):
    """Проверка ликвидности на указанных биржах с использованием ордербуков после запроса цены и построения диапазонов."""
    available_buy_volume_list = []
    available_sell_volume_list = []

    for _ in range(LIQUIDITY_CHECK_TIMES):
        # Получение текущей цены и диапазона для покупки и продажи
        buy_price, buy_lower, buy_upper = await fetch_current_price_and_range(buy_exchange, symbol, price_range_percent, is_buy=True)
        sell_price, sell_lower, sell_upper = await fetch_current_price_and_range(sell_exchange, symbol, price_range_percent, is_buy=False)

        if not all([buy_price, sell_price]):
            logging.warning(f"Цены не получены для {symbol} на {buy_exchange} или {sell_exchange}. Пропуск текущей итерации.")
            continue

        # Немедленно запрашиваем ордербуки после получения актуальной цены и диапазона
        buy_order_book = await fetch_order_book(buy_exchange, symbol)
        sell_order_book = await fetch_order_book(sell_exchange, symbol)

        # Проверка ликвидности на покупку
        if buy_order_book:
            buy_liquidity, buy_volume = await fetch_liquidity(
                buy_exchange, symbol, buy_price, MIN_TRADE_AMOUNT, is_buy=True,
                price_range_percent=price_range_percent, order_book=buy_order_book
            )
            if buy_liquidity:
                available_buy_volume_list.append(buy_volume)
            log_liquidity_check(
                symbol, buy_lower, buy_upper, buy_volume, buy_order_book['asks'][0][0] if buy_order_book['asks'] else 0,
                buy_order_book['asks'][-1][0] if buy_order_book['asks'] else 0, buy_exchange,
                buy_liquidity, MIN_TRADE_AMOUNT, 'buy', buy_price
            )

        # Проверка ликвидности на продажу
        if sell_order_book:
            sell_liquidity, sell_volume = await fetch_liquidity(
                sell_exchange, symbol, sell_price, MIN_TRADE_AMOUNT, is_buy=False,
                price_range_percent=price_range_percent, order_book=sell_order_book
            )
            if sell_liquidity:
                available_sell_volume_list.append(sell_volume)
            log_liquidity_check(
                symbol, sell_lower, sell_upper, sell_volume, sell_order_book['bids'][0][0] if sell_order_book['bids'] else 0,
                sell_order_book['bids'][-1][0] if sell_order_book['bids'] else 0, sell_exchange,
                sell_liquidity, MIN_TRADE_AMOUNT, 'sell', sell_price
            )

    # Расчёт медианных значений после всех циклов
    median_buy_volume = np.median(available_buy_volume_list) if available_buy_volume_list else 0
    median_sell_volume = np.median(available_sell_volume_list) if available_sell_volume_list else 0

    # Проверка, достаточно ли объёмов для торговли
    buy_liquidity = median_buy_volume >= MIN_TRADE_AMOUNT
    sell_liquidity = median_sell_volume >= MIN_TRADE_AMOUNT
    has_liquidity = buy_liquidity and sell_liquidity

    # Рассчёт динамического объёма для торговли
    dynamic_trade_volume = min(median_buy_volume, median_sell_volume) * 0.9 if has_liquidity else MIN_TRADE_AMOUNT
    logging.info(f"Ликвидность проверена для {symbol} на {buy_exchange} и {sell_exchange}. Результат: {'достаточно' if has_liquidity else 'недостаточно'}")
    return has_liquidity, dynamic_trade_volume



def log_liquidity_check(symbol, lower_bound, upper_bound, median_volume, highest_price, lowest_price, exchange, result,
                        liquidity_amount, order_type, current_price):
    """Логирование результата проверки ликвидности"""
    logging.info(f"Symbol: {symbol} ({exchange})")
    logging.info(f"Current Price: {current_price:.8f} USDT")
    logging.info(f"Price Range: {lower_bound:.8f} - {upper_bound:.8f} USDT")
    logging.info(f"Highest {order_type}: {highest_price:.8f} USDT")
    logging.info(f"Lowest {order_type}: {lowest_price:.8f} USDT")
    logging.info(
        f"Total Medium {order_type} Volume (USDT): {median_volume:.2f} (Threshold: {liquidity_amount:.2f} USDT)")

    if result:
        logging.info(f"Ликвидность для {symbol} на {exchange} достаточна. Проверка ликвидности пройдена.")
    else:
        logging.info(f"Недостаточная ликвидность для {symbol} на {exchange}. Проверка ликвидности не пройдена.")


def normalize_symbol(exchange, symbol):
    """Форматирует символы в зависимости от биржи."""
    if exchange in ["kucoin", "htx"]:
        # Для KuCoin и HTX символы должны быть в формате 'BASE/QUOTE', без лишних подчеркиваний
        return symbol.replace('_', '/').replace('USDT', '/USDT').replace('//', '/').upper()
    elif exchange in ["binance", "bybit", "mexc"]:
        # Для Binance, Bybit и MEXC символы должны быть без подчеркиваний или слэшей
        return symbol.replace('/', '').replace('_', '').upper()
    return symbol.upper()




# Генерация URL для биржи
def generate_exchange_url(exchange, symbol):
    """Генерация правильного URL для бирж на основе символа."""

    # Для Binance остается без изменений
    if exchange == "binance":
        return f"https://www.binance.com/en/trade/{symbol.replace('/', '')}?type=spot"

    # Исправление для Bybit
    elif exchange == "bybit":
        # Преобразуем символы в нужный формат с '/' между базовой валютой и котируемой
        return f"https://www.bybit.com/en/trade/spot/{symbol.replace('_', '/').replace('/', '/')}"

    # Исправление для HTX (ранее Huobi)
    elif exchange == "htx":
        # Преобразуем символы в нужный формат с '_' между базовой валютой и котируемой в нижнем регистре
        base_symbol = symbol.replace('/', '_').lower()
        return f"https://www.htx.com/ru-ru/trade/{base_symbol}?type=spot"

    # Для MEXC и KuCoin оставляем прежние форматы
    elif exchange == "mexc":
        base_symbol = symbol.replace('/', '_')
        return f"https://www.mexc.com/ru-RU/exchange/{base_symbol}?_from=search_spot_trade"

    elif exchange == "kucoin":
        base_symbol = symbol.replace('USDT', '-USDT') if 'USDT' in symbol else symbol
        return f"https://www.kucoin.com/trade/{base_symbol}"

    # Если биржа не распознана
    else:
        return "#"


# Функция для выполнения запроса к серверу с повторными попытками
async def fetch_data_with_retries(url, params=None, retries=3, delay=3, timeout=10):
    """Запрос к серверу с повторными попытками в случае ошибки."""
    for attempt in range(1, retries + 1):
        try:
            timeout_setting = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=timeout_setting) as session:
                async with session.get(url=url, params=params) as response:
                    status_code = response.status
                    data = await response.json()

            if status_code == 200:
                logging.info(f"Данные успешно получены с {url}.")
                return data
            else:
                logging.error(f"Ошибка получения данных с {url}: статус {status_code}")
        except Exception as e:
            logging.error(f"Попытка {attempt}/{retries}: ошибка при запросе к {url}: {e}")
            if attempt < retries:
                logging.info(f"Повторная попытка через {delay} секунд...")
                await asyncio.sleep(delay)
            else:
                logging.error(f"Превышено количество попыток для {url}.")
                return None

# Асинхронная функция для получения данных с нескольких Flask серверов с ограничением количества параллельных запросов
async def get_data_from_servers():
    logging.info("Получение данных с Flask серверов.")
    tasks = []

    # Ограничиваем количество параллельных запросов до 3
    semaphore = asyncio.Semaphore(3)

    async def fetch_data_with_semaphore(url):
        async with semaphore:
            return await fetch_data_with_retries(url, retries=3, delay=3, timeout=10)

    for url in FLASK_SERVER_URLS:
        tasks.append(fetch_data_with_semaphore(url))

    responses = await asyncio.gather(*tasks, return_exceptions=True)

    combined_data = {}
    for response in responses:
        if isinstance(response, Exception):
            logging.error(f"Ошибка при запросе данных: {response}")
        elif response:
            combined_data.update(response)

    return combined_data


# Функция для выполнения запроса к серверу
async def fetch_data(url, params=None):
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url=url, timeout=5, params=params) as response:
                status_code = response.status
                data = await response.json()
        if status_code == 200:
            logging.info(f"Данные успешно получены с {url}.")
            return data
        else:
            logging.error(f"Ошибка получения данных с {url}: {status_code}")
            return None
    except Exception as e:
        logging.error(f"Ошибка при запросе к {url}: {e}")
        return None

def normalize_symbol_for_comparison(symbol):
    """Нормализуем символы в формат 'BTCUSDT', удаляя разделители."""
    # Убедимся, что символ всегда заканчивается на 'USDT'
    if symbol[-4:].upper() == 'USDT':
        # Удаляем любые разделители (/, _) и приводим к верхнему регистру
        normalized_symbol = symbol.replace('/', '').replace('_', '').upper()
        return normalized_symbol
    else:
        raise ValueError(f"Некорректный символ: {symbol}. Символ должен заканчиваться на 'USDT'.")


# Функция для проверки условий и расчета прибыли
def check_and_calculate_opportunities(data, min_spread, max_spread, min_trade_amount, min_volume, require_withdraw=True, require_deposit=True):
    logging.info("Проверка возможностей арбитража.")
    opportunities = []

    for exchange1, coins1 in data.items():
        logging.info(f"Проверка монет на {exchange1}")
        for symbol1, details1 in coins1.items():
            try:
                normalized_symbol1 = normalize_symbol_for_comparison(symbol1)
            except ValueError as e:
                logging.warning(f"Ошибка нормализации символа {symbol1} на {exchange1}: {e}")
                continue

            # Фильтрация по возможности вывода и ввода
            if (not require_withdraw or details1['withdraw_possible']) and (not require_deposit or details1['deposit_possible']):
                if details1['volume'] >= min_volume:
                    for exchange2, coins2 in data.items():
                        if exchange1 != exchange2:
                            for symbol2, details2 in coins2.items():
                                try:
                                    normalized_symbol2 = normalize_symbol_for_comparison(symbol2)
                                except ValueError as e:
                                    logging.warning(f"Ошибка нормализации символа {symbol2} на {exchange2}: {e}")
                                    continue

                                if normalized_symbol1 == normalized_symbol2:
                                    logging.info(f"Сравнение {symbol1} между {exchange1} и {exchange2}")
                                    buy_price = details1['price']
                                    sell_price = details2['price']

                                    if buy_price == 0 or sell_price == 0:
                                        logging.warning(f"Цены для {symbol1} равны нулю: buy_price={buy_price}, sell_price={sell_price}")
                                        continue

                                    spread = (sell_price - buy_price) / buy_price * 100

                                    if spread < 0:
                                        logging.info(f"Спред отрицательный. Меняем местами биржи для {symbol1}.")
                                        buy_price, sell_price = sell_price, buy_price
                                        exchange1, exchange2 = exchange2, exchange1
                                        spread = (sell_price - buy_price) / buy_price * 100

                                    logging.info(f"Спред для {symbol1} между {exchange1} и {exchange2}: {spread:.2f}%")

                                    if min_spread <= spread <= max_spread:
                                        # Получаем сеть и комиссии
                                        network1, buy_fee = get_best_network(details1['withdraw_fees'])
                                        network2, sell_fee = get_best_network(details2['withdraw_fees'])

                                        if network1 is None or network2 is None:
                                            logging.warning(f"Не удалось найти подходящую сеть для {symbol1} между {exchange1} и {exchange2}, пропуск.")
                                            continue

                                        opportunity = {
                                            'symbol': symbol1,
                                            'buy_exchange': exchange1,
                                            'sell_exchange': exchange2,
                                            'buy_price': buy_price,
                                            'sell_price': sell_price,
                                            'spread': spread,
                                            'spread_display': spread,
                                            'withdraw_possible': details1['withdraw_possible'],
                                            'deposit_possible': details2['deposit_possible'],
                                            'network': network1,  # Добавляем информацию о сети
                                            'withdraw_fee': buy_fee + sell_fee,  # Указываем общую комиссию
                                            'time_found': time.strftime('%Y-%m-%d %H:%M:%S')
                                        }
                                        logging.info(f"Возможность арбитража найдена: {opportunity}")
                                        opportunities.append(opportunity)
                                    else:
                                        logging.info(f"Недостаточный спред для {symbol1} между {exchange1} и {exchange2}: {spread:.2f}%")
            else:
                logging.info(f"Пропуск {symbol1} на {exchange1} из-за недоступности вывода или депозита")

    logging.info(f"Всего найдено возможностей: {len(opportunities)}")
    if opportunities:
        exchanges = set()
        for opp in opportunities:
            exchanges.add(opp['buy_exchange'])
            exchanges.add(opp['sell_exchange'])
        logging.info(f"Возможности арбитража найдены на следующих биржах: {', '.join(exchanges)}")

    return opportunities




def round_price_to_min_precision(price_a, price_b):
    """Округляем price_a до количества знаков после запятой price_b"""
    price_b_str = f"{price_b:.10f}".rstrip('0')  # Убираем лишние нули справа
    decimal_places = len(price_b_str.split('.')[1]) if '.' in price_b_str else 0

    # Округляем price_a до этого количества знаков
    return round(price_a, decimal_places)


# Асинхронная функция для актуализации спреда
async def update_spread(opportunities, spread_threshold):
    logging.info("Актуализация значений спреда.")
    updated_opportunities = []

    tasks = []
    for opp in opportunities:
        tasks.append(asyncio.create_task(update_single_spread(opp, spread_threshold, MIN_SPREAD)))

    updated_opportunities = await asyncio.gather(*tasks)

    removed_signals = len(opportunities) - len([opp for opp in updated_opportunities if opp])
    logging.info(f"Сигналов, исключенных после обновления спреда: {removed_signals}")

    return [opp for opp in updated_opportunities if opp]


async def update_single_spread(opp, spread_threshold, minimal_spread):
    symbol = opp['symbol']
    buy_exchange = opp['buy_exchange']
    sell_exchange = opp['sell_exchange']
    original_spread = opp['spread']

    new_buy_price = await get_price_from_exchange(buy_exchange, symbol)
    new_sell_price = await get_price_from_exchange(sell_exchange, symbol)

    if new_buy_price is None or new_sell_price is None:
        logging.warning(f"Не удалось получить обновленные цены для {symbol} на {buy_exchange} или {sell_exchange}.")
        return None

    # Округляем новые цены перед расчетом спреда
    rounded_new_buy_price = round_price_to_min_precision(new_buy_price, new_sell_price)
    rounded_new_sell_price = round_price_to_min_precision(new_sell_price, new_buy_price)

    new_spread = ((rounded_new_sell_price - rounded_new_buy_price) / rounded_new_buy_price) * 100
    minimal_allowed_spread = original_spread - spread_threshold

    logging.info(
        f"Оригинальный спред для {symbol} ({buy_exchange} -> {sell_exchange}) был {original_spread:.8f}%. "
        f"Цена покупки: {opp['buy_price']:.8f}, Цена продажи: {opp['sell_price']:.8f}. "
        f"Пороговый спред: {minimal_allowed_spread:.8f}%. Время: {opp['time_found']}."
    )
    logging.info(
        f"Обновленный спред для {symbol} составляет {new_spread:.8f}%. "
        f"Новая цена покупки: {rounded_new_buy_price:.8f}, Новая цена продажи: {rounded_new_sell_price:.8f}. "
        f"Время: {time.strftime('%Y-%m-%d %H:%M:%S')}."
    )

    if new_spread < minimal_spread:
        logging.info(
            f"Спред для {symbol} уменьшился ниже минимального значения {minimal_spread:.8f}%. Эта возможность не будет отправлена в Telegram.")
        return None
    elif new_spread < minimal_allowed_spread:
        logging.info(
            f"Спред для {symbol} уменьшился ниже порогового значения {minimal_allowed_spread:.8f}%, но остаётся выше минимального значения {minimal_spread:.8f}%. Эта возможность будет отправлена в Telegram.")
        opp['spread'] = new_spread
        opp['buy_price'] = rounded_new_buy_price
        opp['sell_price'] = rounded_new_sell_price
        return opp
    else:
        logging.info(
            f"Спред для {symbol} соответствует пороговому значению {minimal_allowed_spread:.8f}%. Эта возможность будет обновлена и отправлена в Telegram.")
        opp['spread'] = new_spread
        opp['buy_price'] = rounded_new_buy_price
        opp['sell_price'] = rounded_new_sell_price
        return opp


async def get_price_from_exchange(exchange, symbol):
    """Асинхронная функция для получения цены с конкретной биржи"""
    # Нормализуем символ перед получением цены
    symbol = normalize_symbol(exchange, symbol)

    if exchange == "binance":
        return await get_binance_price(symbol)
    elif exchange == "bybit":
        return await get_bybit_price(symbol)
    elif exchange == "mexc":
        return await get_mexc_price(symbol)
    elif exchange == "kucoin":
        return await get_kucoin_price(symbol)
    elif exchange == "htx":
        return await get_htx_price(symbol)
    else:
        logging.warning(f"Биржа {exchange} не распознана для {symbol}.")
        return None


async def fetch_with_timeout(url, params=None, timeout=10):
    """Асинхронный запрос с тайм-аутом"""
    try:
        response = await fetch_data(url, {'params': params, 'timeout': timeout})
        response.raise_for_status()  # Проверяем статус ответа
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка запроса к {url} с параметрами {params}: {e}")
        return None


async def get_binance_price(symbol):
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        data = await fetch_data(url)

        for item in data:
            if item['symbol'] == symbol:
                return float(item['lastPrice'])
    except Exception as e:
        logging.error(f"Ошибка получения цены с Binance для {symbol}: {e}")
    return None


async def get_bybit_price(symbol):
    try:
        url = "https://api.bybit.com/v5/market/tickers"
        params = {'category': 'spot'}
        data = await fetch_data(url, params)

        for item in data['result']['list']:
            if item['symbol'] == symbol:
                return float(item['lastPrice'])
    except Exception as e:
        logging.error(f"Ошибка получения цены с Bybit для {symbol}: {e}")
    return None


async def get_mexc_price(symbol):
    try:
        # Нормализация символа для MEXC в нужный формат без подчеркиваний и дефисов
        normalized_symbol = symbol.replace('_', '').replace('/', '').upper()
        url = f"https://api.mexc.com/api/v3/ticker/price?symbol={normalized_symbol}"
        data = await fetch_data(url)

        # Нормализация и округление цены MEXC
        if 'price' in data:
            current_price = float(data['price'])
            price_str = str(current_price)
            if price_str[-1] != '0':
                decimal_part = price_str.split('.')[1]
                num_digits = len(decimal_part)
                minus_price = 1 / (10 ** num_digits)
                current_price = round(current_price - minus_price, num_digits)
            return current_price

        logging.warning(f"Символ {normalized_symbol} не найден в данных MEXC: {data}")
    except Exception as e:
        logging.error(f"Ошибка получения цены с MEXC для {symbol}: {e}")
    return None


async def get_kucoin_price(symbol):
    try:
        # Нормализация символа для KuCoin с дефисом
        normalized_symbol = symbol.replace('/USDT', '-USDT').upper()
        url = f"https://api.kucoin.com/api/v1/market/stats?symbol={normalized_symbol}"
        data = await fetch_data(url)

        if 'data' in data and 'last' in data['data']:
            return float(data['data']['last'])  # Используем 'last' как последнюю цену
        logging.warning(f"Символ {normalized_symbol} не найден в данных KuCoin: {data}")
    except Exception as e:
        logging.error(f"Ошибка получения цены с KuCoin для {symbol}: {e}")
    return None


async def get_htx_price(symbol):
    try:
        # Нормализуем символ для HTX (Huobi) в нижний регистр и без разделителей
        normalized_symbol = symbol.replace('/', '').lower()
        url = f"https://api.huobi.pro/market/detail/merged?symbol={normalized_symbol}"
        data = await fetch_data(url)

        if 'tick' in data and 'close' in data['tick']:
            return float(data['tick']['close'])
        logging.warning(f"Символ {normalized_symbol} не найден в данных HTX: {data}")
    except Exception as e:
        logging.error(f"Ошибка получения цены с HTX для {symbol}: {e}")
    return None


def get_best_network(withdraw_fees):
    """Функция для выбора сети с наименьшей комиссией и возможностью вывода"""
    if not withdraw_fees:
        return None, float('inf')

    valid_fees = []
    for fee in withdraw_fees:
        # Проверяем, является ли элемент словарем
        if isinstance(fee, dict) and fee.get('withdraw_enabled') and fee.get('fee') != 'N/A':
            try:
                fee_value = float(fee['fee'])
                valid_fees.append((fee.get('chainType', fee.get('chain', 'N/A')), fee_value))
            except ValueError:
                continue

    if not valid_fees:
        return None, float('inf')

    best_fee = min(valid_fees, key=lambda x: x[1])
    return best_fee



async def check_opportunity_liquidity(opp):
    """Проверка ликвидности для одной возможности"""
    await connect_selected_websockets(opp['symbol'], opp['buy_exchange'], opp['sell_exchange'])
    has_liquidity, dynamic_trade_amount = await check_liquidity_custom(
        opp['buy_exchange'], opp['sell_exchange'], opp['symbol'], PRICE_RANGE_PERCENT
    )
    if has_liquidity:
        opp['dynamic_trade_amount'] = dynamic_trade_amount
        return opp
    return None


# Основная функция
async def main():
    logging.info("Запуск арбитражного бота.")

    while True:
        data = await get_data_from_servers()
        opportunities = check_and_calculate_opportunities(data, MIN_SPREAD, MAX_SPREAD, MIN_TRADE_AMOUNT,
                                                          MIN_VOLUME)

        if opportunities:
            logging.info(f"Найдено {len(opportunities)} возможностей. Проверка ликвидности...")

            # Разбиваем на блоки и обрабатываем по `TASK_BATCH_SIZE`
            for i in range(0, len(opportunities), TASK_BATCH_SIZE):
                batch = opportunities[i:i + TASK_BATCH_SIZE]

                # Параллельная обработка задач в текущем блоке
                await asyncio.gather(*[process_opportunity(opp) for opp in batch])

                # Небольшая задержка между обработкой блоков для оптимизации нагрузки
                await asyncio.sleep(1)

        logging.info("Ожидание 2 минуты перед следующей проверкой.")
        await asyncio.sleep(120)


if __name__ == '__main__':
    asyncio.run(main())