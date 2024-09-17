import psycopg2
import pandas as pd
import pickle
import requests
import time
from catboost import Pool
import re
import logging
from dotenv import load_dotenv
import os


# Подключаем переменные окружения
load_dotenv()


def configure_logging(level):
    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s.%(msecs)03d] %(module)10s:%(lineno)-3d %(levelname)-7s - %(message)s",
    )


logger = logging.getLogger(__name__)
configure_logging(level=logging.INFO)


def preprocessing_info(string):
    pattern = r'\("{""(.*?)""}",{\+(\d{7})-(\d{2})-(\d{2})}\)'
    match = re.match(pattern, string)
    if match:
        return [match.group(1), f'+{match.group(2)}-{match.group(3)}-{match.group(4)}']
    else:
        return [None, 'не удалось получить информацию']

def company_name(string):
    new_string = ''
    for i in range(len(string)):
        if (string[i].isalpha()) or (string[i] in (' ', '-')):
            new_string += string[i]
        elif string[i] == ',':
            return new_string
    return 'не удалось получить информацию'

def return_message(row):
    k = 0
    if row['prediction'] == 0 and row['loading'] == 1: # нет информации о цене + догрузка
        k = 1
        message = f"""
❔❔❔❔❔❔❔❔❔❔❔❔❔

Тип кузова: {row['transport']}

Груз: {row['cargo'][1:-1]}

Расстояние: {row['distance_km']} км

Несколько точек выгрузки⚠️⚠️⚠️

Начало маршрута: {row['start']}

Конец маршрута: {row['end'][1:-1]}

Дата погрузки: {row['date_load']}

———————————————————
Ставка с НДС: нет информации
Оценка бота: ставка не указана
———————————————————

Информация о заказчике: {company_name(row['new_info'])}

Контактный телефон: {preprocessing_info(row['new_info'])[1]}

Ссылка на ati: {row['link']}

❔❔❔❔❔❔❔❔❔❔❔❔❔
"""
    elif row['prediction'] == 0 and row['loading'] == 0: # нет информации о цене без догрузки
        k = 1
        message = f"""
❔❔❔❔❔❔❔❔❔❔❔❔❔

Тип кузова: {row['transport']}

Груз: {row['cargo'][1:-1]}

Расстояние: {row['distance_km']} км

Начало маршрута: {row['start']}

Конец маршрута: {row['end'][1:-1]}

Дата погрузки: {row['date_load']}

———————————————————
Ставка с НДС: нет информации
Оценка бота: ставка не указана
———————————————————

Информация о заказчике: {company_name(row['new_info'])}

Контактный телефон: {preprocessing_info(row['new_info'])[1]}

Ссылка на ati: {row['link']}

❔❔❔❔❔❔❔❔❔❔❔❔❔
"""
    elif row['prediction'] == 1 and row['loading'] == 1: # низкая цена + догрузка
        k = 2
        message = f"""
❌❌❌❌❌❌❌❌❌❌❌❌❌

Тип кузова: {row['transport']}

Груз: {row['cargo'][1:-1]}

Расстояние: {row['distance_km']} км

Несколько точек выгрузки⚠️⚠️⚠️

Начало маршрута: {row['start']}

Конец маршрута: {row['end'][1:-1]}

Дата погрузки: {row['date_load']}

———————————————————
Ставка с НДС: {row['nds']} руб.
Оценка бота: низкая цена за перевозку
———————————————————

Информация о заказчике: {company_name(row['new_info'])}

Контактный телефон: {preprocessing_info(row['new_info'])[1]}

Ссылка на ati: {row['link']}

❌❌❌❌❌❌❌❌❌❌❌❌❌
"""
    elif row['prediction'] == 1 and row['loading'] == 0: # низкая цена без догрузки
        k = 2
        message = f"""
❌❌❌❌❌❌❌❌❌❌❌❌❌

Тип кузова: {row['transport']}

Груз: {row['cargo'][1:-1]}

Расстояние: {row['distance_km']} км

Начало маршрута: {row['start']}

Конец маршрута: {row['end'][1:-1]}

Дата погрузки: {row['date_load']}

———————————————————
Ставка с НДС: {row['nds']} руб.
Оценка бота: низкая цена за перевозку
———————————————————

Информация о заказчике: {company_name(row['new_info'])}

Контактный телефон: {preprocessing_info(row['new_info'])[1]}

Ссылка на ati: {row['link']}

❌❌❌❌❌❌❌❌❌❌❌❌❌
"""
    elif row['prediction'] == 2 and row['loading'] == 1: # хорошая цена + догрузка
        k = 3
        message = f"""
✅✅✅✅✅✅✅✅✅✅✅✅✅

Тип кузова: {row['transport']}

Груз: {row['cargo'][1:-1]}

Расстояние: {row['distance_km']} км

Несколько точек выгрузки⚠️⚠️⚠️

Начало маршрута: {row['start']}

Конец маршрута: {row['end'][1:-1]}

Дата погрузки: {row['date_load']}

———————————————————
Ставка с НДС: {row['nds']} руб.
Оценка бота: хороший груз
———————————————————

Информация о заказчике: {company_name(row['new_info'])}

Контактный телефон: {preprocessing_info(row['new_info'])[1]}

Ссылка на ati: {row['link']}

✅✅✅✅✅✅✅✅✅✅✅✅✅
"""
    elif row['prediction'] == 2 and row['loading'] == 0: # хорошая цена без догрузки
        k = 3
        message = f"""
✅✅✅✅✅✅✅✅✅✅✅✅✅

Тип кузова: {row['transport']}

Груз: {row['cargo'][1:-1]}

Расстояние: {row['distance_km']} км

Начало маршрута: {row['start']}

Конец маршрута: {row['end'][1:-1]}

Дата погрузки: {row['date_load']}

———————————————————
Ставка с НДС: {row['nds']} руб.
Оценка бота: хороший груз
———————————————————

Информация о заказчике: {company_name(row['new_info'])}

Контактный телефон: {preprocessing_info(row['new_info'])[1]}

Ссылка на ati: {row['link']}

✅✅✅✅✅✅✅✅✅✅✅✅✅
"""
    return k, message


# Установка соединения с базой данных PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)
cursor = conn.cursor()

# Загрузка обученной модели из pickle файла
with open('model2.pkl', 'rb') as file:
    trained_model = pickle.load(file)


def fetch_new_records():
    cursor.execute("""
    SELECT * FROM data
    WHERE number_of_cargo NOT IN (SELECT number_of_cargo FROM processed_records)
    """)
    new_records = cursor.fetchall()
    if new_records:
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(new_records, columns=columns)
    return pd.DataFrame()


def process_and_update_records(new_data):
    if not new_data.empty:
        pred = new_data.drop(['transport', 'link', 'number_of_cargo', 'new_info', 'loading',
                              'request_price', 'cargo', 'date_load'], axis=1)

        # Указание категориальных признаков
        cat_features = ['start', 'end']

        # Создание объекта Pool с указанием категориальных признаков
        pool = Pool(pred, cat_features=cat_features)

        # Прогнозирование моделью машинного обучения
        predictions = trained_model.predict(pool)
        new_data['prediction'] = predictions

        # Обновление таблицы processed_records
        for _, row in new_data.iterrows():
            cursor.execute("INSERT INTO processed_records (number_of_cargo) VALUES (%s)", (row['number_of_cargo'],))
            conn.commit()

        # Отправка результатов в Telegram-бота
        for _, row in new_data.iterrows():
            # print(row)
            send_to_telegram(row)


def send_to_telegram(row):
    bot_token = os.getenv('BOT_TOKEN')
    chat_id_test = os.getenv('TEST_CHAT')

    chat_id_yellow = os.getenv('YELLOW_CHAT')
    chat_id_red = os.getenv('RED_CHAT')
    chat_id_green = os.getenv('GREEN_CHAT')

    flag, message = return_message(row)
    # print(flag)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    # payload = {
    #     'chat_id': chat_id_test,
    #     'text': message
    # }

    if flag == 1:
        payload = {
            'chat_id': chat_id_yellow,
            'text': message
        }
    elif flag == 2:
        payload = {
            'chat_id': chat_id_red,
            'text': message
        }
    elif flag == 3:
        payload = {
            'chat_id': chat_id_green,
            'text': message
        }
    else:
        logger.warning(f"Неверный флаг: {flag} для строки: {row}")


    while True:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            logger.info("Перевозка добавлена в бота")
            break
        elif response.status_code == 429:
            retry_after = response.json().get("parameters", {}).get("retry_after", 30)
            logger.info(f"Слишком много запросов. Ожидание {retry_after} секунд.")
            time.sleep(retry_after)  # Ожидание перед повторной попыткой
        else:
            logger.info(f"Ошибка при отправке сообщения: {response.text}")
            break  # Выход из цикла при других ошибках


def tg_loading():
    new_data = fetch_new_records()
    process_and_update_records(new_data)
    logger.info("Все новые перевозки добавлены в бота\n")
