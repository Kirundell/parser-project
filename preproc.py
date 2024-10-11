import pandas as pd
import re
import ast
from sqlalchemy import create_engine
import logging
from db_connect import load_to_db, create_tables
from tg_load import tg_loading
from dotenv import load_dotenv
import os


# Подключаем переменные окружения
load_dotenv()

# Настройка базовых конфигураций логов
def configure_logging(level):
    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s.%(msecs)03d] %(module)10s:%(lineno)-3d %(levelname)-7s - %(message)s",
    )


logger = logging.getLogger(__name__)
configure_logging(level=logging.INFO)


def first_step(df: pd.DataFrame) -> pd.DataFrame:
    sub = df['Contacts'].value_counts().idxmax()  # данные, доступные только по подписке
    new_df = df[df['Contacts'] != sub]  # удалим их
    new_df = new_df.drop_duplicates(subset='Number_of_cargo',
                                    ignore_index=True)  # удалим повторяющиеся грузы и обновим индексацию

    logger.info("Первый этап обработки данных завершен")
    return new_df


def second_step(new_df: pd.DataFrame) -> pd.DataFrame:
    new_df.loc[:,'Cargo'] = new_df['Cargo'].fillna(
        'Информация о грузе не найдена')  # заменим пропуски значением 'Информация о грузе не найдена'
    new_df = new_df.assign(number_of_cargo=new_df['Number_of_cargo'].apply(
        lambda x: re.sub(r'Ном.*$', '', x)))  # удалим лишнее в первом столбце
    new_df = new_df.assign(
        distance_km=new_df['Distance'].apply(lambda x: int(''.join(x.split(' '))[:-2])))  # удалим лишнее в distance

    def del1(string: str) -> str:  # функция удаляет все, что в строке идет после букв Отп
        for i in range(len(string) - 2):
            if string[i:i + 3] == 'Отп':
                return string[:i]
        return string

    def del2(string: str) -> str:  # еще одна функция для отчистки рекламного мусора в конце строки
        for i in range(len(string) - 3):
            if string[i:i + 4] == 'торг':
                return string[:i + 4]
        return string

    new_df = new_df.assign(price_del_end=new_df['Price'].apply(lambda x: del2(del1(x))))  # чистка конца столбца прайс

    new_df.loc[new_df['price_del_end'].str.startswith('запрос'), 'price_del_end'] = 'запрос ставки'
    new_df.loc[new_df['price_del_end'].str.startswith('груза нет'), 'price_del_end'] = 'запрос ставки'

    logger.info("Второй этап обработки данных завершен")

    return new_df


def third_step(new_df: pd.DataFrame) -> pd.DataFrame:
    # добавим столбец, содержащий список цен
    def find_num(string: str) -> str | list:
        if string == 'запрос ставки':
            return string
        new_string = ''
        a = []
        for i in range(len(string)):
            if string[i].isdigit():
                new_string += string[i]
            elif string[i] == ',':
                new_string += '.'
            elif string[i] == 'р':
                a.append(new_string)
                new_string = ''
            elif string[i] == 'т':
                a.append(new_string)
                new_string = ''
            elif string[i] == 'ч':
                break
            else:
                pass
        b = [x for x in a if len(x) != 0]
        return b

    new_df = new_df.assign(price=new_df['price_del_end'].apply(find_num))

    # В столбцах Start и End содержаться массивы, которые записаны в виде строк. Напишем функцию, которая это исправит
    def str_to_list(array_str: str) -> list:
        return ast.literal_eval(array_str)

    new_df['cities'] = new_df['Start'].apply(str_to_list)
    new_df['date'] = new_df['End'].apply(str_to_list)

    # информация о погрузках
    def loading(loading_info: list) -> int:
        if len(loading_info) > 2:
            return 1
        else:
            return 0

    new_df['loading'] = new_df['cities'].apply(loading)

    # Теперь распарсим данные из колонки cities на место погрузки (start) и место выгрузки (end)
    def start_end(city_values: list) -> pd.Series:
        values = [None] * 2
        values[0] = city_values[0]
        values[1] = city_values[1:]
        return pd.Series(values)

    start_stop_df = new_df['cities'].apply(start_end).rename(columns={0: 'start', 1: 'end'})

    new_df = pd.concat([new_df, start_stop_df], axis=1)

    # Также распарсим из колонки date информацию о том, когда будет осуществляться погрузка
    def get_date(date_val: str) -> str:
        for item in date_val:
            if item.strip()[0:5] == 'готов':
                return item
            elif item.strip() == 'постоянно':
                return item
        return 'Не удалось получить информацию'

    new_df['date_load'] = new_df['date'].apply(get_date)

    # Удалим лишние столбцы
    new_df = new_df.drop(['Start', 'End', 'date', 'cities'], axis=1)

    logger.info("Третий этап обработки данных завершен")

    return new_df


def fourth_step(new_df: pd.DataFrame) -> pd.DataFrame:
    # Разделим столбец с информацией о цене на 5 столбцов.
    def split_data(info: list) -> pd.Series:
        request = isinstance(info, str)
        values = [None] * 5
        if not request:
            for i, val in enumerate(info):
                if i < 4:
                    values[i] = val

        values[4] = int(request)

        return pd.Series(values)

    price_df = new_df['price'].apply(split_data).rename(columns={
        0: 'nds',
        1: 'nds_km',
        2: 'without_nds',
        3: 'without_nds_km',
        4: 'request_price'
    })

    new_df = pd.concat([new_df, price_df], axis=1)

    # Обработаем контактную информацию
    new_df['Contacts'] = new_df['Contacts'].fillna('Информация не найдена')

    def del_cont(string: str) -> list:
        new_string = []
        for i in range(len(string) - 3):
            if string[i:i + 4] == 'Код:':
                new_string.append(string[:i])
            if (string[i] == '+') and (string[i + 1].isdigit() is True):
                j = i
                number = ''
                for j in range(j, len(string)):
                    if string[j] != ',':
                        number += string[j]
                    else:
                        new_string.append(number)
                        break

        return new_string

    new_df = new_df.assign(inform=new_df['Contacts'].apply(del_cont))

    # Разделим столбец с информацией на 2 столбца.
    # В первом будет содержаться информация о перевозке, во втором контактные номера телефонов.
    def split_info(number_info: str) -> tuple[list[str], list[str]]:
        arr_info, arr_number = [], []
        if len(number_info) != 0:
            for i in range(len(number_info)):
                if number_info[i][:2] == '+7':
                    arr_number.append(number_info[i])
                else:
                    arr_info.append(number_info[i])

        return arr_info, arr_number

    new_df = new_df.assign(new_info=new_df['inform'].apply(split_info))

    # Теперь немного почистим столбец, содержащий информацию о грузе.
    # Запишем в формате массива, где первый элемент будет содержать информацию об объеме груза, а второй о самом грузе
    def split_cargo(string: str) -> str | list:
        if string == 'Информация о грузе не найдена':
            return [string]
        else:
            for i in range(len(string)):
                if string[i].isalpha():
                    return [string[:i], string[i:]]

    new_df = new_df.assign(cargo=new_df['Cargo'].apply(split_cargo))

    fin_df = new_df.drop(['Number_of_cargo', 'Distance', 'Cargo', 'Price', 'Contacts',
                          'price_del_end', 'price', 'nds_km', 'without_nds', 'without_nds_km', 'inform'], axis=1)
    fin_df['nds'] = fin_df['nds'].fillna(0)

    fin_df = fin_df.rename(columns={'Transport': 'transport', 'Link': 'link'})

    logger.info("Четвертый этап обработки данных завершен\n")

    return fin_df


def preprocessing_result():
    data = pd.read_excel("test_data/server_orders_test.xlsx")

    data_1 = first_step(data)
    data_2 = second_step(data_1)
    data_3 = third_step(data_2)
    finally_df = fourth_step(data_3)

    db_url = (f'postgresql+psycopg2://'
              f'{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:'
              f'{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
              )
    engine = create_engine(db_url)
    create_tables(engine)
    load_to_db(engine, 'data', finally_df, 'number_of_cargo')
    tg_loading()


if __name__ == '__main__':
    preprocessing_result()
