import pandas as pd
from sqlalchemy import Engine, text
import logging


def configure_logging(level):
    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s.%(msecs)03d] %(module)10s:%(lineno)-3d %(levelname)-7s - %(message)s",
    )


logger = logging.getLogger(__name__)
configure_logging(level=logging.INFO)


def create_tables(engine: Engine):
    # Функция создает таблицы data и processed_records в базе данных, если они не существуют.
    create_data_table_query = """
    CREATE TABLE IF NOT EXISTS data (
        transport VARCHAR NOT NULL,
        link VARCHAR NOT NULL,
        number_of_cargo VARCHAR PRIMARY KEY NOT NULL,
        distance_km INTEGER NOT NULL,
        loading INTEGER NOT NULL,
        "start" VARCHAR NOT NULL,
        "end" VARCHAR NOT NULL,
        date_load VARCHAR NOT NULL,
        nds INTEGER NOT NULL,
        request_price INTEGER NOT NULL,
        new_info VARCHAR NOT NULL,
        cargo VARCHAR NOT NULL
    )
    """

    create_processed_records_table_query = """
    CREATE TABLE IF NOT EXISTS processed_records (
        number_of_cargo VARCHAR PRIMARY KEY NOT NULL
    )
    """

    with engine.connect() as conn:
        try:
            # Выполнение запросов на создание таблиц
            conn.execute(text(create_data_table_query))
            conn.execute(text(create_processed_records_table_query))
            conn.commit()  # Фиксация транзакции
            logger.info("Таблицы data и processed_records успешно созданы или уже существуют.")
        except Exception as e:
            logger.error("Ошибка при создании таблиц: %s", e)



def load_to_db(engine: Engine, table_name: str, df: pd.DataFrame, check_column: str) -> None:
    with engine.connect() as conn:
        for index, row in df.iterrows():
            try:
                # Создаем SQL-запрос для проверки существования строки
                check_query = text(f"SELECT COUNT(*) FROM {table_name} WHERE {check_column} = :value")
                result = conn.execute(check_query, {'value': row[check_column]}).scalar()

                # Если строка не существует, вставляем ее
                if result == 0:
                    # Поскольку row является Series, преобразуем его в DataFrame
                    row_df = row.to_frame().T
                    row_df.to_sql(table_name, engine, if_exists='append', index=False)
                    logger.info("Новая перевозка %s добавлена в таблицу.", row[check_column])
                else:
                    # Если строка существует, обновляем ее
                    update_query = text(f"""
                        UPDATE {table_name}
                        SET
                            transport = :transport,
                            link = :link,
                            number_of_cargo = :number_of_cargo,
                            distance_km = :distance_km,
                            loading = :loading,
                            start = :start,
                            "end" = :end,
                            date_load = :date_load,
                            nds = :nds,
                            request_price = :request_price,
                            new_info = :new_info,
                            cargo = :cargo
                        WHERE {check_column} = :check_value
                    """)
                    conn.execute(update_query, {
                        'transport': row['transport'],
                        'link': row['link'],
                        'number_of_cargo': row['number_of_cargo'],
                        'distance_km': row['distance_km'],
                        'loading': row['loading'],
                        'start': row['start'],
                        'end': row['end'],
                        'date_load': row['date_load'],
                        'nds': row['nds'],
                        'request_price': row['request_price'],
                        'new_info': row['new_info'],
                        'cargo': row['cargo'],
                        'check_value': row[check_column]
                    })
                    # logger.info("Запись с %s = %s обновлена в таблице.", check_column, row[check_column])
            except Exception as e:
                logger.error("Ошибка при обработке строки %s: %s", index, e)

    logger.info("Выполнена загрузка информации в базу данных")
