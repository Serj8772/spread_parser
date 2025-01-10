import psycopg2

from config import host, user, password, database


def create_table(exchange_name):
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                DROP TABLE IF EXISTS {exchange_name};
                CREATE TABLE {exchange_name} (pair TEXT PRIMARY KEY, bid VARCHAR, ask VARCHAR);
                """
            )
            connection.commit()

    except Exception as ex:
        print('Error', ex)
        pass
    finally:
        if connection:
            connection.close()
            print(f'Таблица {exchange_name} создана')

def send_data(exchange_name, data_list):
    # Подключение к базе данных с использованием "with"
    try:

        with psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=database
        ) as conn:  # Автоматически закрывает соединение
            with conn.cursor() as cursor:  # Автоматически закрывает курсор
                # Вставка данных
                for pair, values in data_list.items():
                    # print(pair, values)

                    cursor.execute(
                        f'''
                        INSERT INTO {exchange_name} (pair, bid, ask)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (pair)
                        DO UPDATE SET
                            bid = EXCLUDED.bid,
                            ask = EXCLUDED.ask;
                        ''',
                        (pair, values[0], values[1])
                    )
                conn.commit()  # Сохранение изменений

        print("Данные успешно добавлены в таблицу!")

    except Exception as e:
        print(f"Произошла ошибка: {e}")


def main_db(exchange_name, data_list):
    create_table(exchange_name)
    send_data(exchange_name, data_list)




