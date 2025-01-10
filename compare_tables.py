import psycopg2

from config import host, user, password, database, links_for_pairs


tables = ['binance', 'gateio', 'bybit']
merge_table = 'merge_table'

min_spread = 2
max_spread = 70


def compare(bid1, ask1, bid2, ask2, min_spread, max_spread):
    if (ask2 - bid1) / ask2 * 100 > min_spread and (ask2 - bid1) / ask2 * 100 < max_spread:
        print(f'процент1 ={(ask2 - bid1) / ask2 * 100}')
        return True
    elif (ask1 - bid2) / ask1 * 100 > min_spread and (ask1 - bid2) / ask1 * 100 < max_spread:
        print(f'процент2 ={(ask1 - bid2) / ask1 * 100}')
        return True
    else:
        return False


def load_tables_to_list(tables):

    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database)
        cursor = connection.cursor()

        table_data = []
        with connection.cursor() as cursor:
            for table in tables:
                cursor.execute(f"SELECT * FROM {table};")
                rows = cursor.fetchall()  # Считываем все строки таблицы
                table_data.append(rows)  # Добавляем содержимое таблицы в список
        return table_data

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()


def compare_tables(tables_lists):
    pairs = []
    duplicates = []
    seen = []

    # # добавляем все названия пар в список
    for i in tables_lists:
        for pair in i:
            pairs.append(pair[0])

    # ищем повторяющиеся пары и добавляем их в список дубликатов
    for item in pairs:
        if item in seen and item not in duplicates:
            duplicates.append(item)
        elif item not in seen:
            seen.append(item)

    duplicates_lists = []
    # добавляем пары в список списков дубликатов для каждой таблицы
    for table_index, table in enumerate(tables_lists):
        duplicates_lists.append([])
        for row in table:
            if row[0] in duplicates:
                duplicates_lists[table_index].append(row)

    pairwise_results = []
    # Перебираем все пары списков
    for i in range(len(duplicates_lists)):
        for j in range(i + 1, len(duplicates_lists)):
            # Текущая пара таблиц
            list1 = duplicates_lists[i]
            list2 = duplicates_lists[j]

            # Найти пересечения по значению row[0]
            intersection = []
            for row1 in list1:
                for row2 in list2:
                    if row1[0] == row2[0]:  # Совпадение по row[0]
                        # Сохраняем значение row1[0], row1[1], row1[2], row2[1], row2[2]
                        intersection.append({
                            'key': row1[0],
                            'bid1': row1[1],
                            'ask1': row1[2],
                            'bid2': row2[1],
                            'ask2': row2[2],
                        })

            # Сохраняем результат для текущей пары таблиц
            pairwise_results.append(((tables[i], tables[j]), intersection))

    print("Пересечения по парам таблиц:")
    for pair, intersection in pairwise_results:
        print(f"Таблица {pair[0]} и Таблица {pair[1]}:")
        for match in intersection:
            if compare(float(match['bid1']), float(match['bid2']), float(match['ask1']), float(match['ask2']),
                       min_spread, max_spread):
                print(
                    f'''tiker: {match['key']}, \n{pair[0]} Bid: {match['bid1']}, Ask: {match['ask1']}\n{pair[1]} Bid: {match['bid2']},  Ask: {match['ask2']}''')
                print(f'''{links_for_pairs(pair[0], match['key'])} | {links_for_pairs(pair[1], match['key'])}\n''')



if __name__ == '__main__':
    compare_tables(load_tables_to_list(tables))
