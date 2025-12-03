import shlex

import prompt
from prettytable import PrettyTable

from src.primitive_db.core import (
    create_table,
    delete,
    drop_table,
    get_table_info,
    insert,
    list_tables,
    select,
    update,
)
from src.primitive_db.constants import METADATA_FILE
from src.primitive_db.decorators import create_cacher
from src.primitive_db.parser import parse_set_clause, parse_where_clause
from src.primitive_db.utils import (
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)

cache_result = create_cacher()


def print_help():
    print("\n***Операции с данными***")
    print("Функции:")
    print(
        "<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) - создать запись."
    )
    print(
        "<command> select from <имя_таблицы> where <столбец> = <значение> - прочитать записи по условию."
    )
    print("<command> select from <имя_таблицы> - прочитать все записи.")
    print(
        "<command> update <имя_таблицы> set <столбец1> = <новое_значение1> where <столбец_условия> = <значение_условия> - обновить запись."
    )
    print(
        "<command> delete from <имя_таблицы> where <столбец> = <значение> - удалить запись."
    )
    print("<command> info <имя_таблицы> - вывести информацию о таблице.")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def format_select_result(records, columns):
    if not records:
        return "Записи не найдены."

    table = PrettyTable()
    col_names = []
    for col_def in columns:
        if ':' in col_def:
            col_name, _ = col_def.split(':', 1)
            col_names.append(col_name)
        else:
            col_names.append(col_def)

    table.field_names = col_names

    for record in records:
        row = []
        for col_name in col_names:
            value = record.get(col_name, '')
            row.append(value)
        table.add_row(row)

    return table.get_string()


def run():
    print("***Операции с данными***")
    print_help()

    while True:
        metadata = load_metadata(METADATA_FILE)
        user_input = prompt.string(">>> Введите команду: ").strip()

        if not user_input:
            continue

        try:
            args = shlex.split(user_input)
        except ValueError:
            print(f"Некорректное значение: {user_input}. Попробуйте снова.")
            continue

        if not args:
            continue

        command = args[0]

        if command == 'exit':
            break
        elif command == 'help':
            print_help()
        elif command == 'insert' and len(args) >= 2 and args[1] == 'into':
            if len(args) < 4 or args[2] == 'values':
                print("Некорректное значение: недостаточно аргументов. Попробуйте снова.")
                continue
            table_name = args[2]
            if args[3] != 'values':
                print("Некорректное значение: ожидается 'values'. Попробуйте снова.")
                continue

            values_str = ' '.join(args[4:])
            if values_str.startswith('(') and values_str.endswith(')'):
                values_str = values_str[1:-1]

            values = []
            current_value = []
            in_quotes = False
            quote_char = None
            for char in values_str:
                if char in ('"', "'") and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_value.append(char)
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_value.append(char)
                elif char == ',' and not in_quotes:
                    if current_value:
                        values.append(''.join(current_value).strip())
                        current_value = []
                else:
                    current_value.append(char)
            if current_value:
                values.append(''.join(current_value).strip())

            result = insert(metadata, table_name, values)
            if result is None:
                continue
            record, error = result
            if error:
                print(error)
            else:
                table_data = load_table_data(table_name)
                if table_data:
                    max_id = max(rec.get('ID', 0) for rec in table_data if 'ID' in rec)
                    new_id = max_id + 1
                else:
                    new_id = 1
                record['ID'] = new_id
                table_data.append(record)
                save_table_data(table_name, table_data)
                print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
        elif command == 'select' and len(args) >= 3 and args[1] == 'from':
            table_name = args[2]
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            table_data = load_table_data(table_name)
            where_clause = None

            if len(args) > 3 and args[3] == 'where':
                where_str = ' '.join(args[4:])
                where_clause = parse_where_clause(where_str)
                if where_clause is None:
                    print(f"Некорректное значение: {where_str}. Попробуйте снова.")
                    continue

            cache_key = (table_name, tuple(where_clause.items()) if where_clause else None)

            def get_results():
                return select(table_data, where_clause)

            results = cache_result(cache_key, get_results)
            if results is None:
                continue

            table_info = metadata[table_name]
            columns = table_info['columns']
            formatted = format_select_result(results, columns)
            print(formatted)
        elif command == 'update':
            if len(args) < 2:
                print("Некорректное значение: недостаточно аргументов. Попробуйте снова.")
                continue

            table_name = args[1]
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            if len(args) < 4 or args[2] != 'set':
                print("Некорректное значение: ожидается 'set'. Попробуйте снова.")
                continue

            where_idx = -1
            for i, arg in enumerate(args):
                if arg == 'where':
                    where_idx = i
                    break

            if where_idx == -1:
                print("Некорректное значение: ожидается 'where'. Попробуйте снова.")
                continue

            set_str = ' '.join(args[3:where_idx])
            where_str = ' '.join(args[where_idx + 1:])

            set_clause = parse_set_clause(set_str)
            where_clause = parse_where_clause(where_str)

            if set_clause is None or where_clause is None:
                print("Некорректное значение. Попробуйте снова.")
                continue

            table_data = load_table_data(table_name)
            result = update(table_data, set_clause, where_clause)
            if result is None:
                continue
            updated_data, count = result
            if count > 0:
                save_table_data(table_name, updated_data)
                updated_id = where_clause.get('ID', '?')
                if updated_id == '?':
                    for rec in updated_data:
                        match = True
                        for key, value in where_clause.items():
                            if rec.get(key) != value:
                                match = False
                                break
                        if match:
                            updated_id = rec.get('ID', '?')
                            break
                print(f'Запись с ID={updated_id} в таблице "{table_name}" успешно обновлена.')
            else:
                print(f'Записи не найдены в таблице "{table_name}".')
        elif command == 'delete' and len(args) >= 2 and args[1] == 'from':
            if len(args) < 5 or args[3] != 'where':
                print("Некорректное значение: недостаточно аргументов. Попробуйте снова.")
                continue

            table_name = args[2]
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            where_str = ' '.join(args[4:])
            where_clause = parse_where_clause(where_str)
            if where_clause is None:
                print(f"Некорректное значение: {where_str}. Попробуйте снова.")
                continue

            table_data = load_table_data(table_name)
            result = delete(table_data, where_clause)
            if result is None:
                continue
            updated_data, count = result
            if isinstance(count, str) and "отменена" in count:
                print(count)
                continue
            if count > 0:
                save_table_data(table_name, updated_data)
                deleted_id = where_clause.get('ID', '?')
                print(f'Запись с ID={deleted_id} успешно удалена из таблицы "{table_name}".')
            else:
                print(f'Записи не найдены в таблице "{table_name}".')
        elif command == 'info':
            if len(args) < 2:
                print("Некорректное значение: недостаточно аргументов. Попробуйте снова.")
                continue

            table_name = args[1]
            table_data = load_table_data(table_name)
            info = get_table_info(metadata, table_name, table_data)
            if info is not None:
                print(info)
        elif command == 'create_table':
            if len(args) < 3:
                print("Некорректное значение: недостаточно аргументов. Попробуйте снова.")
                continue
            table_name = args[1]
            columns = args[2:]
            result = create_table(metadata, table_name, columns)
            if result is None:
                continue
            metadata, message = result
            print(message)
            if message.startswith('Таблица'):
                save_metadata(METADATA_FILE, metadata)
        elif command == 'drop_table':
            if len(args) < 2:
                print("Некорректное значение: недостаточно аргументов. Попробуйте снова.")
                continue
            table_name = args[1]
            result = drop_table(metadata, table_name)
            if result is None:
                continue
            metadata, message = result
            print(message)
            if isinstance(message, str) and "отменена" in message:
                continue
            if message.startswith('Таблица') and 'успешно удалена' in message:
                save_metadata(METADATA_FILE, metadata)
        elif command == 'list_tables':
            result = list_tables(metadata)
            if result is not None:
                print(result)
        else:
            print(f"Функции {command} нет. Попробуйте снова.")


def welcome():
    print("***")
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")
    command = prompt.string("Введите команду: ")
    while command != "exit":
        if command == "help":
            print("<command> exit - выйти из программы")
            print("<command> help - справочная информация")
        command = prompt.string("Введите команду: ")
