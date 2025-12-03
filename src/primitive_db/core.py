from src.primitive_db.constants import ID_COLUMN, VALID_TYPES
from src.primitive_db.decorators import (
    confirm_action,
    handle_db_errors,
    log_time,
)


def _parse_column_type(col_def):
    """Парсит определение столбца в формате 'имя:тип'."""
    if ':' not in col_def:
        return None, None
    col_name, col_type = col_def.split(':', 1)
    return col_name, col_type


def _validate_value_type(value, expected_type):
    """Проверяет соответствие значения ожидаемому типу."""
    if expected_type == 'int':
        return isinstance(value, int)
    elif expected_type == 'str':
        return isinstance(value, str)
    elif expected_type == 'bool':
        return isinstance(value, bool)
    return False


def _convert_value(value_str, expected_type):
    """Преобразует строковое значение в значение нужного типа."""
    value_str = value_str.strip()
    if value_str.startswith('"') and value_str.endswith('"'):
        value_str = value_str[1:-1]
    elif value_str.startswith("'") and value_str.endswith("'"):
        value_str = value_str[1:-1]

    if expected_type == 'int':
        try:
            return int(value_str)
        except ValueError:
            return None
    elif expected_type == 'str':
        return value_str
    elif expected_type == 'bool':
        if value_str.lower() == 'true':
            return True
        elif value_str.lower() == 'false':
            return False
        return None
    return None


@handle_db_errors
def create_table(metadata, table_name, columns):
    """Создает новую таблицу с указанными столбцами."""
    if table_name in metadata:
        return metadata, f'Ошибка: Таблица "{table_name}" уже существует.'

    parsed_columns = []
    for col_def in columns:
        if ':' not in col_def:
            return metadata, f'Некорректное значение: {col_def}. Попробуйте снова.'
        col_name, col_type = col_def.split(':', 1)
        if col_type not in VALID_TYPES:
            return metadata, f'Некорректное значение: {col_def}. Попробуйте снова.'
        parsed_columns.append(f'{col_name}:{col_type}')

    table_columns = [ID_COLUMN] + parsed_columns

    metadata[table_name] = {
        'columns': table_columns
    }

    columns_str = ', '.join(table_columns)
    return metadata, f'Таблица "{table_name}" успешно создана со столбцами: {columns_str}'


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    """Удаляет таблицу из метаданных."""
    if table_name not in metadata:
        return metadata, f'Ошибка: Таблица "{table_name}" не существует.'

    del metadata[table_name]
    return metadata, f'Таблица "{table_name}" успешно удалена.'


@handle_db_errors
def list_tables(metadata):
    """Возвращает список всех таблиц в базе данных."""
    if not metadata:
        return 'Нет созданных таблиц.'
    return '\n'.join(f'- {table_name}' for table_name in metadata.keys())


@handle_db_errors
@log_time
def insert(metadata, table_name, values):
    """Создает новую запись в указанной таблице."""
    if table_name not in metadata:
        return None, f'Ошибка: Таблица "{table_name}" не существует.'

    table_info = metadata[table_name]
    columns = table_info['columns']
    data_columns = columns[1:]

    if len(values) != len(data_columns):
        return None, 'Некорректное значение: количество значений не соответствует столбцам. Попробуйте снова.'

    record = {}
    for i, col_def in enumerate(data_columns):
        col_name, col_type = _parse_column_type(col_def)
        if col_name is None:
            return None, f'Некорректное значение: {col_def}. Попробуйте снова.'

        converted_value = _convert_value(values[i], col_type)
        if converted_value is None:
            return None, f'Некорректное значение: {values[i]}. Попробуйте снова.'

        record[col_name] = converted_value

    return record, None


@handle_db_errors
@log_time
def select(table_data, where_clause=None):
    """Выбирает записи из таблицы с опциональным условием фильтрации."""
    if where_clause is None:
        return table_data

    result = []
    for record in table_data:
        match = True
        for key, value in where_clause.items():
            if key not in record or record[key] != value:
                match = False
                break
        if match:
            result.append(record)
    return result


@handle_db_errors
def update(table_data, set_clause, where_clause):
    """Обновляет записи в таблице по условию."""
    updated_count = 0
    for record in table_data:
        match = True
        for key, value in where_clause.items():
            if key not in record or record[key] != value:
                match = False
                break
        if match:
            for key, value in set_clause.items():
                record[key] = value
            updated_count += 1
    return table_data, updated_count


@handle_db_errors
@confirm_action("удаление записи")
def delete(table_data, where_clause):
    """Удаляет записи из таблицы по условию."""
    result = []
    deleted_count = 0
    for record in table_data:
        match = True
        for key, value in where_clause.items():
            if key not in record or record[key] != value:
                match = False
                break
        if not match:
            result.append(record)
        else:
            deleted_count += 1
    return result, deleted_count


@handle_db_errors
def get_table_info(metadata, table_name, table_data):
    """Возвращает информацию о таблице."""
    if table_name not in metadata:
        return f'Ошибка: Таблица "{table_name}" не существует.'

    table_info = metadata[table_name]
    columns = table_info['columns']
    columns_str = ', '.join(columns)
    record_count = len(table_data)

    return f'Таблица: {table_name}\nСтолбцы: {columns_str}\nКоличество записей: {record_count}'
