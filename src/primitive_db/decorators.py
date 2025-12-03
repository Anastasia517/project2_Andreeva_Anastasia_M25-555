import functools
import time

import prompt


def handle_db_errors(func):
    """Декоратор для обработки ошибок базы данных."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print("Ошибка: Файл данных не найден. Возможно, база данных не инициализирована.")
            return None
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец {e} не найден.")
            return None
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
            return None
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
            return None

    return wrapper


def confirm_action(action_name):
    """Декоратор для подтверждения опасных операций."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            confirmation = prompt.string(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            )
            if confirmation.lower() != 'y':
                print("Операция отменена.")
                if len(args) > 0:
                    first_arg = args[0]
                    if isinstance(first_arg, list):
                        return first_arg, "Операция отменена."
                    elif isinstance(first_arg, dict):
                        return first_arg, "Операция отменена."
                return None, "Операция отменена."
            return func(*args, **kwargs)

        return wrapper

    return decorator


def log_time(func):
    """Декоратор для измерения времени выполнения функции."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()
        elapsed = end_time - start_time
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result

    return wrapper


def create_cacher():
    """Создает функцию кэширования с замыканием."""
    cache = {}

    def cache_result(key, value_func):
        """Кэширует результат выполнения функции по ключу."""
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value

    return cache_result
