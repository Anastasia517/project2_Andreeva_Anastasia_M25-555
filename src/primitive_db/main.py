#!/usr/bin/env python3


def main():
    """Точка входа в приложение базы данных."""
    from src.primitive_db.engine import run
    run()


if __name__ == '__main__':
    main()
