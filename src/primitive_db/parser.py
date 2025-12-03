def parse_where_clause(where_str):
    if '=' not in where_str:
        return None

    parts = where_str.split('=', 1)
    if len(parts) != 2:
        return None

    column = parts[0].strip()
    value = parts[1].strip()

    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    elif value.startswith("'") and value.endswith("'"):
        value = value[1:-1]

    if value.lower() == 'true':
        value = True
    elif value.lower() == 'false':
        value = False
    elif value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
        value = int(value)

    return {column: value}


def parse_set_clause(set_str):
    if '=' not in set_str:
        return None

    parts = set_str.split('=', 1)
    if len(parts) != 2:
        return None

    column = parts[0].strip()
    value = parts[1].strip()

    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    elif value.startswith("'") and value.endswith("'"):
        value = value[1:-1]

    if value.lower() == 'true':
        value = True
    elif value.lower() == 'false':
        value = False
    elif value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
        value = int(value)

    return {column: value}
