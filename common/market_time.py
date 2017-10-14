from datetime import datetime

date_format_1 = '%Y-%m-%d'
date_format_2 = '%Y%m%d'
datetime_format_1 = '%Y-%m-%d %H:%M:%S'
datetime_format_2 = '%Y%m%d%H%M%S'

def int_to_date(date_int):
    return string_to_date(str(date_int), date_format = date_format_2)

def string_to_date(date_str, date_format = date_format_1):
    try:
        return datetime.strptime(date_str, date_format)
    except:
        return None

def decode_date(date_input):
    if isinstance(date_input, str):
        return string_to_date(date_input)
    elif isinstance(date_input, int):
        return int_to_date(date_input)
    elif isinstance(date_input, datetime):
        return date_input
    else:
        return None

def int_to_datetime(datetime_int):
    return string_to_datetime(str(datetime_int), datetime_format = datetime_format_2)

def string_to_datetime(datetime_str, datetime_format = datetime_format_1):
    try:
        return datetime.strptime(datetime_str, datetime_format)
    except:
        return None

def decode_datetime(datetime_input):
    if isinstance(datetime_input, str):
        return string_to_datetime(datetime_input)
    elif isinstance(datetime_input, int):
        return int_to_datetime(datetime_input)
    elif isinstance(datetime_input, datetime):
        return string_to_datetime(datetime_input)
    else:
        return None

def day_duration(start_str, end_str):
    start = string_to_date(start_str)
    end = string_to_date(end_str)
    return (end - start).days + 1

def time_parse(time_str):
    day_time = time_str.split(' ')[0]
    hour_time = time_str.split(' ')[1]
    if len(hour_time) == 5:
        hour_time = '0' + hour_time
    return int_to_datetime(day_time+hour_time)

def symbol_str(symbol):
    return symbol.upper().replace("SZ", 'SZ.').replace("SH", 'SH.')

def columns_str(column):
    return column.replace(".", '')
