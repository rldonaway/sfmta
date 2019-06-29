import re

dt_regexp = "^201\d{1}-[01]\d-[0123]\d [012]\d:[0-5]\d:[0-5]\d$"

def validate_date_input(datetime, time_window):
    error = None

    if not datetime:
        error = "Date and time are required."

    if not re.search(dt_regexp, datetime):
        error = "Date and time must be in the format YYYY-MM-DD hh:mm:ss and be between 2013-01-01 and 2015-12-28."

    if not time_window:
        error = "Time window is required."

    try:
        time_window_int = int(time_window)
        if not (1 <= time_window_int <= 120):
            error = "Time window must be between 1 and 120."
    except ValueError:
        error = 'Please enter an integer value for the time window.'

    return error


def validate_time_input(hour_of_day, min_stop, max_stop):
    error = None

    if not hour_of_day:
        error = "Hour of day is required."

    try:
        hour_of_day_int = int(hour_of_day)
        if not (0 <= hour_of_day_int <= 23):
            error = "Hour of day must be between 0 and 23."
    except ValueError:
        error = 'Please enter an integer value for the hour of day.'

    try:
        min_stop_int = int(min_stop)
        if not (min_stop_int >= 0):
            error = "If entered, minimum stop length must be positive."
    except ValueError:
        error = 'Please enter an integer value for the minimum stop length.'

    try:
        max_stop_int = int(max_stop)
        if not (max_stop_int >= 0):
            error = "If entered, maximum stop length must be positive."
    except ValueError:
        error = 'Please enter an integer value for the maximum stop length.'

    return error, hour_of_day_int, min_stop_int, max_stop_int


