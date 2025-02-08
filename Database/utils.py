from datetime import datetime
import pytz

def get_hour():
    tz = pytz.timezone('America/Mexico_City')
    current_time = datetime.now(tz)
    time_format = '%B %d %Y %H:%M:%S'
    return current_time.strftime(time_format)

def logger(func):
    print(func.__name__)