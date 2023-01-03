from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
from datetime import datetime
import re


def string_to_delta(s):
    value = int(re.search(r'\d+', s).group())
    if "day" in s:
        value = value*1
    elif "month" in s:
        value = value *30   
    date_ago = (datetime.now() - timedelta(days=value)).date() 
    str_date= date_ago.strftime("%b %d, %Y")
    return str_date

def get_time_range(n_days):
    days = []
    today = date.today()
    for i in range(0,n_days):
        past = today - timedelta(days=i)
        days.append(past.strftime("%b %d, %Y"))
        days.append(str(i)+' days ago')
    return days

def to_datetime(s):
    date = datetime.strptime(s,"%b %d, %Y")
    return date.strftime("%Y-%m-%d")