from calendar import month
from dbm import ndbm
from tokenize import Name
from dateutil.relativedelta import relativedelta
import datetime
import pandas as pd

REQUEST_TYPES = [
    'MTD',
    'PM',
    'Custom',
    'CustomMonth'
]

def date_dt(date_str):
  return datetime.date.fromisoformat(date_str)

def date_str (date_dt: datetime.date) -> str:
    return date_dt.strftime("%Y-%m-%d")

def Date_Interval_Start_Str (date_str, interval):
    date = datetime.date.fromisoformat(date_str)
    if interval == 'day':
        r = date
    elif interval == 'week':
        r = date - pd.DateOffset(days=(date.isoweekday()-1))
    elif interval == 'month':
        r = date - pd.DateOffset(days=(date.day - 1))
    elif interval == 'quarter':
        month_of_quarter = (date.month - 1) // 3 + 1
        r = datetime.datetime(
            year=date.year,
            month = (3*month_of_quarter)-2, day = 1
        )
    elif interval == 'year':
        r = datetime.datetime(year=date.year, month = 1, day = 1)
    return r.date().isoformat()

def Date_Interval_End_Str (date_str, interval):
    date = datetime.date.fromisoformat(date_str)
    if interval == 'day':
        r = date
    elif interval == 'week':
        r = date + pd.DateOffset(days=(7-date.isoweekday()))
    elif interval == 'month':
        if not pd.Timestamp(date).is_month_end:
            r = date + pd.offsets.MonthEnd()
        else:
            r = datetime.datetime.combine(
                date=date_dt(date_str), time=datetime.datetime.min.time())
    elif interval == 'quarter':
        if not pd.Timestamp(date).is_quarter_end:
            r = date + pd.offsets.QuarterEnd()
        else:
            r = datetime.datetime.combine(
                date=date_dt(date_str), time=datetime.datetime.min.time())
    elif interval == 'year':
        if not pd.Timestamp(date).is_year_end:
            r = date + pd.offsets.YearEnd()
        else:
            r = datetime.datetime.combine(
                date=date_dt(date_str), time=datetime.datetime.min.time())
    return r.date().isoformat() 

def shift_date_str(date_str, days=0, weeks=0, months=0, years=0):
  shifted_dt = shift_date_dt(
    date_dt(date_str=date_str), days=days, weeks=weeks, months=months,
    years=years)
  shifted_str = shifted_dt.isoformat()

  return shifted_str

def shift_date_dt(date_dt, days=0, weeks=0, months=0, years=0):
  shifted_dt = date_dt + relativedelta(
    days=days, weeks=weeks, months=months, years=years)

  return shifted_dt


def split_month_buckets (start: str, end: str, quantity:int) -> list:
    original_start = pd.to_datetime(start)
    original_end = pd.to_datetime(end)
    interval = pd.DateOffset(months=quantity)
    start = original_start
    buckets = []
    while start < original_end:
        end = min(start + interval - pd.DateOffset(days=1), original_end)
        buckets.append((start, end))
        start = end + pd.DateOffset(days=1)
    res = [[date_str(item[0]), date_str(item[1])] for item in buckets]
    return res

def set_start_end_interval (
    requestType:str = 'MTD',
    start:str=None, 
    end:str=None
) -> tuple:
    """
    Function to return the start and end for request based on the type of
    interval requested which are defined according to needs. Current intervals 
    include the following:
        - MTD (month-to-date): start to end of month, the request will only get
            available data to the point. Does not require to enter start/end.
        - PM (past month): start to end of previous month. Does not require to 
            enter start/end dates. 
        - Custom (any dates): custom setting for start and end dates. Must 
            provide at least Start Date. If end date is omitted current date 
            will be used.
        - CustomMonth (any date): start and end of requested month-year. Must 
            provide start date which can be any date of the month to get.
    Args:
        requestType (str, optional): default value of Month-To-Date. Must be
        in REQUEST_TYPES.
        start (str, optional): defaults to None. For use with certain Types.
            Behavior of function requires this parameter for several types.
        end (str, optional): defaults to None. For use with certain types.
    Returns:
        tuple: one dimensional tuple with Start date string and End date String
    """
    if requestType not in REQUEST_TYPES:
        raise NameError('Request type entered not in defined list.')
    if requestType == 'MTD':
        startDate = Date_Interval_Start_Str(
            date_str=pd.Timestamp.now().date().isoformat(), interval = 'month')
        endDate = Date_Interval_End_Str(
            date_str=pd.Timestamp.now().date().isoformat(), interval = 'month')
        if pd.Timestamp.now().date().isoformat() == startDate:
            startDate = shift_date_str(startDate,months=-1)
            endDate = shift_date_str(endDate, months=-1)
    elif requestType == 'PM':
        date = shift_date_str(
            date_str=pd.Timestamp.now().date().isoformat(), months=-1
        )
        startDate = Date_Interval_Start_Str(
            date_str=date, interval = 'month')
        endDate = Date_Interval_End_Str(
            date_str=date, interval = 'month')
    elif requestType == 'Custom':
        if start is None:
            raise NameError('With custom request type you must provide Start')
        startDate = start
        endDate = end if end is not None else pd.Timestamp.now().date().isoformat()
    elif requestType == 'CustomMonth':
        if start is None:
            raise NameError('With custom request type you must provide Start')
        startDate = Date_Interval_Start_Str(
            date_str=start, interval = 'month')
        endDate = Date_Interval_End_Str(
            date_str=start, interval = 'month')
    return (startDate, endDate)
