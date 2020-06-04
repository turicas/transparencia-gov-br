import calendar
import datetime


def today():
    date = datetime.datetime.now()
    return datetime.date(date.year, date.month, date.day)


def next_day(date):
    return date + datetime.timedelta(days=1)


def next_month(date):
    return datetime.date(
        year=date.year + (date.month // 12), month=(date.month % 12) + 1, day=date.day
    )


def next_year(date):
    if calendar.isleap(date.year):
        days_to_add = 366
    else:
        days_to_add = 365
    return date + datetime.timedelta(days=days_to_add)


def next_date(date, interval="daily"):
    from_interval = {"daily": next_day, "monthly": next_month, "yearly": next_year}

    return from_interval[interval](date)


def date_range(start, stop, interval="daily"):
    current = start
    while current < stop:
        yield current
        current = next_date(date=current, interval=interval)


def date_to_dict(date):
    return {"year": date.year, "month": date.month, "day": date.day}
