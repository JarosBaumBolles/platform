"""Common utils to work with dates"""
import uuid
from datetime import datetime, time, timedelta
from decimal import Decimal
from typing import Optional, Tuple, Union
import pendulum as pdl
import pytz
from pendulum.datetime import DateTime
from pendulum.tz.timezone import FixedTimezone, Timezone
from common.cache import lru_cache_expiring
from common.date_utils.constants import (
    DEFAULT_LOCAL_TIMEZONE,
    SIXTY,
)
from common.logging import Logger
from common import settings as CFG

LOGGER = Logger(
    name="Date Utils",
    level="DEBUG",
    description="Date Utils",
    trace_id=uuid.uuid4(),
)

TRUNCATE_MAPPER = {
    "year": {
        "month": 1,
        "day": 1,
        "hour": 0,
        "minute": 0,
        "second": 0,
        "microsecond": 0,
    },
    "month": {
        "day": 1,
        "hour": 0,
        "minute": 0,
        "second": 0,
        "microsecond": 0,
    },
    "day": {
        "hour": 0,
        "minute": 0,
        "second": 0,
        "microsecond": 0,
    },
    "hour": {
        "minute": 0,
        "second": 0,
        "microsecond": 0,
    },
    "minutes": {
        "second": 0,
        "microsecond": 0,
    },
    "seconds": {
        "microsecond": 0,
    },
}


class DateParseException(Exception):
    """Exception class specific to this package."""


def parse(
    value: Optional[
        Union[
            DateTime,
            datetime,
            int,
            float,
            Decimal,
            str,
        ]
    ] = None,
    dt_format: Optional[str] = None,
    tz_info: Union[str, FixedTimezone, Timezone] = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
    day_first: bool = False,
    year_first: bool = True,
    strict: bool = False,
) -> DateTime:
    """Convert date from value to specific Date"""

    pdl_tz = parse_timezone(tz_info)

    if not value:
        return pdl.now(tz=pdl_tz)

    if isinstance(value, DateTime):
        return value

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=pytz.timezone("UTC"))
        return pdl.from_format(
            value.strftime("%Y-%m-%dT%H:%M:%S%Z"), "YYYY-MM-DD[T]HH:mm:ssz"
        )

    if isinstance(value, (float, Decimal)):
        value = int(value)

    if isinstance(value, int):
        return pdl.from_timestamp(value, pdl_tz)

    if not dt_format:

        return _pdl_parse(
            value,
            tz_info=pdl_tz,
            day_first=day_first,
            year_first=year_first,
            strict=strict,
        )

    try:

        value = pdl.from_format(value, dt_format)

        if value.timezone is None:
            if tz_info:
                value.replace(tzinfo=pdl_tz)

    except (ValueError, SyntaxError, TypeError) as err:
        LOGGER.warning(  # pylint:disable=logging-fstring-interpolation
            f'WARNING: DATE PARSING: Cannot parse "{value}" date using '
            f'pattern "{dt_format}"  due to the {err}. '
            "Try to parse using universal parser"
        )
        value = _pdl_parse(
            value,
            tz_info=pdl_tz,
            day_first=day_first,
            year_first=year_first,
            strict=strict,
        )
    return value


def _pdl_parse(
    parse_date: Optional[
        Union[
            DateTime,
            datetime,
            int,
            float,
            Decimal,
            str,
        ]
    ] = None,
    tz_info: Optional[Union[str, FixedTimezone, Timezone]] = None,
    day_first: bool = False,
    year_first: bool = True,
    strict: bool = False,
) -> DateTime:

    try:
        value = pdl.parse(
            parse_date, day_first=day_first, year_first=year_first, strict=strict
        )
        return value.replace(tzinfo=tz_info) if tz_info else value
    except (pdl.parsing.exceptions.ParserError, ValueError, TypeError) as err:
        raise DateParseException from err


@lru_cache_expiring(maxsize=512, expires=3600)
def parse_timezone(
    zone: Optional[Union[str, FixedTimezone, Timezone]] = None, raise_error: bool = True
) -> Optional[Union[FixedTimezone, Timezone]]:
    """Parse Time zone"""

    if zone:
        if isinstance(zone, (FixedTimezone, Timezone)):
            return zone

        try:
            return pdl.timezone(zone)
        except (
            pdl.tz.zoneinfo.exceptions.InvalidTimezone,
            ValueError,
            TypeError,
            AttributeError,
        ) as err:
            if raise_error:
                raise DateParseException from err
            LOGGER.error(  # pylint:disable=logging-fstring-interpolation
                f'ERROR: cannot retrieve timezone for the given name - "{zone}". '
                f'Return local timezone instead "{CFG.DEFAULT_LOCAL_TIMEZONE_NAME}"'
            )
            return parse_timezone(CFG.DEFAULT_LOCAL_TIMEZONE_NAME)
    return None


@lru_cache_expiring(maxsize=512, expires=3600)
def get_week_of_year(
    date_value: Optional[
        Union[
            DateTime,
            datetime,
            int,
            float,
            Decimal,
            str,
        ]
    ] = None,
    timezone: str = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
) -> int:
    """Convert date to year and week number"""
    date_value = parse(date_value, tz_info=timezone)
    return int(date_value.week_of_year)


@lru_cache_expiring(maxsize=512, expires=3600)
def get_year(
    date_value: Optional[
        Union[
            datetime,
            int,
            float,
            Decimal,
            str,
        ]
    ] = None,
    timezone: str = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
) -> int:
    """Get year from the given date"""
    date_value = parse(date_value, tz_info=timezone)
    return int(date_value.year)


@lru_cache_expiring(maxsize=512, expires=3600)
def get_month_number(
    date_value: Optional[
        Union[
            datetime,
            int,
            float,
            Decimal,
            str,
        ]
    ] = None,
    timezone: str = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
) -> int:
    """Get month number from the given date"""
    date_value = parse(date_value, tz_info=timezone)
    return int(date_value.month)


@lru_cache_expiring(maxsize=512, expires=3600)
def last_day_of_month(
    date_value: Optional[
        Union[
            datetime,
            int,
            float,
            Decimal,
            str,
        ]
    ] = None,
    timezone: str = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
) -> DateTime:
    """Get last day for maonth in the given date"""
    date_value = parse(date_value, tz_info=timezone).datetime
    return date_value.last_of("month")


@lru_cache_expiring(maxsize=512, expires=3600)
def first_day_of_month(
    date_value: Optional[
        Union[
            datetime,
            int,
            float,
            Decimal,
            str,
        ]
    ] = None,
    timezone: str = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
) -> DateTime:
    """Get first day for maonth in the given date"""
    date_value = parse(date_value, tz_info=timezone).datetime
    return date_value.start_of("month")


@lru_cache_expiring(maxsize=512, expires=3600)
def week_to_date_range(
    year: int,
    week: int,
    timezone: str = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
    truncate_month: bool = False,
    month: Optional[int] = None,
):
    """Get the list of closest monday and sunday dates for the given date"""
    curr_date = DateTime(year=year, month=1, day=1, tzinfo=timezone).add(weeks=week)

    monday = curr_date.previous(pdl.MONDAY)
    sunday = curr_date.next(pdl.SUNDAY)

    if truncate_month:
        if sunday.month != sunday.month:
            if monday.month == month:
                sunday = monday.last_of("month")
            else:
                monday = sunday.first_of("month")

    return monday, sunday


def truncate(date_value: DateTime, level: str = "hour") -> DateTime:
    """Trunc valuses fror the given date based on the geiven leval"""
    trunc = TRUNCATE_MAPPER.get(level)
    if not trunc:
        return date_value

    return date_value.replace(**trunc)


def date_range(
    start_date: Optional[DateTime] = None,
    end_date: Optional[DateTime] = None,
    range_unit: str = "hours",
    except_values: Optional[Tuple[DateTime]] = None,
) -> Union[pdl.Period, list]:
    """Generate list of dates with the given period"""
    if start_date > end_date:
        LOGGER.warning(
            "WARNING: Generate date range: Start date is greater then end date. Swap"
        )

    period = pdl.Period(start_date, end_date, absolute=True)
    range_val = period.range(range_unit)
    if not except_values:
        return range_val

    return list(filter(lambda x: x not in except_values, range_val))


def date_range_in_past(
    start_date: Optional[DateTime] = None,
    years: int = 0,
    months: int = 0,
    weeks: int = 0,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
    microseconds: int = 0,
    trunc_level: Optional[str] = None,
    range_unit: str = "hours",
    except_values: Optional[Tuple[DateTime]] = None,
) -> pdl.Period:
    """Create a list of dates from the specified date to the past with the
    specified depth
    """
    if not isinstance(start_date, DateTime):
        start_date = pdl.now(tz=DEFAULT_LOCAL_TIMEZONE)

    end_date = truncate(start_date, level=trunc_level)

    start_date = start_date.subtract(
        years=years,
        months=months,
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        microseconds=microseconds,
    )

    return date_range(
        start_date=start_date,
        end_date=end_date,
        range_unit=range_unit,
        except_values=except_values,
    )


def format_date(
    date_value: Optional[
        Union[
            datetime,
            int,
            float,
            Decimal,
            str,
        ]
    ] = None,
    dt_format: str = "YYYY-m-d",
    tz_obj: str = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
    locale: str = "en",
) -> str:
    """Format date to string in specified"""
    value = parse(date_value, tz_info=tz_obj)

    try:
        return value.format(dt_format, locale=locale)
    except AttributeError as err:
        raise DateParseException from err


@lru_cache_expiring(maxsize=512, expires=3600)
def get_month_name(
    dt_value: Optional[
        Union[
            datetime,
            DateTime,
            int,
            float,
            Decimal,
            str,
        ]
    ] = None,
    timezone: str = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
) -> str:
    """Get human redable month name"""
    return parse(dt_value, tz_info=timezone).format("MMMM")


@lru_cache_expiring(maxsize=512, expires=3600)
def humanize_seconds(value: Union[str, int, Decimal]) -> Union[time, timedelta]:
    """Convert seconds mount intu human redable string"""
    # assert isinstance(value, (int, Decimal)) or (
    #     isinstance(value, str) and value.isdecimal()
    # )

    if isinstance(value, str) and not value.isdecimal():
        raise ValueError(f'ERROR: THe given value "{value}" is not a number.')

    if not isinstance(value, Decimal):
        value = Decimal(str(value))

    minutes, seconds = divmod(value, SIXTY)
    hours, minutes = divmod(minutes, SIXTY)
    hours, minutes, seconds = map(int, (hours, minutes, seconds))

    if hours > 23:
        return timedelta(hours=hours, minutes=minutes, seconds=seconds, milliseconds=0)
    return time(hours, minutes, seconds, 0)


if __name__ == "__main__":
    LOGGER.debug("=" * 40)

    date = parse("2021-12-07T12:10:12EST", dt_format="YYYY-MM-DD[T]HH:mm:ssz")
    date = parse("2021-12-07T12:10:12EST")
    date = parse("2021-12-07T12:10:12EST", tz_info=pdl.timezone("UTC"))
    date = parse(datetime.now())
    date = parse(pdl.now())

    LOGGER.debug(">" * 40)
