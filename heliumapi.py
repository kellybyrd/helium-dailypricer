import atexit
import json
import logging
import math
import sqlite3
import statistics
from datetime import date, datetime, timedelta

import requests
from dateutil.parser import parse as dateparse

log = logging.getLogger(__name__)

BONES_PER_HNT = 100000000
API_URL = "https://api.helium.io/v1"

_DB = None
_DB_FILE = "cache.sqlite"


def _close_db():
    if _DB is not None:
        _DB.close()


_DB = sqlite3.connect(_DB_FILE)
cur = _DB.cursor()
cur.execute(
    "CREATE TABLE IF NOT EXISTS OraclePrices ("
    "block INTEGER PRIMARY KEY, "
    "timestamp TEXT NOT NULL, "
    "price_bones INTEGER DEFAULT 0);"
)

cur.execute(
    "CREATE TABLE IF NOT EXISTS DailyRewards ("
    "timestamp TEXT NOT NULL, "
    "address TEXT NOT NULL, "
    "sum_bones INTEGER NOT NULL, "
    "UNIQUE(timestamp, address) "
    ");"
)

atexit.register(_close_db)
# END MODULE INIT


def _daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def _api_request(url, query_params=dict()):
    """
    Takes a helium api URL which returns JSON and may have paged results as
    described in the "Cursors" section here:
    https://docs.helium.com/api/blockchain/introduction

    Merges the "data" keys from all pages

    Returns
    List of dicts from merged results.

    Note: This assumes all the repsonse json has "data" and optionaly
          "cursor" top level keys. So far has been true
    """
    ret = list()
    cursor = None

    # repeat-until cursor is None
    while True:
        if cursor is not None:
            query_params["cursor"] = cursor

        try:
            resp = requests.get(url, params=query_params)
            resp.raise_for_status()
            json = resp.json()
            data = json.get("data")
            if type(data) is list:
                ret.extend(data)
                cursor = json.get("cursor")
                log.debug(f"New cursor is : {cursor}")
            else:
                # Not a JSON array, so not a paged result
                ret = data
                cursor = None

        except Exception as ex:
            log.error(f"Error: {ex}")
            # This will break us out of the while loop
            cursor = None

        if cursor is None:
            break

    return ret


def _cache_oracle_prices(until_ts=None):
    """
    A copy of _api_request() that stops paging at until_ts and also
    saves each page to the DB as it goes.

    Returns
    List of dicts from merged results.

    Note: This assumes all the repsonse json has "data" and optionally
          "cursor" top level keys. So far has been true
    """

    # TODO: Factor this and _api_request out into some common bit.
    ret = list()
    cursor = None
    query_params = {}
    url = f"{API_URL}//oracle/prices"

    if until_ts is None:
        until_ts = datetime.min

    # repeat-until cursor is None or until_block is reached
    while True:
        if cursor is not None:
            query_params["cursor"] = cursor
        try:
            resp = requests.get(url, params=query_params)
            resp.raise_for_status()
            json = resp.json()
            data = json.get("data")
            if type(data) is list:
                ret.extend(data)
                _db_price_put_many(data)
                min_ts = dateparse(min(data, key=lambda x: x["timestamp"])["timestamp"])
                log.debug(f"Oracle min_ts is now {min_ts}")
                if min_ts <= until_ts:
                    log.debug(
                        f"Not continuing because found block {min_ts} is "
                        f"lower than {until_ts}"
                    )
                    cursor = None
                else:
                    cursor = json.get("cursor")
                    log.debug(f"New cursor is : {cursor}")
            else:
                # Not a JSON array, so not a paged result
                ret = data
                cursor = None

        except Exception as ex:
            log.error(f"Error: {ex}")
            # This will break us out of the while loop
            cursor = None

        if cursor is None:
            break

    return ret


def _db_price_at_time(as_of_time):
    """
    Give the most recent Oracle prices record right before as_of_time, this will be the
    effective price at that timestamps

    Returns:
    A DB record containing the price
    """

    ret = dict()
    cur = _DB.cursor()
    cur.execute(
        "SELECT block, max(timestamp), price_bones FROM OraclePrices WHERE timestamp <= :time ",
        {"time": as_of_time},
    )
    price = cur.fetchone()

    ret["block"] = price[0]
    ret["timestamp"] = price[1]
    ret["price"] = price[2]

    return ret


def _db_price_put_many(prices):
    """
    Save a several reward records to the DB.

    I probably should be doing this with executemany(), but it felt like transforming
    a large rewards list-of-dicts into a list of properly ordered tuples was as much
    work as calling execute() a bunch between commit()
    """
    # Break rows up into chunks of 50 rows. See:
    # https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
    CHUNK_SIZE = 50
    chunks = [prices[i : i + CHUNK_SIZE] for i in range(0, len(prices), CHUNK_SIZE)]
    cur = _DB.cursor()

    for chunk in chunks:
        # There's a constraint on block, but code that calls this gets blocks from the
        # API that are already in the DB. We're using REPLACE here so we can be lazy and
        # not deal with CONSTRAINT violations.
        for r in chunk:
            cur.execute(
                "REPLACE INTO OraclePrices VALUES (:block, :timestamp, :price_bones)",
                {
                    "block": r["block"],
                    "timestamp": r["timestamp"],
                    "price_bones": r["price"],
                },
            )
        # commit per chunk, not per execute.
        _DB.commit()


def _db_price_put(block, timestamp, price):
    """
    Save an Oracle price to the db. Does not yet handle errors like an existing
    record
    """
    cur = _DB.cursor()
    cur.execute(
        "INSERT INTO OraclePrices VALUES (:block, :timestamp, :price)",
        {"block": block, "timestamp": timestamp, "price_bones": price},
    )
    _DB.commit()


def _db_price_max_time():
    """
    Get the timestamp of the most recent oracle price in the DB
    """
    ret = None

    cur = _DB.cursor()
    cur.execute("SELECT MAX(timestamp) FROM OraclePrices;")
    result = cur.fetchone()[0]
    if result is not None:
        ret = dateparse(result).date()

    return ret


def _db_reward_fetch(address, start, stop):
    """
    Fetch rewards from the cache filling of the cache from the REST API
    if needed.

    Returns:
    A list of reward records
    """
    ret = list()
    cur = _DB.cursor()
    cur.execute(
        "SELECT timestamp, address, sum_bones FROM DailyRewards "
        "WHERE address=:addr AND "
        "timestamp BETWEEN :start AND :stop "
        "ORDER BY timestamp ASC;",
        {"addr": address, "start": start.isoformat(), "stop": stop.isoformat()},
    )

    rewards = cur.fetchall()
    for r in rewards:
        tmp = dict()
        tmp["timestamp"] = r[0]
        tmp["address"] = r[1]
        tmp["sum"] = r[2]
        ret.append(tmp)

    return ret


def _db_reward_put_many(address, rewards):
    """
    Save a several reward records to the DB.

    I probably should be doing this with executemany(), but it felt like transforming
    a large rewards list-of-dicts into a list of properly ordered tuples was as much
    work as calling execute() a bunch between commit()
    """
    # Break rows up into chunks of 50 rows. See:
    # https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
    CHUNK_SIZE = 50
    chunks = [rewards[i : i + CHUNK_SIZE] for i in range(0, len(rewards), CHUNK_SIZE)]
    cur = _DB.cursor()

    for chunk in chunks:
        # There's a constraint on hash, but code that calls this intentionally overlaps
        # the times it fetches from the API with time should be in the DB in order to be
        # sure it doesn't miss anything. We're using REPLACE here so we can be lazy and
        # not deal with CONSTRAINT viloations.
        for r in chunk:
            cur.execute(
                "REPLACE INTO DailyRewards VALUES (:timestamp, :address, :sum_bones)",
                {
                    "timestamp": r["timestamp"],
                    "address": address,
                    "sum_bones": r["sum"],
                },
            )
        # commit per chunk, not per execute.
        _DB.commit()


def _db_reward_put(timestamp, address, sum_bones):
    """
    Save a daily reward total to the db. Does not yet handle errors like an
    existing record
    """
    cur = _DB.cursor()
    # There's a constraint on hash, but code that calls this intentionally overlaps
    # the times it fetches from the API with time should be in the DB in order to be
    # sure it doesn't miss anything. We're using REPLACE here so we can be lazy and
    # not deal with CONSTRAINT viloations.
    cur.execute(
        "REPLACE INTO DailyRewards VALUES (:timestamp, :address, :sum_bones)",
        {"timestamp": timestamp, "address": address, "sum_bones": sum_bones},
    )
    _DB.commit()


def _db_reward_max_min(address):
    """
    Given an address, get the oldest and newest reward timestamps.
    """
    ts_min = None
    ts_max = None

    cur = _DB.cursor()
    cur.execute(
        "SELECT MIN(timestamp) FROM DailyRewards WHERE address=:address;",
        {"address": address},
    )
    result = cur.fetchone()[0]
    if result is not None:
        ts_min = dateparse(result).date()
    cur.execute(
        "SELECT MAX(timestamp) FROM DailyRewards WHERE address=:address;",
        {"address": address},
    )
    result = cur.fetchone()[0]
    if result is not None:
        ts_max = dateparse(result).date()

    return (ts_min, ts_max)


def _api_reward_fetch(address, start, stop):
    # Handle paged results and put items in the DB
    ret = list()
    url = f"{API_URL}/hotspots/{address}/rewards/sum"
    params = dict()
    params["max_time"] = stop.isoformat()
    params["min_time"] = start.isoformat()
    params["bucket"] = "day"

    try:
        ret = _api_request(url, params)
    except:
        pass

    log.debug(f"_api_reward_fetch: putting {len(ret)} records in the DB")
    _db_reward_put_many(address, ret)

    return ret


def oracle_price_for_day(day):
    """
    Return the closing Helium oracle price for a given day. This is the Oracle price
    at just before midnight the next day. Ex:
      If day is 2021-03-27, we will return the price as of '2021-03-27T23:59:59.999Z'
    """
    END_OF_DAY = timedelta(days=2, microseconds=-1)

    # The oracle price API doesn't take a time range. It is a paged result of
    # JSON objects in what appears to be descending timestamp order.
    #
    # The overall strategy is to get the most recent block in the DB, then fetch
    # from the API, paging thru results until we reach that max DB block. Then
    # put these results in the DB. The net result is we end of with all oracle
    # prices from the beginning of time, and then keep adding new prices every
    # time this is called, ensuring no time gaps in the DB.
    #
    db_max_ts = _db_price_max_time()
    if day > db_max_ts:
        log.debug(f"Fetching Oracle prices since {db_max_ts}")
        _cache_oracle_prices(db_max_ts)

    # The DB now covers the time range we need, so fetch prices from there.
    ret = _db_price_at_time(day + END_OF_DAY)["price"]
    if ret is None:
        ret = 0

    return ret


def hotspot_earnings(address, start, stop):
    """
    Get all earnings [start, end) for the given hotspot
    address.

    Returns
    A list of dicts, one dict for each reward.

    Will return an empty list if the address is not found or no earnings
    were found.
    """
    ONE_SEC = timedelta(
        seconds=1
    )  # Added to API fetch times to ensure we overlap a bit

    # The overall strategy is to ensure no holes in the date range of the db cache.
    # To do this we always try to extend the cache, even if the start/stop params do
    # not overlap with the cached data.
    # * Get db_min and db_max
    # * If start < db_min: api_fetch for start to db_min(+1sec)
    #   Insert these into db. At this point db range is start to db_max
    # * If stop > db_max: api_fetch for db_max(-1sec) to stop
    #   Insert these into db. At this point db range will be at least start to stop.
    # * Read start to stop from DB
    (db_min_ts, db_max_ts) = _db_reward_max_min(address)
    if db_min_ts is None:
        # Nothing in the DB yet
        log.debug(f"DB: rewards empty")
        _api_reward_fetch(address, start, stop)
    else:
        if start < db_min_ts:
            # Need data earlier than range in db
            log.debug(f"DB: rewards fetch before")
            _api_reward_fetch(address, start, db_min_ts + ONE_SEC)
        if stop > db_max_ts:
            # Need data later than range in db
            log.debug(f"DB: rewards fetch after")
            _api_reward_fetch(address, db_max_ts - ONE_SEC, stop)

    # The DB now covers the time range we need, so fetch it from there.
    return _db_reward_fetch(address, start, stop)
