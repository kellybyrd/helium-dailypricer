import atexit
import json
import logging
import pickle

import requests

log = logging.getLogger(__name__)

BONES_PER_HNT = 100000000
API_URL = "https://api.helium.io/v1"

# Cache of retrieved Helium Oracle prices at specific blocks
_ORACLE_FILE = "oracle.pkl"


def _save_oracle():
    if len(_oracle_cache) > 0:
        with open(_ORACLE_FILE, "wb+") as f:
            pickle.dump(_oracle_cache, f)


try:
    with open(_ORACLE_FILE, "rb") as f:
        _oracle_cache = pickle.load(f)
except FileNotFoundError:
    _oracle_cache = {}

atexit.register(_save_oracle)


def _api_request(url, query_params={}):
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
                log.debug(f"Ret size is now: {len(ret)}")
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


def oracle_price_at_block(block):
    """
    Return the Helium API oracle price in bones at a given block
    """
    if block in _oracle_cache:
        log.debug(f"Cached price for block {block} is {_oracle_cache[block]}")
        return _oracle_cache[block]

    url = f"{API_URL}/oracle/prices/{block}"
    ret = _api_request(url)["price"]
    _oracle_cache[block] = ret
    log.debug(f"Returning price for block {block} is {_oracle_cache[block]}")
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
    ret = []
    url = f"{API_URL}/hotspots/{address}/rewards"
    params = dict()
    params["max_time"] = stop
    params["min_time"] = start

    try:
        ret = _api_request(url, params)
    except:
        pass

    return ret
