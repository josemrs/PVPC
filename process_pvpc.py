#!/usr/bin/env python3

import aiohttp
import asyncio
import json
import logging
import pprint
import sys
from copy import deepcopy
from datetime import date, timedelta
from queue import Queue
from pprint import pprint

API_DATE_FORMAT = '%Y-%m-%d'

REQUEST_URL = 'https://api.esios.ree.es/archives/70/download_json?locale=es&date=:date:'

REQUEST_HEADERS = {"User-Agent": "FoX-PVPC-Processor",
                   "Accept": "application/json; application/vnd.esios-api-v2+json",
                   "Authorization": "Token token=\":token:\"" }
"""Requesting API version 2 and JSON format"""

logger = logging.getLogger("Main")
logger.setLevel(logging.DEBUG)
file_log_hanlder = logging.FileHandler('console.log')
console_log_handler = logging.StreamHandler()

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_log_hanlder.setFormatter(formatter)
console_log_handler.setFormatter(formatter)

file_log_hanlder.setLevel(logging.INFO)
console_log_handler.setLevel(logging.ERROR)
logger.addHandler(file_log_hanlder)
#logger.addHandler(console_log_handler)

class Tariff():
    DEFAULT = "GEN" 
    TWO_PERIODS = "NOC"
    ELECTRIC_VEHICLE = "VHC"

async def fetch_pvpc(session, url):
    """
    Uses HTTP GET method to get the PVPC archive from REE.

    :param session: HTTP session to use

    :return: Dictionary with response JSON parsed
    :rtype: Dict
    """
    response_json = None
    logger.info("Fetching URL {}".format(url))
    try:
        async with session.get(url, headers=REQUEST_HEADERS) as response:
            response.raise_for_status()
            response_json = json.loads(await response.text())
    except Exception:
        logger.exception("Unexpected exception getting the issues")

    return response_json


def extract_dha_prices(response_json, tariff):
    pvpc = response_json['PVPC']

    prices = {}
    for entry in pvpc:
        time = entry['Hora'].replace('-', ':')
        price = float(entry[tariff].replace(',', '.'))
        logger.info("Date: %s Time: %s Tariff: %s Price: %.4f" % (entry['Dia'], time, tariff, price))

        prices[time] = price

    return prices
    

async def main():

    auth = ()
    if len(sys.argv) != 2:
        print("Usage: python3 process_pvpc.py\n"
              "\t<token> HTTP Auth token")
        sys.exit(1)

    tomorrows_date = date.today() + timedelta(days=1)
    tomorrows_date = date.today()
    url = REQUEST_URL
    url = url.replace(':date:', tomorrows_date.strftime(API_DATE_FORMAT))
    logger.info("URL: %s" % (url))

    token = sys.argv[1]
    REQUEST_HEADERS['Authorization'] = REQUEST_HEADERS['Authorization'].replace(':token:', token)

    response = None

    tasks = []  
    # Create the HTTP client session and perform all the requests using tasks
    async with aiohttp.ClientSession() as session:
        task = asyncio.ensure_future(fetch_pvpc(session, url))
        tasks.append(task)
        response = await asyncio.gather(*tasks)

    prices = extract_dha_prices(response[0], Tariff.TWO_PERIODS)
    minimum_price_time = min(prices, key=prices.get)
    minimum_price = prices[minimum_price_time] / 1000
    logger.info("Minimum price %.6f at %s" % (minimum_price, minimum_price_time))

loop = asyncio.get_event_loop()
loop.run_until_complete(main())



