import json
import random
import aiohttp
from aiohttp_socks import ProxyConnector
from aiohttp import ClientSession
from datetime import datetime as datett, timedelta, timezone
from dateutil import parser
import logging
from typing import AsyncGenerator, List
import tldextract as tld
import hashlib
import asyncio

from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
)

# Default parameters
DEFAULT_OLDNESS_SECONDS = 3600 * 3  # 3 hours
DEFAULT_MAXIMUM_ITEMS = 10
DEFAULT_MIN_POST_LENGTH = 10
BASE_TIMEOUT = 30

USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
]

logging.basicConfig(level=logging.INFO)

async def fetch_data(session, url, headers=None):
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json(content_type=None)
            else:
                logging.error(f"Error fetching data: {response.status} {await response.text()}")
                return []
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return []

def convert_to_standard_timezone(_date):
    dt = parser.parse(_date)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")

def is_within_timeframe_seconds(dt_str, timeframe_sec):
    dt = datett.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.00Z")
    dt = dt.replace(tzinfo=timezone.utc)
    current_dt = datett.now(timezone.utc)
    time_diff = current_dt - dt
    return abs(time_diff) <= timedelta(seconds=timeframe_sec)

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
    else:
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
    return max_oldness_seconds, maximum_items_to_collect, min_post_length

async def create_session_with_proxy(ip, port):
    connector = ProxyConnector.from_url(f"socks5://{ip}:{port}", rdns=True)
    session = ClientSession(connector=connector, headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=BASE_TIMEOUT)
    logging.info(f"Created session with proxy {ip}:{port}")
    return session, f"{ip}:{port}"

async def fetch_and_process(session, feed_url, max_oldness_seconds, min_post_length, maximum_items_to_collect):
    data = await fetch_data(session, feed_url)
    if not data:
        return []

    sorted_data = sorted(data, key=lambda x: x["pubDate"], reverse=True)
    sorted_data = [entry for entry in sorted_data if is_within_timeframe_seconds(convert_to_standard_timezone(entry["pubDate"]), max_oldness_seconds)]
    sorted_data = random.sample(sorted_data, int(len(sorted_data) * 0.3))

    items = []
    for entry in sorted_data:
        if random.random() < 0.25:
            continue

        pub_date = convert_to_standard_timezone(entry["pubDate"])

        if is_within_timeframe_seconds(pub_date, max_oldness_seconds):
            sha1 = hashlib.sha1()
            author = entry["creator"][0] if entry.get("creator") else "anonymous"
            sha1.update(author.encode())
            author_sha1_hex = sha1.hexdigest()

            content_article_str = entry.get("content") or entry.get("description") or entry["title"]
            domain_str = tld.extract(entry["source_url"] or "unknown").registered_domain
            
            new_item = Item(
                content=Content(str(content_article_str)),
                author=Author(str(author_sha1_hex)),
                created_at=CreatedAt(pub_date),
                title=Title(entry["title"]),
                domain=Domain(str(domain_str)),
                url=Url(entry["link"]),
                external_id=ExternalId(entry["article_id"])
            )

            if len(new_item.content) >= min_post_length and len(items) < maximum_items_to_collect:
                items.append(new_item)
        else:
            break
    return items

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    feed_url = 'https://raw.githubusercontent.com/user1exd/rss_realtime_feed/main/data/feed.json'
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    logging.info(f"[News stream collector] Fetching data from {feed_url} with parameters: {parameters}")
    
    proxies = load_proxies('/exorde/ips.txt')
    sessions = await asyncio.gather(*[create_session_with_proxy(ip, port) for ip, port in proxies])

    try:
        tasks = [fetch_and_process(session, feed_url, max_oldness_seconds, min_post_length, maximum_items_to_collect) for session, _ in sessions]
        results = await asyncio.gather(*tasks)

        yielded_items = 0
        for items in results:
            for item in items:
                if yielded_items >= maximum_items_to_collect:
                    return
                yield item
                yielded_items += 1
    finally:
        for session, _ in sessions:
            await session.close()
            await asyncio.sleep(0.1)  # Add delay between each request
    logging.info(f"[News stream collector] Done.")

def load_proxies(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    proxies = []
    for line in lines:
        ip_port = line.strip().split('=')[1]
        ip, port = ip_port.split(':')
        proxies.append((ip, port))
    return proxies

# Ensure you have 'aiohttp_socks' installed to use the ProxyConnector.
# You can install it using pip: pip install aiohttp_socks
