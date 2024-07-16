import aiohttp
import aiofiles
import hashlib
import logging
from datetime import datetime as datett, timedelta, timezone
from dateutil import parser
from typing import AsyncGenerator, List, Set
import tldextract as tld
import random
import json
import asyncio
from aiohttp_socks import ProxyConnector
from aiohttp import ClientSession, ClientTimeout

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

# Constants
DEFAULT_OLDNESS_SECONDS = 3600 * 3  # 3 hours
DEFAULT_MAXIMUM_ITEMS = 10
DEFAULT_MIN_POST_LENGTH = 10
DEFAULT_CHUNK_SIZE = 15  # Default number of articles to process per chunk
BASE_TIMEOUT = 30
FETCH_DELAY = 5  # Delay between fetch attempts
RESET_INTERVAL = 86400  # 24 hours in seconds

# User Agent List
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
]

# Logging configuration
logging.basicConfig(level=logging.INFO)

async def fetch_data(url, proxy, headers=None):
    """Fetch data from the URL using a specified proxy."""
    connector = ProxyConnector.from_url(f"socks5://{proxy[0]}:{proxy[1]}", rdns=True)
    timeout = ClientTimeout(total=BASE_TIMEOUT)
    
    try:
        async with ClientSession(connector=connector, headers=headers or {"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=timeout) as session:
            async with session.get(url) as response:
                response_text = await response.text()
                if response.status == 200:
                    json_data = await response.json(content_type=None)  # Manually handle content type
                    logging.info(f"Successfully fetched data with proxy {proxy}")
                    return json_data
                else:
                    logging.error(f"Error fetching data: {response.status} {response_text}")
                    return []
    except Exception as e:
        logging.error(f"Error fetching data with proxy {proxy}: {e}")
        return []

def convert_to_standard_timezone(_date):
    """Convert datetime string to standard timezone format."""
    dt = parser.parse(_date)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")

def is_within_timeframe_seconds(dt_str, timeframe_sec):
    """Check if the datetime is within the specified timeframe."""
    dt = datett.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.00Z")
    dt = dt.replace(tzinfo=timezone.utc)
    current_dt = datett.now(timezone.utc)
    time_diff = current_dt - dt
    return abs(time_diff) <= timedelta(seconds=timeframe_sec)

def read_parameters(parameters):
    """Read and set default parameters."""
    max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
    min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
    chunk_size = parameters.get("chunk_size", DEFAULT_CHUNK_SIZE)
    return max_oldness_seconds, maximum_items_to_collect, min_post_length, chunk_size

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    """Query the feed and yield items."""
    feed_url = 'https://raw.githubusercontent.com/user1exd/rss_realtime_feed/main/data/feed.json'
    proxies = [proxy async for proxy in load_proxies('/exorde/ips.txt')]
    max_oldness_seconds, maximum_items_to_collect, min_post_length, chunk_size = read_parameters(parameters)
    logging.info(f"[News stream collector] Fetching data from {feed_url} with parameters: {parameters}")

    yielded_items = 0
    queried_article_ids: Set[str] = set()  # Track queried article IDs
    last_reset_time = datett.now(timezone.utc)

    while yielded_items < maximum_items_to_collect:
        # Reset queried_article_ids every 24 hours
        current_time = datett.now(timezone.utc)
        if (current_time - last_reset_time).total_seconds() > RESET_INTERVAL:
            queried_article_ids.clear()
            last_reset_time = current_time
            logging.info("Reset queried_article_ids set.")

        proxy = random.choice(proxies)
        logging.info(f"Using proxy {proxy}")
        data = await fetch_data(feed_url, proxy)

        if not data:
            await asyncio.sleep(FETCH_DELAY)  # Add delay before retrying
            continue

        logging.info(f"Total data fetched: {len(data)}")

        # Sort data by publication date in descending order to process newest first
        sorted_data = sorted(data, key=lambda x: x["pubDate"], reverse=True)
        
        filtered_data = []
        for entry in sorted_data:
            pub_date = convert_to_standard_timezone(entry["pubDate"])
            if is_within_timeframe_seconds(pub_date, max_oldness_seconds):
                filtered_data.append(entry)
            else:
                logging.debug(f"Entry {entry['title']} with date {pub_date} is outside the timeframe.")

        logging.info(f"[News stream collector] Filtered data: {len(filtered_data)}")

        if len(filtered_data) == 0:
            logging.info("No data found within the specified timeframe and length.")
            await asyncio.sleep(FETCH_DELAY)  # Add delay before retrying
            continue

        # Process data in chunks
        for i in range(0, len(filtered_data), chunk_size):
            chunk = filtered_data[i:i + chunk_size]

            successive_old_entries = 0  # Reset the counter for each chunk

            for entry in chunk:
                if random.random() < 0.25:
                    continue
                if yielded_items >= maximum_items_to_collect:
                    break

                pub_date = convert_to_standard_timezone(entry["pubDate"])

                if is_within_timeframe_seconds(pub_date, max_oldness_seconds):
                    sha1 = hashlib.sha1()
                    author = entry["creator"][0] if entry.get("creator") else "anonymous"
                    sha1.update(author.encode())
                    author_sha1_hex = sha1.hexdigest()

                    content_article_str = ""
                    if entry.get("content"):
                        content_article_str = entry["content"]
                    elif entry.get("description"):
                        content_article_str = entry["description"]
                    else:
                        content_article_str = entry["title"]

                    domain_str = entry["source_url"] if entry.get("source_url") else "unknown"
                    domain_str = tld.extract(domain_str).registered_domain

                    new_item = Item(
                        content=Content(str(content_article_str)),
                        author=Author(str(author_sha1_hex)),
                        created_at=CreatedAt(pub_date),
                        title=Title(entry["title"]),
                        domain=Domain(str(domain_str)),
                        url=Url(entry["link"]),
                        external_id=ExternalId(entry["article_id"])
                    )

                    if len(new_item.content) >= min_post_length:
                        yielded_items += 1
                        logging.info(f"Processing entry: {entry['title']} - {entry['pubDate']} - {entry['source_url']}")
                        yield new_item
                    else:
                        logging.debug(f"Skipping entry {entry['title']} with content length {len(new_item.content)}")
                else:
                    # Log the entry being too old
                    dt = datett.strptime(pub_date, "%Y-%m-%dT%H:%M:%S.00Z")
                    dt = dt.replace(tzinfo=timezone.utc)
                    current_dt = datett.now(timezone.utc)
                    time_diff = current_dt - dt
                    logging.info(f"[News stream collector] Entry {entry['title']} is {abs(time_diff)} old: skipping.")

                    successive_old_entries += 1
                    if successive_old_entries >= 5:
                        logging.info(f"[News stream collector] Too many old entries. Stopping.")
                        break

            await asyncio.sleep(FETCH_DELAY)  # Add delay before processing the next chunk

    logging.info(f"[News stream collector] Done.")

async def load_proxies(file_path):
    """Load proxies from a file asynchronously."""
    async with aiofiles.open(file_path, 'r') as file:
        async for line in file:
            ip_port = line.strip().split('=')[1]
            ip, port = ip_port.split(':')
            yield (ip, port)

# Ensure you have 'aiohttp_socks' installed to use the ProxyConnector.
# You can install it using pip: pip install aiohttp_socks
