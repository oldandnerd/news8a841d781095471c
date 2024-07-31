import aiohttp
import hashlib
import logging
import asyncio
from datetime import datetime as datett, timedelta, timezone
from dateutil import parser
from typing import AsyncGenerator
import tldextract as tld
import random

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

DEFAULT_OLDNESS_SECONDS = 3600*3  # 3 hours
DEFAULT_MAXIMUM_ITEMS = 10
DEFAULT_MIN_POST_LENGTH = 10

async def fetch_data(url, headers=None):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response_text = await response.text()
                if response.status == 200:
                    json_data = await response.json(content_type=None)  # Manually handle content type
                    return json_data
                else:
                    logging.error(f"Error fetching data: {response.status} {response_text}")
                    return []
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return []

def convert_to_standard_timezone(_date):
    dt = parser.parse(_date)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")

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

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    feed_url = f'http://192.227.159.4:8000/get_articles?size={maximum_items_to_collect}'  # Adjust this URL to match your server's endpoint
    data = await fetch_data(feed_url)

    logging.info(f"[News stream collector] Fetching data from {feed_url} with parameters: {parameters}")
    yielded_items = 0

    logging.info(f"[News stream collector] Total data received: {len(data)}")

    if not data:
        logging.warning("Fetched data is empty. Retrying after 60 seconds.")
        await asyncio.sleep(60)
        return

    sorted_data = random.sample(data, int(len(data) * 0.3))

    successive_old_entries = 0

    for entry in sorted_data:
        if random.random() < 0.25:
            continue
        logging.info(f"[News stream collector] Processing entry: {entry['title']} - {entry['pubDate']} - {entry['source_url']}")
        if yielded_items >= maximum_items_to_collect:
            break

        pub_date = convert_to_standard_timezone(entry["pubDate"])

        sha1 = hashlib.sha1()
        author = entry["creator"][0] if entry.get("creator") else "anonymous"
        sha1.update(author.encode())
        author_sha1_hex = sha1.hexdigest()

        content_article_str = ""
        # if no content (null), then if description is not null, use it as content
        # else if description is null as well, then use title as content
        if entry.get("content"):
            content_article_str = entry["content"]
        elif entry.get("description"):
            content_article_str = entry["description"]
        else:
            content_article_str = entry["title"]

        domain_str = entry["source_url"] if entry.get("source_url") else "unknown"
        # remove the domain using tldextract to have only the domain name
        # e.g. http://www.example.com -> example.com
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

        yielded_items += 1
        yield new_item

    logging.info(f"[News stream collector] Done.")
