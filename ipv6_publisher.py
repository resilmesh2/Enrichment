import asyncio
import http
import logging
import os
from logging.handlers import RotatingFileHandler
from math import floor

from filelock import FileLock, Timeout
from settings import API_URL, EVENTS_DIR, PUBLISHER_LOG_FILE, log_format
from utils.api import (
    bulk_publish_messages,
    do_request,
    prepare_ips_to_publish, get_cached_events, set_cached_events
)
from utils.custom_ndjson_reader import CustomNDJSONReader
from utils.subscriberState import SubscriberState
from utils.utils import skip_current_file

"""
Enriches IPV6 events from NATS using Silent Push API
"""


logger = logging.getLogger("ipv6_publisher")
log_file_handler = RotatingFileHandler(
    PUBLISHER_LOG_FILE, maxBytes=100_000_000, backupCount=5
)
log_file_handler.setFormatter(log_format)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(log_file_handler)


def bulk_enrich_ipv6s(ipv6s: set) -> list:
    if not ipv6s:
        return []
    # _URI = API_URL + "explore/bulk/summary/ipv6?explain=1&scan_data=1"
    _URI = API_URL + "explore/bulk/summary/ipv6?explain=1"
    logger.info(f"Enriching {len(ipv6s)} IPv6s")
    ipv6s = list(set(ipv6s))
    enriched_data, status_code = do_request({"ips": ipv6s}, _URI, logger)
    if status_code == http.HTTPStatus.SERVICE_UNAVAILABLE:
        subscriberState.close_connection()
    if not enriched_data:
        logger.info(f"No enriched data: {enriched_data}")
    try:
        enriched_data = list(
            map(  # calculates Wazuh rule level based on SLP risk score
                lambda i:
                dict(i, wazuh_rule_level=floor((i.get('sp_risk_score') or 0) / 6.5)),
                enriched_data
            )
        )
    except AttributeError:
        logger.warning("skipping, attribute error")
    return enriched_data


async def enrich_ipv6s():
    while True:
        path = f"{EVENTS_DIR}outbound/ipv6s/"
        with os.scandir(path) as files:
            for file in files:
                lock = FileLock(path + file.name + ".lock", thread_local=False)
                if skip_current_file(file, lock):
                    continue
                ipv6s = list()
                messages_to_publish = list()
                try:
                    with lock.acquire(timeout=5):
                        with open(path + file.name) as f:
                            reader = CustomNDJSONReader(f)
                            for line in reader:
                                if not line:
                                    continue
                                ipv6s.append((line.get("source", {}) or {}).get("ip"))
                                ipv6s.append((line.get("destination", {}) or {}).get("ip"))
                                messages_to_publish.append(line)
                            if not ipv6s:
                                continue
                            enriched_cache, new_ipv6s = get_cached_events(set(ipv6s), logger)
                            enriched_ipv6s = bulk_enrich_ipv6s(set(new_ipv6s))
                            if enriched_cache:
                                enriched_ipv6s.extend(enriched_cache)
                            messages_to_publish = prepare_ips_to_publish(
                                enriched_ipv6s, messages_to_publish, logger
                            )
                            set_cached_events(enriched_ipv6s, logger)
                            await bulk_publish_messages(messages_to_publish, logger)
                    if os.path.exists(path + file.name):
                        os.remove(path + file.name)
                except Timeout:
                    logger.info(
                        f"Can't acquire lock on {lock.lock_file}"
                        f", locked? {lock.is_locked}, skipping..."
                    )
                finally:
                    lock.release()
                lock.release()


async def main():
    enrich_ipv6s_task = loop.create_task(enrich_ipv6s())
    await asyncio.wait([enrich_ipv6s_task])
    return (("enrich_ipv6s_task", enrich_ipv6s_task),)


# @TODO: maybe this doesn't need to be async, only the bulk_publish_messages
if __name__ == "__main__":
    logger.info("!!! starting Silent Push - IPV6 enrichment publisher !!!")
    subscriberState = SubscriberState()
    subscriberState._open = False
    loop = asyncio.get_event_loop()
    loop.set_debug(1)
    (t1,) = loop.run_until_complete(main())
    loop.close()
