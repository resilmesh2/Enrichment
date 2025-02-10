import asyncio
import http
import logging
import os
from logging.handlers import RotatingFileHandler
from math import floor

from filelock import FileLock, Timeout
from settings import API_URL, EVENTS_DIR, PUBLISHER_LOG_FILE, log_format
from utils.api import bulk_publish_messages, do_request, prepare_ips_to_publish, get_cached_events, set_cached_events
from utils.custom_ndjson_reader import CustomNDJSONReader
from utils.subscriberState import SubscriberState
from utils.utils import skip_current_file

"""
Enriches IPV4 events from NATS using Silent Push API
"""


logger = logging.getLogger("ipv4_publisher")
log_file_handler = RotatingFileHandler(
    PUBLISHER_LOG_FILE, maxBytes=100_000_000, backupCount=5
)
log_file_handler.setFormatter(log_format)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(log_file_handler)


def bulk_enrich_ipv4s(ipv4s: set) -> list:
    if not ipv4s:
        return []
    # _URI = API_URL + "explore/bulk/summary/ipv4?explain=1&scan_data=1"
    _URI = API_URL + "explore/bulk/summary/ipv4?explain=1"
    logger.info(f"Enriching {len(ipv4s)} IPv4s")
    ipv4s = list(set(ipv4s))
    enriched_data, status_code = do_request({"ips": ipv4s}, _URI, logger)
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


async def enrich_ipv4s():
    while True:
        path = f"{EVENTS_DIR}outbound/ipv4s/"
        with os.scandir(path) as files:
            # logger.info(f"files {files}")
            for file in files:
                # logger.info(f"file {file.name}")
                lock = FileLock(path + file.name + ".lock", thread_local=False)
                if skip_current_file(file, lock):
                    continue
                ipv4s = list()
                messages_to_publish = list()
                try:
                    with lock.acquire(timeout=5):
                        with open(path + file.name) as f:
                            reader = CustomNDJSONReader(f)
                            for line in reader:
                                if not line:
                                    continue
                                ipv4s.append((line.get("source", {}) or {}).get("ip"))
                                messages_to_publish.append(line)
                            if not ipv4s:
                                continue
                            enriched_cache, new_ipv4s = get_cached_events(set(ipv4s), logger)
                            enriched_ipv4s = bulk_enrich_ipv4s(set(new_ipv4s))
                            if enriched_cache:
                                enriched_ipv4s.extend(enriched_cache)
                            messages_to_publish = prepare_ips_to_publish(
                                enriched_ipv4s, messages_to_publish, logger
                            )
                            set_cached_events(enriched_ipv4s, logger)
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
    enrich_ipv4s_task = loop.create_task(enrich_ipv4s())
    await asyncio.wait([enrich_ipv4s_task])
    return (("enrich_ipv4s_task", enrich_ipv4s_task),)


# @TODO: maybe this doesn't need to be async, only the bulk_publish_messages
if __name__ == "__main__":
    logger.info("!!! starting Silent Push - IPV4 enrichment publisher !!!")
    subscriberState = SubscriberState()
    subscriberState._open = False
    loop = asyncio.get_event_loop()
    loop.set_debug(1)
    (t1,) = loop.run_until_complete(main())
    loop.close()
