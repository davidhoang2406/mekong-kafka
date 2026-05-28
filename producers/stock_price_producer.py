import logging
import time
from pathlib import Path

from dotenv import load_dotenv
from vnstock import Vnstock

from market_data_models.coerce import coerce_float, coerce_int
from market_data_models.message import build_envelope
from market_data_models.topics import STOCK_PRICE_REALTIME
from model.base_producer import BaseProducer
from producers.utils import (CIRCUIT_OPEN_SLEEP, CIRCUIT_THRESHOLD,
                              backoff_delay, load_json_config)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = STOCK_PRICE_REALTIME
CONFIG = Path(__file__).parent.parent / "config" / "stocks.json"


def _publish_snapshot(producer: BaseProducer, symbols: list, default_exchange: str) -> int:
    df = Vnstock(source="VCI", show_log=False).stock(symbol=symbols[0]).trading.price_board(symbols_list=symbols)
    if df is None or df.empty:
        log.warning("price_board returned empty result")
        return 0

    count = 0
    for _, row in df.iterrows():
        match_price = coerce_float(row[("match", "match_price")])
        if not match_price:
            log.debug("Skipping %s — no live price (market closed?)",
                      row.get(("listing", "symbol"), "?"))
            continue

        ref_price  = coerce_float(row[("listing", "ref_price")])
        change     = round(match_price - ref_price, 2) if ref_price else 0.0
        pct_change = round(change / ref_price * 100, 2) if ref_price else 0.0

        symbol   = str(row[("listing", "symbol")] or "UNKNOWN").upper()
        exchange = str(row[("listing", "exchange")] or default_exchange).upper()
        payload = {
            "price":      match_price,
            "change":     change,
            "pct_change": pct_change,
            "volume":     coerce_int(row[("match", "accumulated_volume")]),
            "bid":        coerce_float(row.get(("bid_ask", "bid_1_price"))),
            "ask":        coerce_float(row.get(("bid_ask", "ask_1_price"))),
        }
        producer.send(
            TOPIC,
            value=build_envelope("price.snapshot", symbol, exchange, payload),
            key=symbol,
        )
        count += 1

    producer.flush()
    return count


def run():
    config = load_json_config(CONFIG)
    exchange: str  = config["exchange"]
    symbols: list  = config["symbols"]
    interval: int  = config.get("poll_interval_seconds", 300)

    log.info("Starting price producer | exchange=%s | symbols=%s | interval=%ds", exchange, symbols, interval)
    consecutive_failures = 0
    with BaseProducer(topic=TOPIC) as producer:
        while True:
            try:
                n = _publish_snapshot(producer, symbols, exchange)
                log.info("Published %d price snapshots → %s", n, TOPIC)
                consecutive_failures = 0
                time.sleep(interval)
            except Exception as exc:
                consecutive_failures += 1
                if consecutive_failures >= CIRCUIT_THRESHOLD:
                    log.error("Circuit open after %d consecutive failures — pausing %ds",
                              consecutive_failures, CIRCUIT_OPEN_SLEEP)
                    time.sleep(CIRCUIT_OPEN_SLEEP)
                    consecutive_failures = 0
                else:
                    delay = backoff_delay(consecutive_failures)
                    log.warning("Fetch failed (attempt %d): %s — retrying in %.1fs",
                                consecutive_failures, exc, delay)
                    time.sleep(delay)
