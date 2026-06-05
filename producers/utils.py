import json
import random
import socket
from pathlib import Path

BACKOFF_BASE       = 10    # seconds
BACKOFF_MAX        = 300   # 5-minute cap
CIRCUIT_THRESHOLD  = 5     # consecutive failures before circuit opens
CIRCUIT_OPEN_SLEEP = 600   # 10-minute pause when circuit is open

_DNS_MARKERS = ("name or service not known", "nameresolutionerror", "failed to resolve")


def is_transient_network_error(exc: BaseException) -> bool:
    """True for DNS resolution failures — transient during cluster restarts.

    These should not count toward the circuit breaker threshold because they
    self-heal once the pod's network is restored, and opening the circuit
    causes up to 10 minutes of unnecessary data loss.
    """
    if isinstance(exc, socket.gaierror):
        return True
    msg = str(exc).lower()
    return any(marker in msg for marker in _DNS_MARKERS)


def asset_class(source: str) -> str:
    """Map an envelope source string to its asset-class token (stock / crypto)."""
    if source.startswith("vnstock"):
        return "stock"
    if source.startswith("ccxt"):
        return "crypto"
    return "unknown"


def load_json_config(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def backoff_delay(consecutive: int) -> float:
    """Exponential backoff with 10% jitter, capped at BACKOFF_MAX seconds."""
    delay = min(BACKOFF_BASE * (2 ** (consecutive - 1)), BACKOFF_MAX)
    return delay + random.uniform(0, delay * 0.1)
