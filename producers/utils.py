import json
from pathlib import Path


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
