"""
execution/order_ids.py

Client order ids: '{strategy_prefix}.{kind}.{base62 uuid}'.
Binance allows ^[\\.A-Z\\:/a-z0-9_-]{1,36}$, so the id is at most 10 + 1 + 1 + 1 + 22 = 35 chars.
Attribution never relies on parsing the prefix (the OMS keeps the full
mapping); the prefix is for humans reading exchange order history.
"""

import re
import string
import uuid

from messages.model import IntentKind

BASE62_ALPHABET = string.digits + string.ascii_letters
_ALLOWED = re.compile(r"[^A-Za-z0-9_\-]")


def _base62(num: int) -> str:
    out = []
    while num:
        num, rem = divmod(num, 62)
        out.append(BASE62_ALPHABET[rem])
    return "".join(reversed(out)) or "0"


def generate_client_order_id(strategy_id: str, kind: IntentKind) -> str:
    """e.g. 'MOCK_STRAT.O.4Sn2OBJx3BcaEaBattmMo9' (prefix = first 10 allowed chars of the strategy id)."""
    prefix = _ALLOWED.sub("", strategy_id)[:10] or "S"
    return f"{prefix}.{kind.code}.{_base62(uuid.uuid4().int)}"
