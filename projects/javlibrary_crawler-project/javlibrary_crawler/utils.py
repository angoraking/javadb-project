# -*- coding: utf-8 -*-

from datetime import datetime, timezone


def get_utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)


def to_s3_key_friendly_url(url) -> str:
    return url.replace(":", "_").replace("/", "_")
