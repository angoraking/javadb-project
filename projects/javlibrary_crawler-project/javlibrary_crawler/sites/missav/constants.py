# -*- coding: utf-8 -*-

from ...vendor.better_enum import BetterIntEnum


class LangCodeEnum(BetterIntEnum):
    """
    **Usage example**

    You can use ``LangCodeEnum["zh"].value`` to get the language code value.
    """

    zh = 1  # 繁体中文
    cn = 2  # 简体中文
    en = 3
    ja = 4  # 日文
    ko = 5
    ms = 6
    th = 7
    de = 8
    fr = 9
    vi = 10
    id = 11
    fil = 12
    pt = 13


# number of DynamoDB GSI shards for status field when the status is pending
N_PENDING_SHARD = 10

# This value has to align with the cron expression in
# https://github.com/angoraking/javadb-project/blob/javlibrary_crawler/tst/crawler/.github/workflows/javlibrary_crawler_site_missav_cron_job.yml
# For example, if the cron expression is '*/15 * * * *' (run every 15 minutes),
# then this value should be 900 (15 * 60 = 900).
GITHUB_ACTION_RUN_INTERVAL = 900

if GITHUB_ACTION_RUN_INTERVAL < 300:  # pragma: no cover
    raise ValueError(
        "GitHub Action run interval has to be at least 5 minutes (300 seconds)."
    )

# How long it takes for processing one downloads,
# including http request, and DynamoDB / S3 operations
# just use the happy path time, not including retry time
TASK_PROCESSING_TIME = 2
