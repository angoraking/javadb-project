# -*- coding: utf-8 -*-

"""
DynamoDB 是用来追踪 HTML 的下载进度的. 由于爬虫是运行在 GitHub Action 中的, 所以
对应的数据库也必须在云端, 不能用本地的.
"""

import typing as T
import gzip
import base64
import hashlib

from tenacity import (
    retry,
    wait_exponential,
    retry_if_exception_type,
    stop_after_attempt,
)
from s3pathlib import S3Path, ContentTypeEnum
from boto_session_manager import BotoSesManager
import pynamodb_mate.api as pm

from javlibrary_crawler.vendor.better_enum import BetterStrEnum
from ...config.load import config
from ...boto_ses import bsm
from ...utils import get_utc_now
from ...logger import logger
from ..constants import SiteEnum

from .constants import LangCodeEnum
from .downloader import HttpError, MalformedHtmlError, get_video_detail_html


st = pm.patterns.status_tracker
large_attribute = pm.patterns.large_attribute


class StatusAndUpdateTimeIndex(st.StatusAndUpdateTimeIndex):
    class Meta:
        index_name = "status_and_update_time-index"
        projection = pm.IncludeProjection(
            [
                "create_time",
            ]
        )


def s3_key_getter(
    pk: T.Union[str, int],
    sk: T.Optional[T.Union[str, int]],
    attr: str,
    value: bytes,
    prefix: str,
) -> str:
    parts = list()
    if prefix:  # pragma: no cover
        if prefix.startswith("/"):
            prefix = prefix[1:]
        elif prefix.endswith("/"):
            prefix = prefix[:-1]
        parts.append(prefix)
    pk_b64encode = base64.b64encode(pk.encode("utf-8")).decode("utf-8")
    parts.append(f"pk={pk_b64encode}")
    if sk is not None:  # pragma: no cover
        parts.append(f"sk={sk}")
    parts.append(f"attr={attr}")
    md5 = hashlib.md5(value).hexdigest()
    parts.append(f"md5={md5}")
    return "/".join(parts)


class BaseTask(
    st.BaseTask,
    large_attribute.LargeAttributeMixin,
):
    """
    所有的下载任务的基类. 不同的语言的下载任务放在不同的 DynamoDB table 中. 比如我们有:

    - :class:`TaskJaJp`
    - :class:`TaskZhCN`
    - :class:`TaskZhTW`
    """

    html: pm.OPTIONAL_STR = pm.UnicodeAttribute(null=True)

    status_and_update_time_index = StatusAndUpdateTimeIndex()

    @classmethod
    def set_connection(cls, bsm: BotoSesManager):
        with bsm.awscli():
            cls._connection = None
            cls.Meta.region = bsm.aws_region
            conn = pm.Connection()
            cls.create_table(wait=True)

    @property
    def url(self):
        return self.task_id

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(10),
        retry=retry_if_exception_type(HttpError),
        reraise=True,
    )
    def do_download_task(self):
        html = get_video_detail_html(self.url)
        content = gzip.compress(html.encode("utf-8"))
        s3dir_missav_downloads = config.env.s3dir_missav_downloads
        new_model = self.update_large_attribute_item(
            s3_client=bsm.s3_client,
            pk=self.key,
            sk=None,
            kvs={self.__class__.html.attr_name: content},
            bucket=s3dir_missav_downloads.bucket,
            prefix=s3dir_missav_downloads.key,
            update_at=get_utc_now(),
            s3_put_object_kwargs={
                "ContentType": ContentTypeEnum.app_gzip,
            },
            s3_key_getter=s3_key_getter,
            clean_up_when_failed=True,
        )
        s3path = S3Path(new_model.html)
        logger.info(f"Html is stored at: {s3path.console_url}")


class TaskJaJp(BaseTask):
    """
    todo: docstring
    """

    class Meta:
        table_name = config.env.get_tracker_dynamodb_table_name(
            site=SiteEnum.missav.value,
            language="jaJP",
        )
        region = "us-east-1"
        billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE

    status_and_update_time_index = StatusAndUpdateTimeIndex()


class TaskZhCN(BaseTask):
    """
    todo: docstring
    """

    class Meta:
        table_name = config.env.get_tracker_dynamodb_table_name(
            site=SiteEnum.missav.value,
            language="zhCN",
        )
        region = "us-east-1"
        billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE

    status_and_update_time_index = StatusAndUpdateTimeIndex()


class TaskZhTW(BaseTask):
    """
    todo: docstring
    """

    class Meta:
        table_name = config.env.get_tracker_dynamodb_table_name(
            site=SiteEnum.missav.value,
            language="zhTW",
        )
        region = "us-east-1"
        billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE

    status_and_update_time_index = StatusAndUpdateTimeIndex()


class UseCaseEnum(BetterStrEnum):
    download = "download"


class Step1DownloadStatusEnum(st.BaseStatusEnum):
    pending = 10
    in_progress = 12
    failed = 14
    succeeded = 16
    ignored = 18


DownloadStatusEnum = Step1DownloadStatusEnum


def make_config() -> st.TrackerConfig:
    return st.TrackerConfig.make(
        use_case_id=UseCaseEnum.download.value,
        pending_status=Step1DownloadStatusEnum.pending.value,
        in_progress_status=Step1DownloadStatusEnum.in_progress.value,
        failed_status=Step1DownloadStatusEnum.failed.value,
        succeeded_status=Step1DownloadStatusEnum.succeeded.value,
        ignored_status=Step1DownloadStatusEnum.ignored.value,
        n_pending_shard=10,
        n_in_progress_shard=5,
        n_failed_shard=5,
        n_succeeded_shard=10,
        n_ignored_shard=5,
        status_zero_pad=3,
        status_shard_zero_pad=3,
        max_retry=3,
        lock_expire_seconds=60,
    )


class DownloadJaJP(TaskJaJp):
    status_and_update_time_index = st.StatusAndUpdateTimeIndex()

    config = make_config()


class DownloadZhCN(TaskZhCN):
    status_and_update_time_index = st.StatusAndUpdateTimeIndex()

    config = make_config()


class DownloadZhTW(TaskZhTW):
    status_and_update_time_index = st.StatusAndUpdateTimeIndex()

    config = make_config()


lang_to_task_mapping: T.Dict[int, T.Type[st.BaseTask]] = {
    LangCodeEnum.ja.value: TaskJaJp,
    LangCodeEnum.zh.value: TaskZhTW,
    LangCodeEnum.cn.value: TaskZhCN,
}

lang_to_step1_mapping: T.Dict[int, T.Type[st.BaseTask]] = {
    LangCodeEnum.ja.value: DownloadJaJP,
    LangCodeEnum.zh.value: DownloadZhTW,
    LangCodeEnum.cn.value: DownloadZhCN,
}
