# -*- coding: utf-8 -*-

import typing as T
import gzip

from s3pathlib import ContentTypeEnum
import pynamodb_mate.api as pm
from javlibrary_crawler.vendor.better_enum import BetterStrEnum

from ...config.load import config
from ...boto_ses import bsm
from ...utils import get_utc_now

from .constants import LangCodeEnum
from .downloader import HttpError, MalformedHtmlError, get_video_detail_html


st = pm.patterns.status_tracker
large_attribute = pm.patterns.large_attribute


class BaseTask(
    st.BaseTask,
    large_attribute.LargeAttributeMixin,
):
    url: pm.REQUIRED_STR = pm.UnicodeAttribute()
    html: pm.OPTIONAL_STR = pm.UnicodeAttribute(null=True)

    status_and_update_time_index = st.StatusAndUpdateTimeIndex()

    def do_download_task(self):
        html = get_video_detail_html(self.url)
        content = gzip.compress(html.encode("utf-8"))
        s3dir_missav_downloads = config.env.s3dir_missav_downloads
        self.update_large_attribute_item(
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
            clean_up_when_failed=True,
        )


class TaskJaJp(BaseTask):
    class Meta:
        table_name = "javadb-missav-jaJP-status-tracker"
        region = "us-east-1"
        billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE

    status_and_update_time_index = st.StatusAndUpdateTimeIndex()


class TaskZhCN(BaseTask):
    class Meta:
        table_name = "javadb-missav-zhCN-status-tracker"
        region = "us-east-1"
        billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE

    status_and_update_time_index = st.StatusAndUpdateTimeIndex()


class TaskZhTW(BaseTask):
    class Meta:
        table_name = "javadb-missav-zhTW-status-tracker"
        region = "us-east-1"
        billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE


    status_and_update_time_index = st.StatusAndUpdateTimeIndex()


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
