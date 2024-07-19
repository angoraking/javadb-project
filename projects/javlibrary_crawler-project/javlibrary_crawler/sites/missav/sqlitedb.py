# -*- coding: utf-8 -*-

"""
Sqlite 数据库是用来追踪 Parse HTML 的任务状态的.
"""

import typing as T
import gzip
import enum
import base64
from functools import cached_property

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam

from s3pathlib import S3Path
from boto_session_manager import BotoSesManager

from javlibrary_crawler.vendor.better_enum import BetterIntEnum

from ..constants import SiteEnum
from .constants import LangCodeEnum
from .parser import VideoDetail, parse_video_detail_html


Base = orm.declarative_base()


class Step2ParseHtmlStatusEnum(BetterIntEnum):
    pending = 20
    in_progress = 22
    failed = 24
    succeeded = 26
    ignored = 28


class Job(
    Base,
    sam.ExtendedBase,
    sam.patterns.status_tracker.JobMixin,
):
    __tablename__ = f"{SiteEnum.missav.value}_item_url"

    # fmt: off
    url: orm.Mapped[str] = sa.Column(sa.String)
    html: orm.Mapped[str] = sa.Column(sa.String, nullable=True)
    video_detail_data: orm.Mapped[str] = sa.Column(sam.types.CompressedJSONType, nullable=True)
    # fmt: on

    @cached_property
    def video_detail(self) -> T.Optional["VideoDetail"]:
        if self.video_detail_data:
            return VideoDetail.from_dict(self.video_detail_data)
        else:
            return None

    @classmethod
    def create_and_not_save(
        cls,
        id: str,
        url: str,
        html: str,
    ):
        return cls.create(
            id=id,
            status=Step2ParseHtmlStatusEnum.pending.value,
            url=url,
            html=html,
        )

    def read_html(
        self,
        bsm: BotoSesManager,
    ) -> str:
        s3path = S3Path(self.html)
        return gzip.decompress(s3path.read_bytes(bsm=bsm)).decode("utf-8")

    @classmethod
    def start_parse_html_job(
        cls,
        engine: sa.Engine,
        id: str,
        skip_error: bool = False,
        debug: bool = False,
    ):
        return cls.start(
            engine=engine,
            id=id,
            in_process_status=Step2ParseHtmlStatusEnum.in_progress.value,
            failed_status=Step2ParseHtmlStatusEnum.failed.value,
            success_status=Step2ParseHtmlStatusEnum.succeeded.value,
            ignore_status=Step2ParseHtmlStatusEnum.ignored.value,
            expire=60,
            max_retry=3,
            skip_error=skip_error,
            debug=debug,
        )

    @classmethod
    def do_parse_html_job(
        cls,
        engine: sa.Engine,
        id: str,
        bsm: BotoSesManager,
        lang: LangCodeEnum,
        skip_error: bool = False,
        debug: bool = False,
    ):
        job: "Job"
        with cls.start_parse_html_job(
            engine,
            id=id,
            skip_error=skip_error,
            debug=debug,
        ) as (job, updates):
            html = job.read_html(bsm=bsm)
            video_detail = parse_video_detail_html(lang=lang, html=html)
            if video_detail is None:
                raise NotImplementedError
            video_detail_data = video_detail.to_dict()
            updates.set("video_detail_data", video_detail_data)
