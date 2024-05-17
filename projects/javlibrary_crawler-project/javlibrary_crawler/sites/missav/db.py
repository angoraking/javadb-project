# -*- coding: utf-8 -*-

import gzip
import enum
import base64

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam

from s3pathlib import S3Path
from boto_session_manager import BotoSesManager

from ...vendor.hashes import hashes, HashAlgoEnum
from ..constants import SiteEnum
from .crawler import HttpError, MalformedHtmlError, get_video_detail_html


class StatusEnum(enum.IntEnum):
    pending = 10
    in_progress = 20
    failed = 30
    succeeded = 40
    ignored = 50


Base = orm.declarative_base()


class _ItemJob(Base, sam.ExtendedBase, sam.patterns.status_tracker.JobMixin):
    __tablename__ = f"{SiteEnum.missav.value}_item_url"

    @property
    def url(self) -> str:
        return self.id

    @property
    def encoded_url(self) -> str:
        return base64.b64encode(self.url.encode("utf-8")).decode("utf-8")

    def get_html_s3path(self, s3dir_missav: S3Path) -> S3Path:
        return s3dir_missav.joinpath(f"html", f"{self.encoded_url}.html.gz")

    def write_html(
        self,
        bsm: BotoSesManager,
        s3dir_missav: S3Path,
        html: str,
    ) -> S3Path:
        s3path = self.get_html_s3path(s3dir_missav)
        html_bytes = html.encode("utf-8")
        s3path.write_bytes(
            data=gzip.compress(html_bytes),
            metadata={
                "url": self.url,
                "md5": hashes.of_bytes(html_bytes, algo=HashAlgoEnum.md5),
            },
            bsm=bsm,
        )
        return s3path

    def read_html(
        self,
        bsm: BotoSesManager,
        s3dir_missav: S3Path,
    ) -> str:
        s3path = self.get_html_s3path(s3dir_missav)
        return gzip.decompress(s3path.read_bytes(bsm=bsm)).decode("utf-8")

    @classmethod
    def start_job(
        cls,
        engine: sa.Engine,
        id: str,
        skip_error: bool = False,
        debug: bool = False,
    ):
        return cls.start(
            engine=engine,
            id=id,
            in_process_status=StatusEnum.in_progress.value,
            failed_status=StatusEnum.failed.value,
            success_status=StatusEnum.succeeded.value,
            ignore_status=StatusEnum.ignored.value,
            expire=60,
            max_retry=3,
            skip_error=skip_error,
            debug=debug,
        )

    @classmethod
    def do_job(
        cls,
        engine: sa.Engine,
        id: str,
        bsm: BotoSesManager,
        s3dir_missav: S3Path,
        skip_error: bool = False,
        debug: bool = False,
    ):
        job: "_ItemJob"
        with cls.start_job(
            engine,
            id=id,
            skip_error=skip_error,
            debug=debug,
        ) as (job, updates):
            try:
                html = get_video_detail_html(job.url)
            except HttpError:
                pass
            except MalformedHtmlError:
                pass
            except Exception as e:
                raise e
            s3path = job.write_html(
                bsm=bsm,
                s3dir_missav=s3dir_missav,
                html=html,
            )
            data = job.data
            if data is None:
                data = dict()
            data["html"] = s3path.uri
            updates.set(key="data", value=data)


class ItemJob(_ItemJob):
    """
    :param lang: 这个页面的语种 (ja, zh, cn) 的数字代码. 详情请参考 :class:`LangCodeEnum`.
    """

    lang: orm.Mapped[int] = orm.mapped_column(sa.Integer)
