# -*- coding: utf-8 -*-

import enum

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam


from ..constants import SiteEnum


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


class ItemJob(_ItemJob):
    lang: orm.Mapped[int] = orm.mapped_column(sa.Integer)
