# -*- coding: utf-8 -*-

from datetime import datetime

import sqlalchemy as sa
import sqlalchemy.orm as orm

from ..constants import SiteEnum
from .constants import LangCodeEnum


class Base(orm.DeclarativeBase):
    pass


class ItemUrl(Base):
    __tablename__ = f"{SiteEnum.missav.value}_item_url"

    url: orm.Mapped[str] = orm.mapped_column(sa.String, primary_key=True)
    lang: orm.Mapped[int] = orm.mapped_column(sa.Integer)
    status: orm.Mapped[int] = orm.mapped_column(sa.Integer)
    create_at: orm.Mapped[datetime] = orm.mapped_column(sa.DateTime)
    update_at: orm.Mapped[datetime] = orm.mapped_column(sa.DateTime)
    lock: orm.Mapped[str] = orm.mapped_column(sa.String, nullable=True)
    lock_time: orm.Mapped[datetime] = orm.mapped_column(sa.DateTime, nullable=True)
    data: orm.Mapped[str] = orm.mapped_column(sa.JSON, nullable=True)
