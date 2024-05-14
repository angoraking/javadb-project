# -*- coding: utf-8 -*-

import sqlalchemy as sa
import sqlalchemy.orm as orm

from .paths import path_db


def create_engine() -> sa.engine.Engine:
    return sa.create_engine(f"sqlite:///{path_db}")
