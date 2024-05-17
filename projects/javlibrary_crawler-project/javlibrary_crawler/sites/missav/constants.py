# -*- coding: utf-8 -*-

from ...vendor.better_enum import BetterIntEnum


class LangCodeEnum(BetterIntEnum):
    """
    **Usage example**

    You can use ``LangCodeEnum["zh"].value`` to get the language code value.
    """
    zh = 1 # 繁体中文
    cn = 2 # 简体中文
    en = 3
    ja = 4 # 日文
    ko = 5
    ms = 6
    th = 7
    de = 8
    fr = 9
    vi = 10
    id = 11
    fil = 12
    pt = 13
