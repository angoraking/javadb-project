# -*- coding: utf-8 -*-

import typing as T
import enum
import dataclasses

import bs4

from ...vendor.better_dataclasses import DataClass
from .constants import LangCodeEnum


@dataclasses.dataclass
class Girl(DataClass):
    name: str = dataclasses.field(default=None)
    url: str = dataclasses.field(default=None)


@dataclasses.dataclass
class Boy(DataClass):
    name: str = dataclasses.field(default=None)
    url: str = dataclasses.field(default=None)


@dataclasses.dataclass
class Tag(DataClass):
    name: str = dataclasses.field(default=None)
    url: str = dataclasses.field(default=None)


@dataclasses.dataclass
class Series(DataClass):
    name: str = dataclasses.field(default=None)
    url: str = dataclasses.field(default=None)


@dataclasses.dataclass
class Maker(DataClass):
    name: str = dataclasses.field(default=None)
    url: str = dataclasses.field(default=None)


@dataclasses.dataclass
class Label(DataClass):
    name: str = dataclasses.field(default=None)
    url: str = dataclasses.field(default=None)


@dataclasses.dataclass
class VideoDetail(DataClass):
    release_date: T.Optional[str] = dataclasses.field(default=None)
    code: T.Optional[str] = dataclasses.field(default=None)
    title: T.Optional[str] = dataclasses.field(default=None)
    girls: T.List[Girl] = Girl.list_of_nested_field(default_factory=list)
    boys: T.List[Girl] = Girl.list_of_nested_field(default_factory=list)
    tags: T.List[Girl] = Girl.list_of_nested_field(default_factory=list)
    series: T.Optional[Series] = Series.nested_field(default=None)
    maker: T.Optional[Maker] = Maker.nested_field(default=None)
    label: T.Optional[Label] = Label.nested_field(default=None)


class VideoDetailFieldEnum(str, enum.Enum):
    release_date = "release_date"
    code = "code"
    title = "title"
    girls = "girls"
    boys = "boys"
    tags = "tags"
    series = "series"
    maker = "maker"
    label = "label"


span_mapper = {
    LangCodeEnum.zh.value: {
        VideoDetailFieldEnum.release_date.value: "發行日期:",
        VideoDetailFieldEnum.code.value: "番號:",
        VideoDetailFieldEnum.girls.value: "女優:",
        VideoDetailFieldEnum.boys.value: "男優:",
        VideoDetailFieldEnum.tags.value: "類型:",
        VideoDetailFieldEnum.series.value: "系列:",
        VideoDetailFieldEnum.maker.value: "發行商:",
        VideoDetailFieldEnum.label.value: "標籤:",
    },
    LangCodeEnum.cn.value: {
        VideoDetailFieldEnum.release_date.value: "发行日期:",
        VideoDetailFieldEnum.code.value: "番号:",
        VideoDetailFieldEnum.title.value: "标题:",
        VideoDetailFieldEnum.girls.value: "女优:",
        VideoDetailFieldEnum.boys.value: "男优:",
        VideoDetailFieldEnum.tags.value: "类型:",
        VideoDetailFieldEnum.series.value: "系列:",
        VideoDetailFieldEnum.maker.value: "发行商:",
        VideoDetailFieldEnum.label.value: "标籤:",
    },
    LangCodeEnum.ja.value: {
        VideoDetailFieldEnum.release_date.value: "配信開始日:",
        VideoDetailFieldEnum.code.value: "品番:",
        VideoDetailFieldEnum.girls.value: "女優:",
        VideoDetailFieldEnum.boys.value: "男優:",
        VideoDetailFieldEnum.tags.value: "ジャンル:",
        VideoDetailFieldEnum.series.value: "シリーズ:",
        VideoDetailFieldEnum.maker.value: "メーカー:",
        VideoDetailFieldEnum.label.value: "レーベル:",
    },
}


def parse_video_detail_html(lang: int, html: str):
    data = dict()

    soup = bs4.BeautifulSoup(html, features="html.parser")
    div_video_details = soup.find(
        "div", attrs={"x-show": "currentTab === 'video_details'"}
    )
    if div_video_details is None:
        raise ValueError("Cannot find video details div.")
    div_video_info = div_video_details.find("div", class_="space-y-2")
    if div_video_info is None:
        raise ValueError("Cannot find video info div.")

    # 将 <div class_="text-secondary"> 的内容按照第一个 span, 也就是 key 进行映射
    key_to_div_text_secondary_mapper = {}
    for div_text_secondary in div_video_info.find_all("div", class_="text-secondary"):
        span = div_text_secondary.find("span")
        if span is None:
            pass
        else:
            key = span.text.strip()
            key_to_div_text_secondary_mapper[key] = div_text_secondary

    # --- title
    h1 = soup.find("h1")
    if h1 is None:
        raise ValueError("Cannot find title h1.")
    else:
        data[VideoDetailFieldEnum.title.value] = h1.text

    # --- release_date
    key = span_mapper[lang][VideoDetailFieldEnum.release_date.value]
    if key in key_to_div_text_secondary_mapper:
        div_text_secondary = key_to_div_text_secondary_mapper[key]
        span_value = div_text_secondary.find("span", class_="font-medium")
        if span_value is None:
            pass
        else:
            data[VideoDetailFieldEnum.release_date.value] = span_value.text

    # --- code
    key = span_mapper[lang][VideoDetailFieldEnum.code.value]
    if key in key_to_div_text_secondary_mapper:
        div_text_secondary = key_to_div_text_secondary_mapper[key]
        span_value = div_text_secondary.find("span", class_="font-medium")
        if span_value is None:
            pass
        else:
            data[VideoDetailFieldEnum.code.value] = span_value.text

    # --- girls
    key = span_mapper[lang][VideoDetailFieldEnum.girls.value]
    if key in key_to_div_text_secondary_mapper:
        div_text_secondary = key_to_div_text_secondary_mapper[key]
        girls = list()
        for a in div_text_secondary.find_all("a"):
            girl = Girl(name=a.text, url=a["href"])
            girls.append(girl)
        data[VideoDetailFieldEnum.girls.value] = girls

    # --- boys
    key = span_mapper[lang][VideoDetailFieldEnum.boys.value]
    if key in key_to_div_text_secondary_mapper:
        div_text_secondary = key_to_div_text_secondary_mapper[key]
        boys = list()
        for a in div_text_secondary.find_all("a"):
            boy = Boy(name=a.text, url=a["href"])
            boys.append(boy)
        data[VideoDetailFieldEnum.boys.value] = boys

    # --- tags
    key = span_mapper[lang][VideoDetailFieldEnum.tags.value]
    if key in key_to_div_text_secondary_mapper:
        div_text_secondary = key_to_div_text_secondary_mapper[key]
        tags = list()
        for a in div_text_secondary.find_all("a"):
            tag = Tag(name=a.text, url=a["href"])
            tags.append(tag)
        data[VideoDetailFieldEnum.tags.value] = tags

    # --- series
    key = span_mapper[lang][VideoDetailFieldEnum.series.value]
    if key in key_to_div_text_secondary_mapper:
        div_text_secondary = key_to_div_text_secondary_mapper[key]
        a = div_text_secondary.find("a")
        if a is not None:
            series = Series(name=a.text, url=a["href"])
            data[VideoDetailFieldEnum.series.value] = series

    # --- maker
    key = span_mapper[lang][VideoDetailFieldEnum.maker.value]
    if key in key_to_div_text_secondary_mapper:
        div_text_secondary = key_to_div_text_secondary_mapper[key]
        a = div_text_secondary.find("a")
        if a is not None:
            maker = Maker(name=a.text, url=a["href"])
            data[VideoDetailFieldEnum.maker.value] = maker

    # --- label
    key = span_mapper[lang][VideoDetailFieldEnum.label.value]
    if key in key_to_div_text_secondary_mapper:
        div_text_secondary = key_to_div_text_secondary_mapper[key]
        a = div_text_secondary.find("a")
        if a is not None:
            label = Maker(name=a.text, url=a["href"])
            data[VideoDetailFieldEnum.label.value] = label

    video_detail = VideoDetail(**data)
    return video_detail
