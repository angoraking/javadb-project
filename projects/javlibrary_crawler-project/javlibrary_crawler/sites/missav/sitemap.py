# -*- coding: utf-8 -*-

"""
这个模块负责把 https://missav.com/sitemap.xml 以及里面列出的其他所有的 .xml 文件下载下来.
这些 xml 文件里面包含了所有待爬取的页面的 URL.
"""

import typing as T
import gzip
import dataclasses

# import xml.etree.ElementTree as ET
import lxml.etree

import bs4
import requests
from pathlib_mate import Path
from ...vendor.hashes import hashes, HashAlgoEnum

from .paths import dir_missav_sitemap
from .constants import LangCodeEnum

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
}


@dataclasses.dataclass
class SiteMapSnapshot:
    """
    由于 sitemap.xml 文件会更新, 所以我们会将这个 sitemap 的每一个快照分开保存.
    """

    md5: str = dataclasses.field()

    @property
    def dir_sitemap_snapshot(self) -> Path:
        return dir_missav_sitemap / self.md5

    @property
    def path_missav_sitemap_xml_gz(self) -> Path:
        return self.dir_sitemap_snapshot / "sitemap.xml.gz"

    def get_item_xml(self, ith: int) -> Path:
        return self.dir_sitemap_snapshot / f"sitemap_items_{ith}.xml.gz"

    def get_item_xml_list(self) -> T.List[Path]:
        """
        返回所有的 sitemap_items_*.xml.gz 文件, 按照文件名的数字部分从小到大排序.
        数字越大的文件里面的 URL 越新.
        """

        def filter_func(p: Path):
            return p.basename.startswith("sitemap_items_") and p.basename.endswith(
                ".xml.gz"
            )

        def sort_func(p: Path) -> int:
            """
            按照 sitemap_items_* 中的 * 代表的数字排序.
            """
            return int(p.basename.split("_")[-1].split(".")[0])

        # 数字小的文件 (旧的文件) 排在前面.
        return list(
            sorted(
                self.dir_sitemap_snapshot.select_file(filters=filter_func),
                key=sort_func,
                reverse=False,
            )
        )

    def get_actresses_xml(self, ith: int) -> Path:
        return self.dir_sitemap_snapshot / f"sitemap_actresses_{ith}.xml.gz"

    def get_actresses_xml_list(self) -> T.List[Path]:
        """
        返回所有的 sitemap_actresses_*.xml.gz 文件, 按照文件名的数字部分从小到大排序.
        数字越大的文件里面的 URL 越新.
        """

        def filter_func(p: Path):
            return p.basename.startswith("sitemap_actresses_") and p.basename.endswith(
                ".xml.gz"
            )

        def sort_func(p: Path) -> int:
            """
            按照 sitemap_actresses_* 中的 * 代表的数字排序.
            """
            return int(p.basename.split("_")[-1].split(".")[0])

        # 数字小的文件 (旧的文件) 排在前面.
        return list(
            sorted(
                self.dir_sitemap_snapshot.select_file(filters=filter_func),
                key=sort_func,
                reverse=False,
            )
        )

    @classmethod
    def new(
        cls,
        md5: T.Optional[str] = None,
    ):
        """
        创建一个新的 SiteMapSnapshot 对象. 创建的过程中会去读取最新的 sitemap.xml 的内容,
         并且用内容的 MD5 hash 作为唯一的 ID.

        :param md5: 如果 MD5 没给定, 说明这是一个全新的
        """
        if md5 is None:
            res = requests.get("https://missav.com/sitemap.xml", headers=headers)
            xml_content = res.text
            md5 = hashes.of_str(xml_content, algo=HashAlgoEnum.md5)
            snapshot = cls(md5=md5)
            snapshot.dir_sitemap_snapshot.mkdir_if_not_exists()
            if not snapshot.path_missav_sitemap_xml_gz.exists():
                snapshot.path_missav_sitemap_xml_gz.write_bytes(
                    gzip.compress(res.content)
                )
        else:
            snapshot = cls(md5=md5)
            if not snapshot.path_missav_sitemap_xml_gz.exists():
                raise FileNotFoundError(
                    f"If you are using a md5 to create new snapshot object, "
                    f"we assume the sitemap.xml has been downloeded, however we cannot find it!"
                )
        return snapshot

    def download(self):
        """
        将 sitemap.xml 里面列出的所有 .xml 文件下载下来并压缩保存为 .xml.gz 文件
        """
        root = ET.fromstring(
            gzip.decompress(self.path_missav_sitemap_xml_gz.read_bytes()).decode(
                "utf-8"
            )
        )
        namespaces = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for loc in root.findall(".//ns:loc", namespaces):
            url = loc.text
            filename = url.split("/")[-1] + ".gz"
            path_xml_gz = self.dir_sitemap_snapshot / filename
            if not path_xml_gz.exists():
                print(f"download {url} to {path_xml_gz} ...")
                res = requests.get(url, headers=headers)
                path_xml_gz.write_bytes(gzip.compress(res.content))

    def remove_uncompressed(self):
        """
        人类会需要解压缩 .xml.gz 文件来观察里面的内容, 这样会留下一些非常占空间的 .xml 文件,
        我们可以用这个函数来清除他们.
        """
        for p in self.dir_sitemap_snapshot.select_by_ext(".xml"):
            p.remove()


@dataclasses.dataclass
class ActressUrl:
    url: str
    lang: int


@dataclasses.dataclass
class ItemUrl:
    url: str
    lang: int

    @classmethod
    def filter_by_lang(
        cls,
        item_url_list: T.Iterable["ItemUrl"],
        lang_code: LangCodeEnum,
    ) -> T.List["ItemUrl"]:
        """
        对于每个语言的 Item URL, 一个 sitemap_items_*.xml.gz 文件里面 (去重后)
        会有至多 1000 个 URL.
        """
        expected_lang_code = lang_code.value
        return [
            item_url
            for item_url in item_url_list
            if item_url.lang == expected_lang_code
        ]


def _parse_actress_or_item_xml_v1(p: Path) -> T.List[T.Tuple[str, int]]:
    """
    从 sitemap_actresses_123.xml.gz 或者 sitemap_items_123.xml.gz 中提取出所有的
    URL 和对应的语言代码. 这是 beautifulsoup 的实现, 性能较差.
    """
    soup = bs4.BeautifulSoup(
        gzip.decompress(p.read_bytes()).decode("utf-8"), features="xml"
    )
    dct = dict()
    for t_url in soup.find_all("url"):
        for xhtml in t_url.find_all("xhtml:link"):
            url = xhtml["href"]
            lang = xhtml["hreflang"]
            dct[url] = (url, LangCodeEnum[lang].value)
    lst = list(dct.values())
    return lst


def _parse_actress_or_item_xml_v2(p: Path) -> T.List[T.Tuple[str, int]]:
    """
    从 sitemap_actresses_123.xml.gz 或者 sitemap_items_123.xml.gz 中提取出所有的
    URL 和对应的语言代码.
    """
    root = lxml.etree.fromstring(gzip.decompress(p.read_bytes()))
    namespaces = {"xhtml": "http://www.w3.org/1999/xhtml"}
    links = root.xpath("//xhtml:link", namespaces=namespaces)
    dct = dict()
    for link in links:
        lang = link.get("hreflang")
        url = link.get("href")
        dct[url] = (url, LangCodeEnum[lang].value)
    lst = list(dct.values())
    return lst


_parse_actress_or_item_xml = _parse_actress_or_item_xml_v2


def parse_actresses_xml(p: Path) -> T.List[ActressUrl]:
    """
    从 sitemap_actresses_123.xml.gz 中提取出所有的 Actress URL
    """
    return [
        ActressUrl(url=url, lang=lang) for url, lang in _parse_actress_or_item_xml(p)
    ]


def parse_item_xml(p: Path) -> T.List[ItemUrl]:
    """
    从 解析 sitemap_items_123.xml.gz 中提取出所有的 Item URL
    """
    return [ItemUrl(url=url, lang=lang) for url, lang in _parse_actress_or_item_xml(p)]
