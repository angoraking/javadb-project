# -*- coding: utf-8 -*-

"""
这个模块负责把 https://missav.com/sitemap.xml 以及里面列出的其他所有的 .xml 文件下载下来.
"""

import typing as T
import gzip
import dataclasses
import xml.etree.ElementTree as ET

import bs4
import requests
from pathlib_mate import Path
from ...vendor.hashes import hashes, HashAlgoEnum

from .paths import dir_missav_sitemap

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
}


@dataclasses.dataclass
class SiteMapSnapshot:
    """
    由于 sitemap.xml 文件会更新, 所以我们会将这个 sitemap 的一个快照分开保存.
    """

    md5: str = dataclasses.field()

    @property
    def dir_sitemap_snapshot(self) -> Path:
        return dir_missav_sitemap / self.md5

    @property
    def path_missav_sitemap_xml_gz(self) -> Path:
        return self.dir_sitemap_snapshot / "sitemap.xml.gz"

    @classmethod
    def new(cls, md5: T.Optional[str] = None):
        """
        创建一个新的 SiteMapSnapshot 对象. 创建的过程中会去读取最新的 sitemap.xml 的内容,
         并且用内容的 MD5 hash 作为唯一的 ID.
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
    lang: str
    url: str


@dataclasses.dataclass
class ItemUrl:
    lang: str
    url: str


def _parse_actress_or_item_xml(p: Path) -> T.List[T.Tuple[str, str]]:
    lst = list()
    soup = bs4.BeautifulSoup(
        gzip.decompress(p.read_bytes()).decode("utf-8"), features="xml"
    )
    for t_url in soup.find_all("url"):
        for xhtml in t_url.find_all("xhtml:link"):
            lang = xhtml["hreflang"]
            url = xhtml["href"]
            lst.append((lang, url))
    return lst


def parse_actresses_xml(p: Path) -> T.List[ActressUrl]:
    return [
        ActressUrl(lang=lang, url=url) for lang, url in _parse_actress_or_item_xml(p)
    ]


def parse_item_xml(p: Path) -> T.List[ItemUrl]:
    return [ItemUrl(lang=lang, url=url) for lang, url in _parse_actress_or_item_xml(p)]
