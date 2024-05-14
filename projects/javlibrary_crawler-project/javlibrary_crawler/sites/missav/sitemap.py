# -*- coding: utf-8 -*-

"""
这个模块负责把 sitemap.xml 以及下面所有的 .xml 文件下载下来.
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
    md5: str = dataclasses.field()

    @property
    def dir_sitemap_snapshot(self) -> Path:
        return dir_missav_sitemap / self.md5

    @property
    def path_missav_sitemap_xml_gz(self) -> Path:
        return self.dir_sitemap_snapshot / "sitemap.xml.gz"

    @classmethod
    def new(cls, md5: T.Optional[str] = None):
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


@dataclasses.dataclass
class ItemUrl:
    lang: str
    url: str


def parse_item_xml(p: Path) -> T.List[ItemUrl]:
    item_url_list = list()
    soup = bs4.BeautifulSoup(gzip.decompress(p.read_bytes()).decode("utf-8"), features="xml")
    for t_url in soup.find_all("url"):
        for xhtml in t_url.find_all("xhtml:link"):
            item_url = ItemUrl(lang=xhtml["hreflang"], url=xhtml["href"])
            item_url_list.append(item_url)
    return item_url_list
