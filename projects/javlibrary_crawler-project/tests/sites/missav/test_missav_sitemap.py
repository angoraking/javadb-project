# -*- coding: utf-8 -*-

from pathlib_mate import Path
from javlibrary_crawler.sites.missav.sitemap import parse_actresses_xml, parse_item_xml


def test_parse_actresses_xml():
    p = Path.dir_here(__file__) / "sitemap_actresses_1.xml.gz"
    actress_url_list = parse_actresses_xml(p)
    assert len(actress_url_list) == 3 * 13


def test_parse_item_xml():
    p = Path.dir_here(__file__) / "sitemap_items_1.xml.gz"
    item_url_list = parse_item_xml(p)
    assert len(item_url_list) == 3 * 13


if __name__ == "__main__":
    from javlibrary_crawler.tests import run_cov_test

    run_cov_test(__file__, "javlibrary_crawler.sites.missav.sitemap", preview=False)
