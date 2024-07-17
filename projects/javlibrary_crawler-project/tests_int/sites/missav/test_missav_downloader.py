# -*- coding: utf-8 -*-

from javlibrary_crawler.sites.missav.downloader import get_video_detail_html


def test_get_video_detail_html():
    url = "https://missav.com/dm39/cn/abf-106"
    html = get_video_detail_html(url)
    assert "和素人君单独相处了一整天" in html


if __name__ == "__main__":
    from javlibrary_crawler.tests import run_cov_test

    run_cov_test(
        __file__,
        "javlibrary_crawler.sites.missav.downloader",
        preview=False,
    )
