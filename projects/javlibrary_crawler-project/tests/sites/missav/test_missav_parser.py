# -*- coding: utf-8 -*-

import gzip
from pathlib_mate import Path
from javlibrary_crawler.sites.missav.constants import LangCodeEnum
from javlibrary_crawler.sites.missav.parser import parse_video_detail_html

dir_here = Path.dir_here(__file__)


def read_html(filename: str) -> str:
    path = dir_here.joinpath(filename)
    return gzip.decompress(path.read_bytes()).decode("utf-8")


def test_parse_video_detail_html():
    html = read_html("abf-106-cn.html.gz")
    video_detail = parse_video_detail_html(lang=LangCodeEnum.cn.value, html=html)
    assert video_detail.image_url == "https://fivetiu.com/abf-106/cover-n.jpg"
    assert (
        video_detail.title
        == "ABF-106 和素人君单独相处了一整天。 Atsushi Nonoura [+45 分钟，附赠仅在 MGS 提供的片段] - 野野浦暖"
    )
    assert video_detail.release_date == "2024-05-15"
    assert video_detail.code == "ABF-106"
    assert video_detail.girls[0].name == "野野浦暖 (野々浦暖)"
    assert video_detail.boys[0].name == "蓝井优太"
    assert video_detail.tags[0].name == "苗条"
    assert video_detail.series.name == "素人くんと丸1日2人きり。"
    assert video_detail.maker.name == "Prestige"
    assert video_detail.label.name == "ABSOLUTELY FANTASIA"

    html = read_html("fc2-ppv-1579328-cn.html.gz")
    video_detail = parse_video_detail_html(lang=LangCodeEnum.cn.value, html=html)
    assert video_detail.image_url == "https://fivetiu.com/fc2-ppv-1579328/cover-n.jpg"
    assert (
        video_detail.title
        == "FC2-PPV-1579328 【今日截止】限时3天★终极美少女出炉！私自中出，全脸暴露，现实中不可能有这么调皮、开朗善良的女孩！ ！"
    )
    assert video_detail.release_date == "2024-05-03"
    assert video_detail.code == "FC2-PPV-1579328"
    assert video_detail.girls == []
    assert video_detail.boys == []
    assert video_detail.tags == []
    assert video_detail.series == None
    assert video_detail.maker == None
    assert video_detail.label == None


if __name__ == "__main__":
    from javlibrary_crawler.tests import run_cov_test

    run_cov_test(__file__, "javlibrary_crawler.sites.missav.parser", preview=False)
