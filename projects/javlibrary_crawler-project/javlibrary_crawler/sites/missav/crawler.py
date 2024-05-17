# -*- coding: utf-8 -*-

import requests


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
}


class HttpError(Exception):
    """
    当 HTTP status code 不对的时候抛出这个异常.
    """

    pass


class MalformedHtmlError(Exception):
    """
    当 HTTP status code 对, 但是 HTML 格式不对的时候抛出这个异常.
    """

    pass


def get_video_detail_html(url: str) -> str:
    """
    下载影片详细信息的 HTML. 例如 https://missav.com/cn/abf-106
    """
    res = requests.get(url, headers=headers)
    if res.status_code == 200:
        html = res.text
        if '<link rel="preload" as="image"' in html:
            return html
        else:
            raise MalformedHtmlError(f"Malformed HTML: {url}")
    else:
        raise HttpError(f"HTTP Error: {res.status_code}")
