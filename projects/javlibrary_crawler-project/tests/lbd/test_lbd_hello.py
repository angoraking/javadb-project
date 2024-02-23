# -*- coding: utf-8 -*-

from javlibrary_crawler.logger import logger
from javlibrary_crawler.lbd.hello import low_level_api


def _test_low_level_api():
    # invoke api
    response = low_level_api(name="alice")
    # validate response
    assert response["message"] == "hello alice"


def test_low_level_api():
    with logger.disabled(
        disable=True,
        # disable=False,
    ):
        _test_low_level_api()


if __name__ == "__main__":
    from javlibrary_crawler.tests import run_cov_test

    run_cov_test(__file__, "javlibrary_crawler.lbd.hello", preview=False)
