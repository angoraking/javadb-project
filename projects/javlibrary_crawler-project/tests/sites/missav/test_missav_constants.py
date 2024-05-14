# -*- coding: utf-8 -*-

from javlibrary_crawler.sites.missav.constants import LangCodeEnum


class TestLangCodeEnum:
    def test(self):
        assert LangCodeEnum["zh"].value == 1


if __name__ == "__main__":
    from javlibrary_crawler.tests import run_cov_test

    run_cov_test(__file__, "javlibrary_crawler.sites.missav.constants", preview=False)
