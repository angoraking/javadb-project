# -*- coding: utf-8 -*-

from javlibrary_crawler.runtime import runtime


def test():
    _ = runtime


if __name__ == "__main__":
    from javlibrary_crawler.tests import run_cov_test

    run_cov_test(__file__, "javlibrary_crawler.runtime", preview=False)
