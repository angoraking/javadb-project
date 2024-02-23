# -*- coding: utf-8 -*-

from javlibrary_crawler.env import EnvNameEnum, detect_current_env


def test():
    _ = detect_current_env()


if __name__ == "__main__":
    from javlibrary_crawler.tests import run_cov_test

    run_cov_test(__file__, "javlibrary_crawler.env", preview=False)
