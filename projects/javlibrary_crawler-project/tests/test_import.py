# -*- coding: utf-8 -*-

import os
import pytest
import javlibrary_crawler


def test_import():
    _ = javlibrary_crawler


if __name__ == "__main__":
    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
