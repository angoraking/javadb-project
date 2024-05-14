# -*- coding: utf-8 -*-

from ...paths import dir_home_data

dir_missav = dir_home_data / "missav"
dir_missav.mkdir_if_not_exists()
dir_missav_sitemap = dir_missav / "sitemap"
dir_missav_sitemap.mkdir_if_not_exists()
