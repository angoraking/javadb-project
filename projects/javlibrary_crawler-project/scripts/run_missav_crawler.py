# -*- coding: utf-8 -*-

from javlibrary_crawler.boto_ses import bsm
import javlibrary_crawler.sites.missav.api as missav

bsm.print_who_am_i()

missav.crawl_todo(
    lang_code=missav.LangCodeEnum.cn,
)
