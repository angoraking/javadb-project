# -*- coding: utf-8 -*-

"""
这个脚本是用于在 GitHub Action 中运行爬虫程序的脚本. 我们只在 prd 环境中真正下载大量数据.
在 sbx 和 tst 环境中我们只下载少量数据.
"""

import javlibrary_crawler.sites.missav.api as missav

# missav.insert_todo_list(
#     snapshot_id="4328d4511415a77eea41c3b091eb0e2a",
#     lang_code=missav.LangCodeEnum.cn,
# )
# missav.crawl_todo(
#     lang_code=missav.LangCodeEnum.cn,
# )

