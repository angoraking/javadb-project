# -*- coding: utf-8 -*-

from collections import Counter
import sqlalchemy as sa
import sqlalchemy.orm as orm

from javlibrary_crawler.config.load import config, Config
from javlibrary_crawler.boto_ses import bsm
from javlibrary_crawler.db import create_sqlite_engine, sqlite_round_trip
import javlibrary_crawler.sites.missav.api as missav

config: Config

# ------------------------------------------------------------------------------
# 如果是要下载最新的 sitemap 数据, 就运行下面的代码.
# 它会创建一个 ${HOME}/.projects/javlibrary_crawler/data/missav/sitemap/${md5} 目录,
# 并下载 sitemap 数据到这个目录.
# ------------------------------------------------------------------------------
# sitemap_snapshot = missav.SiteMapSnapshot.new()
# print(f"{sitemap_snapshot.md5 = }")
# sitemap_snapshot.download()


# ------------------------------------------------------------------------------
# 如果是要对已经下载的 sitemap 数据进行解析, 处理, 就运行下面的代码.
# 记得你要手动记录前一步中的 sitemap.xml 的 MD5 哈希值.
# ------------------------------------------------------------------------------
# md5 = "4328d4511415a77eea41c3b091eb0e2a"
# sitemap_snapshot = missav.SiteMapSnapshot.new(md5)
# # 如果你手动解压了 xml.gz 文件, 会产生一些未压缩的临时文件, 这个方法可以自动清除掉这些临时文件以节约磁盘空间
# sitemap_snapshot.remove_uncompressed()
# p = sitemap_snapshot.get_item_xml(58)
# item_url_list = missav.parse_item_xml(p)
# filtered_item_url_list = missav.ItemUrl.filter_by_lang(
#     item_url_list, missav.LangCodeEnum.zh
# )
# for item in filtered_item_url_list[:10]:
#     print(item)


# ------------------------------------------------------------------------------
# 手动创建 DynamoDB import 所需的 data 文件.
# 扫描所有 sitemap_items_*.xml.gz 文件, 从中提取 URL, 写入本地临时文件, 上传到 S3.
# 这里的 snapshot_id 是上面 ``print(f"{sitemap_snapshot.md5 = }")`` 这行代码打印出的值
# ------------------------------------------------------------------------------
# if __name__ == "__main__": # 这个函数用到了多线程, 所以必须在 __main__ 里跑
#     snapshot_id = "4328d4511415a77eea41c3b091eb0e2a"
#     missav.create_dynamodb_import_data_files(
#         snapshot_id=snapshot_id,
#         lang_code=missav.LangCodeEnum.cn,
#         # _first_k_file=1,
#         # _first_k_url=50,
#     )

# ------------------------------------------------------------------------------
# 把下载好的 sitemap_items_*.xml.gz 文件中的数据作为 pending task list 写入 DynamoDB 中.
# 这里用到了 import Dynamodb data from S3.
# 这里的 snapshot_id 是上面 ``print(f"{sitemap_snapshot.md5 = }")`` 这行代码打印出的值
# ------------------------------------------------------------------------------
# if __name__ == "__main__":  # 这个函数用到了多线程, 所以必须在 __main__ 里跑
#     snapshot_id = "4328d4511415a77eea41c3b091eb0e2a"
#     missav.import_dynamodb_data(
#         snapshot_id=snapshot_id,
#         lang_code=missav.LangCodeEnum.cn,
#         # _first_k_file=1,
#         # _first_k_url=50,
#     )

# ------------------------------------------------------------------------------
# 如果是要把尝试运行爬虫, 就运行下面的代码.
# ------------------------------------------------------------------------------
# missav.crawl_todo(
#     lang_code=missav.LangCodeEnum.cn,
# )


# ------------------------------------------------------------------------------
# 如果是要把 DynamoDB 中的数据 export 到 S3, 就运行下面的代码.
# ------------------------------------------------------------------------------
# missav.export_dynamodb(
#     lang_code=missav.LangCodeEnum.cn,
# )

# ------------------------------------------------------------------------------
# 如果是要把 DynamoDB export 的数据转存为一个本地的 Sqlite 数据库, 就运行下面的代码.
# ------------------------------------------------------------------------------
# missav.dynamodb_to_sqlite(
#     lang_code=missav.LangCodeEnum.cn,
#     export_name="01721229501033-b054a93e",
# )

# ------------------------------------------------------------------------------
# 如果是要从所有已经下载好的 HTML 中提取 video details 数据, 就运行下面的代码.
# ------------------------------------------------------------------------------
# missav.extract_video_details(
#     lang_code=missav.LangCodeEnum.cn,
# )
