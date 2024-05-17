# -*- coding: utf-8 -*-

from collections import Counter
import sqlalchemy as sa
import sqlalchemy.orm as orm

from javlibrary_crawler.config.load import config, Config
from javlibrary_crawler.boto_ses import bsm
from javlibrary_crawler.db import create_sqlite_engine, sqlite_round_trip
import javlibrary_crawler.sites.missav.api as missav

config: Config

# md5 = SiteMapSnapshot.new()
# sitemap_snapshot = SiteMapSnapshot.new("4d05151dd6a437da3bbbb45407376b39")
# sitemap_snapshot.download()
# sitemap_snapshot.remove_uncompressed()

# p = sitemap_snapshot.dir_sitemap_snapshot / "sitemap_items_58.xml.gz"
# parse_item_xml(p)

# engine = create_sqlite_engine(missav.path_missav_crawler_db)

snapshot_md5 = "4d05151dd6a437da3bbbb45407376b39"
sitemap_snapshot = missav.SiteMapSnapshot(md5=snapshot_md5)

# with sqlite_round_trip(
#     bsm=bsm,
#     s3path=config.env.s3path_missav_crawler_sqlite,
#     path=missav.path_missav_crawler_db,
# ) as engine:
#     missav.Base.metadata.create_all(engine)
#     path_xml = sitemap_snapshot.dir_sitemap_snapshot / "sitemap_items_1.xml.gz"
#     item_url_list = missav.parse_item_xml(path_xml)
#     allowed_lang_code_set = {
#         missav.LangCodeEnum.ja.value,
#         missav.LangCodeEnum.zh.value,
#         missav.LangCodeEnum.cn.value,
#     }
#     with orm.Session(engine) as ses:
#         for item_url in item_url_list:
#             if item_url.lang in allowed_lang_code_set:
#                 item_job = missav.ItemJob.create(
#                     id=item_url.url,
#                     lang=item_url.lang,
#                     status=missav.StatusEnum.pending.value,
#                 )
#                 ses.add(item_job)
#         ses.commit()


# with sqlite_round_trip(
#     bsm=bsm,
#     s3path=config.env.s3path_missav_crawler_sqlite,
#     path=missav.path_missav_crawler_db,
# ) as engine:
#     with orm.Session(engine) as ses:
#         stmt = (
#             sa.select(missav.ItemJob)
#             .where(
#                 missav.ItemJob.status == missav.StatusEnum.pending,
#                 missav.ItemJob.lang == missav.LangCodeEnum.cn.value,
#             )
#             .order_by(sa.asc(missav.ItemJob.update_at))
#             .limit(10)
#         )
#         item_job_list = ses.scalars(stmt).all()
#         job_id_list = [job.id for job in item_job_list]
#
#     for job_id in job_id_list:
#         missav.ItemJob.do_job(
#             engine=engine,
#             id=job_id,
#             bsm=bsm,
#             s3dir_missav=config.env.s3dir_missav,
#             skip_error=True,
#             debug=True,
#         )