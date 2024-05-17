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

with sqlite_round_trip(
    bsm=bsm,
    s3path=config.env.s3path_missav_crawler_sqlite,
    path=missav.path_missav_crawler_db,
) as engine:
    missav.Base.metadata.create_all(engine)
    sitemap_snapshot = missav.SiteMapSnapshot(md5="4d05151dd6a437da3bbbb45407376b39")
    path_xml = sitemap_snapshot.dir_sitemap_snapshot / "sitemap_items_1.xml.gz"
    item_url_list = missav.parse_item_xml(path_xml)
    with orm.Session(engine) as ses:
        stmt = sa.select(missav.ItemJob).limit(10)
        for job in ses.scalars(stmt):
            print(job)
        # for item_url in item_url_list:
        #     item_job = missav.ItemJob.create(
        #         id=item_url.url,
        #         lang=item_url.lang,
        #         status=missav.StatusEnum.pending.value,
        #     )
        #     ses.add(item_job)
        # ses.commit()
