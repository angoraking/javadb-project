# -*- coding: utf-8 -*-


from javlibrary_crawler.config.load import config, Config
from javlibrary_crawler.boto_ses import bsm
from javlibrary_crawler.db import create_sqlite_engine, sqlite_round_trip
import javlibrary_crawler.sites.missav.api as missav

config: Config

snapshot_md5 = "4d05151dd6a437da3bbbb45407376b39"
sitemap_snapshot = missav.SiteMapSnapshot(md5=snapshot_md5)

path_xml = sitemap_snapshot.dir_sitemap_snapshot / "sitemap_items_1.xml.gz"
item_url_list = missav.parse_item_xml(path_xml)
allowed_lang_code_set = {
    missav.LangCodeEnum.ja.value,
    missav.LangCodeEnum.zh.value,
    missav.LangCodeEnum.cn.value,
}

path_xml = sitemap_snapshot.dir_sitemap_snapshot / "sitemap_items_1.xml.gz"
item_url_list = missav.parse_item_xml(path_xml)
allowed_lang_code_set = {
    missav.LangCodeEnum.ja.value,
    missav.LangCodeEnum.zh.value,
    missav.LangCodeEnum.cn.value,
}
with orm.Session(engine) as ses:
    for item_url in item_url_list:
        if item_url.lang in allowed_lang_code_set:
            item_job = missav.ItemJob.create(
                id=item_url.url,
                lang=item_url.lang,
                status=missav.StatusEnum.pending.value,
            )
            ses.add(item_job)
    ses.commit()