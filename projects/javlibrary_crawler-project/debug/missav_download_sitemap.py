# -*- coding: utf-8 -*-

from javlibrary_crawler.sites.missav.sitemap import SiteMapSnapshot, parse_item_xml, dir_missav_sitemap

# md5 = SiteMapSnapshot.new()
sitemap_snapshot = SiteMapSnapshot.new("4d05151dd6a437da3bbbb45407376b39")
# sitemap_snapshot.download()

p = sitemap_snapshot.dir_sitemap_snapshot / "sitemap_items_58.xml.gz"
parse_item_xml(p)