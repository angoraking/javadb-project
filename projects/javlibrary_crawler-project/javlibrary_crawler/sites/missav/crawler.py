# -*- coding: utf-8 -*-

import typing as T
import time

from javlibrary_crawler.utils import to_s3_key_friendly_url

from .dynamodb import (
    BaseTask,
    lang_to_step1_mapping,
)
from .constants import LangCodeEnum
from .sitemap import SiteMapSnapshot, parse_item_xml, ItemUrl


def insert_todo_list(
    snapshot_id: str,
    lang_code: LangCodeEnum,
    export_arn: T.Optional[str] = None,
):
    """
    把从 sitemap 中解析出来的, 在 DynamoDB 中不存在的 URL 插入到 DynamoDB 中.

    :param snapshot_id: sitemap 的 MD5 哈希值
    :param lang_code: 语言代码, 这会决定数据会插入到哪个表中
    :param export_arn: 如果你的 DynamoDB 中已经有很多数据了, 由于用 DynamoDB 直接
        filter 哪些 url 已经存在哪些不存在不是很方便, 所以会提前将数据导出到 S3 中.
    """
    sitemap_snapshot = SiteMapSnapshot.new(md5=snapshot_id)
    klass: T.Type[BaseTask] = lang_to_step1_mapping[lang_code.value]
    klass.create_table(wait=True)
    with klass.batch_write() as batch:
        # filter by only
        if export_arn:
            raise NotImplementedError
        else:
            path_list = sitemap_snapshot.get_item_xml_list()
            path_list = path_list[:1]
            for path in path_list:
                item_url_list = parse_item_xml(path)
                filtered_item_url_list = ItemUrl.filter_by_lang(
                    item_url_list, lang_code
                )
                filtered_item_url_list = filtered_item_url_list[:10]
                for item in filtered_item_url_list:
                    task = klass.make_and_save(
                        task_id=to_s3_key_friendly_url(item.url),
                        url=item.url,
                    )
                    batch.save(task)


def crawl_todo(
    lang_code: LangCodeEnum,
):
    """
    去 DynamoDB 中找到未完成的任务, 并执行下载任务.
    """
    klass: T.Type[BaseTask] = lang_to_step1_mapping[lang_code.value]
    task: BaseTask
    for task in klass.query_for_unfinished(
        limit=3,
        older_task_first=False,
    ):
        with klass.start(
            task_id=task.task_id,
            debug=True,
        ) as exec_ctx:
            task_on_the_fly: BaseTask = exec_ctx.task
            task_on_the_fly.do_download_task()

        time.sleep(1)
