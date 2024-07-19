# -*- coding: utf-8 -*-

import typing as T
import os
import time
import json
import gzip
from datetime import datetime, timedelta

from s3pathlib import S3Path
import aws_console_url.api as aws_console_url
import aws_arns.api as aws_arns
import pynamodb_mate.api as pm
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam

from github import Github

from javlibrary_crawler.utils import (
    get_utc_now,
    to_s3_key_friendly_url,
    preview_export_details,
)
from javlibrary_crawler.logger import logger
from javlibrary_crawler.boto_ses import bsm
from javlibrary_crawler.runtime import runtime
from javlibrary_crawler.config.load import config
from javlibrary_crawler.vendor.dynamodb_export_to_s3 import Export
from javlibrary_crawler.vendor.waiter import Waiter

from .sqlitedb import Base, Step2ParseHtmlStatusEnum, Job

from .constants import LangCodeEnum
from .paths import dir_missav
from .sitemap import SiteMapSnapshot, parse_item_xml, ItemUrl
from .dynamodb import (
    BaseTask,
    lang_to_step1_mapping,
)
from .downloader import MalformedHtmlError

@logger.emoji_block(
    msg="Insert todo List to DynamoDB",
    emoji="📥",
)
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
            logger.info(f"Got {len(path_list)} xml file to insert")
            for path in path_list:
                logger.info(f"Working on {path.basename} file")
                item_url_list = parse_item_xml(path)
                filtered_item_url_list = ItemUrl.filter_by_lang(
                    item_url_list, lang_code
                )
                filtered_item_url_list = filtered_item_url_list[:20]
                with logger.indent():
                    logger.info(f"Got {len(filtered_item_url_list)} url to insert")
                for item in filtered_item_url_list:
                    task = klass.make_and_save(task_id=item.url)
                    batch.save(task)


@logger.emoji_block(
    msg="Crawl todo",
    emoji="🕸",
)
def crawl_todo(
    lang_code: LangCodeEnum,
):
    """
    去 DynamoDB 中找到未完成的任务, 并执行下载任务.
    """
    klass: T.Type[BaseTask] = lang_to_step1_mapping[lang_code.value]

    # set the right PynamoDB connection
    with bsm.awscli():
        klass._connection = None
        klass.Meta.region = bsm.aws_region
        conn = pm.Connection()
        klass.create_table(wait=True)

    task_list: T.List[BaseTask] = klass.query_for_unfinished(
        limit=1,
        older_task_first=False,
    ).all()
    logger.info(f"Got {len(task_list)} URL to crawl.")

    if runtime.is_github_action:
        g = Github()
        github_run_id = int(os.environ["GITHUB_RUN_ID"])
        wf_run = g.get_repo("angoraking/javadb-project").get_workflow_run(github_run_id)
        start_at = wf_run.run_started_at
    else:
        start_at = get_utc_now()

    # the max time we can run for this ``crawl_todo`` function
    max_job_run_time = 300 # 5 min
    # the max time we can spend on each download task
    each_download_consumed_time = 5
    end_at = start_at + timedelta(seconds=max_job_run_time)
    for task in task_list:
        now = get_utc_now()
        if now >= end_at:
            logger.info("Time is up!")
            break

        how_many_time_left = (end_at - now).total_seconds()
        if how_many_time_left < each_download_consumed_time:
            logger.info("Time is up!")
            break

        logger.info(f"====== Working on {task.url} ======")
        try:
            with klass.start(
                task_id=task.task_id,
                debug=True,
            ) as exec_ctx:
                task_on_the_fly: BaseTask = exec_ctx.task
                task_on_the_fly.do_download_task() # this function has auto retry
        except MalformedHtmlError as e:
            pass
        except Exception as e:
            raise e

        time.sleep(1)


def export_dynamodb(
    lang_code: LangCodeEnum,
    export_time: T.Optional[datetime] = None,
    delays: int = 10,
    timeout: int = 600,
    wait: bool = True,
) -> Export:
    """
    将 DynamoDB 整个表导出到 S3 中.
    """
    klass: T.Type[BaseTask] = lang_to_step1_mapping[lang_code.value]
    url = klass.get_table_overview_console_url()
    print(f"preview table: {url}")
    res = bsm.dynamodb_client.update_continuous_backups(
        TableName=klass.Meta.table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )

    if export_time is None:
        export_time = get_utc_now()
    client_token = export_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f"{client_token = }")
    table_arn = aws_arns.res.DynamodbTable.new(
        aws_account_id=bsm.aws_account_id,
        aws_region=bsm.aws_region,
        table_name=klass.Meta.table_name,
    ).to_arn()
    print(f"export s3 location = {config.env.s3dir_missav_dynamodb_exports.uri}")
    print(
        f"preview export s3 location = {config.env.s3dir_missav_dynamodb_exports.console_url}"
    )

    export = Export.export_table_to_point_in_time(
        dynamodb_client=bsm.dynamodb_client,
        table_arn=table_arn,
        s3_bucket=config.env.s3dir_missav_dynamodb_exports.bucket,
        s3_prefix=config.env.s3dir_missav_dynamodb_exports.key,
        export_time=export_time,
        client_token=client_token,
    )
    export_name = export.arn.split("/")[-1]
    aws_console = aws_console_url.AWSConsole.from_bsm(bsm)
    export_url = aws_console.dynamodb.get_table_export(
        table_or_arn=table_arn,
        export_name=export_name,
    )
    print(f"{export_url = }")

    if wait:
        for _ in Waiter(
            delays=delays,
            timeout=timeout,
        ):
            export.get_details(dynamodb_client=bsm.dynamodb_client)
            if export.is_completed():
                break
            elif export.is_failed():
                raise RuntimeError(f"Dynamodb Export failed!")

    return export


@logger.emoji_block(
    msg="DynamoDB to Sqlite",
    emoji="📄",
)
def dynamodb_to_sqlite(
    lang_code: LangCodeEnum,
    export_name: str,
    remove_existing: bool = False,
):
    """
    Load DynamoDB Export data into Sqlite Database. We will use this database
    to track "parse html" job status.
    """
    path_sqlite = dir_missav.joinpath(f"{lang_code.name}.sqlite")
    if remove_existing:  # pragma: no cover
        path_sqlite.remove_if_exists()
    klass: T.Type[BaseTask] = lang_to_step1_mapping[lang_code.value]
    logger.info("Information")
    with logger.indent():
        logger.info(f"{lang_code = }")
        logger.info(f"{export_name = }")
        export = preview_export_details(
            bsm=bsm,
            table_name=klass.Meta.table_name,
            export_name=export_name,
        )
        logger.info(f"path_sqlite = {path_sqlite}")
        logger.info(f"preview path_sqlite at: file://{path_sqlite.parent}")

    engine = sam.engine_creator.EngineCreator.create_sqlite(str(path_sqlite))
    Base.metadata.create_all(engine)

    if export.is_completed() is False:
        raise SystemError(f"Export is not completed yet!")
    data_file_list = export.get_data_files(
        dynamodb_client=bsm.dynamodb_client,
        s3_client=bsm.s3_client,
    )
    job_list = list()
    total = 0
    for data_file in data_file_list:
        s3uri = f"s3://{data_file.s3_bucket}/{data_file.s3_key}"
        logger.info(f"work on data file: {s3uri}")
        s3path = S3Path(s3uri)
        lines = gzip.decompress(s3path.read_bytes()).splitlines()
        counter = 0
        for line in lines:
            item_data = json.loads(line)
            download_task = klass.from_raw_data(item_data["Item"])
            if download_task.is_succeeded():
                job = Job.create_and_not_save(
                    id=download_task.key,
                    url=download_task.url,
                    html=download_task.html,
                )
                job_list.append(job)
                counter += 1
        with logger.indent():
            logger.info(f"got {counter} succeeded items")
        total += counter

    with orm.Session(engine) as ses:
        ses.add_all(job_list)
        ses.commit()

    logger.info(f"got {total} total succeeded items")


@logger.emoji_block(
    msg="Extract Video Details",
    emoji="📄",
)
def extract_video_details(
    lang_code: LangCodeEnum,
):
    path_sqlite = dir_missav.joinpath(f"{lang_code.name}.sqlite")
    logger.info("Information")
    with logger.indent():
        logger.info(f"{lang_code = }")

    engine = sam.engine_creator.EngineCreator.create_sqlite(str(path_sqlite))
    for job in Job.query_by_status(
        engine_or_session=engine,
        status=Step2ParseHtmlStatusEnum.pending.value,
        limit=3,
        older_task_first=True,
    ):
        Job.do_parse_html_job(
            engine=engine,
            id=job.id,
            bsm=bsm,
            lang=lang_code,
            debug=True,
        )

    # for job in Job.query_by_status(
    #     engine_or_session=engine,
    #     status=Step2ParseHtmlStatusEnum.succeeded.value,
    #     limit=3,
    #     older_task_first=True,
    # ):
    #     print(job.video_detail)
