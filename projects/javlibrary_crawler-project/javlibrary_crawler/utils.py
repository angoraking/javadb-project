# -*- coding: utf-8 -*-

from datetime import datetime, timezone

from boto_session_manager import BotoSesManager
from s3pathlib import S3Path
import aws_console_url.api as aws_console_url
import aws_arns.api as aws_arns
from .vendor.dynamodb_export_to_s3 import Export

from .logger import logger


def get_utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)


def to_s3_key_friendly_url(url) -> str:
    return url.replace(":", "_").replace("/", "_")


def preview_export_details(
    bsm: BotoSesManager,
    table_name: str,
    export_name: str,
) -> Export:
    """
    Print the DynamoDB export details.
    """
    aws_console = aws_console_url.AWSConsole.from_bsm(bsm)
    table_arn = aws_arns.res.DynamodbTable.new(
        aws_account_id=bsm.aws_account_id,
        aws_region=bsm.aws_region,
        table_name=table_name,
    ).to_arn()
    table_url = aws_console.dynamodb.get_table(table_arn)
    logger.info(f"preview DynamoDB table: {table_url}")

    export_url = aws_console.dynamodb.get_table_export(
        table_or_arn=table_arn,
        export_name=export_name,
    )
    logger.info(f"preview DynamoDB export: {export_url}")

    export_arn = aws_arns.res.DynamodbTableExport.new(
        aws_account_id=bsm.aws_account_id,
        aws_region=bsm.aws_region,
        table_name=table_name,
        export_name=export_name,
    ).to_arn()
    export = Export.describe_export(
        dynamodb_client=bsm.dynamodb_client,
        export_arn=export_arn,
    )
    s3_url = S3Path(export.s3uri_export_data).console_url
    logger.info(f"preview DynamoDB export S3 Location: {s3_url}")
    return export
