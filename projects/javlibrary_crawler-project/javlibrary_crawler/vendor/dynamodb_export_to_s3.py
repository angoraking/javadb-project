# -*- coding: utf-8 -*-

"""
DynamoDB export to S3 tool box.

Reference:

- DynamoDB data export to Amazon S3: how it works: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html

Usage:

.. code-block:: python

    from aws_dynamodb_export_to_s3 import Export
"""

import typing as T
import enum
import json
import gzip
import dataclasses
from datetime import datetime, timezone

__version__ = "0.1.1"

def _parse_time(s: str) -> datetime:
    """
    Parse UTC datetime string into timezone aware datetime object.
    """
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


@dataclasses.dataclass
class ManifestSummary:
    """
    The ``manifest-summary.json`` file data model.
    """

    table_id: str
    table_arn: str
    s3_bucket: str
    s3_prefix: str
    item_count: int
    output_format: str
    start_time_str: str
    end_time_str: str
    export_time_str: str
    manifest_files_s3_key: str
    billed_size_bytes: int

    @property
    def start_time(self) -> datetime:
        return _parse_time(self.start_time_str)

    @property
    def end_time(self) -> datetime:
        return _parse_time(self.end_time_str)

    @property
    def export_time(self) -> datetime:
        return _parse_time(self.export_time_str)


# {"key": {"S": "encoded_value"}}
T_ITEM = T.Dict[str, T.Dict[str, T.Any]]


@dataclasses.dataclass
class DataFile:
    """
    The ``s3://.../AWSDynamoDB/${timestamp}-${random_str]/data/${random_str}.json.gz``
    data file data model.
    """

    item_count: int
    md5: str
    etag: str
    s3_bucket: str
    s3_key: str

    def read_items(self, s3_client) -> T.List[T_ITEM]:
        """
        Read items from the data file.

        Ref: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html

        Example item::

            {
                'key1': {'S': '...'},
                'attr1': {'S': '...'},
                'attr2': {'N': '...'},
                ...
            },
        """
        res = s3_client.get_object(
            Bucket=self.s3_bucket,
            Key=self.s3_key,
        )
        lines = gzip.decompress(res["Body"].read()).decode("utf-8").splitlines()
        return [json.loads(line)["Item"] for line in lines]


def parse_s3uri(s3uri: str) -> T.Tuple[str, str]:
    """
    Parse S3 URI into bucket and key.

    Example::

        >>> parse_s3uri("s3://my-bucket/folder/file.txt")
        ('my-bucket', 'folder/file.txt')
    """
    parts = s3uri.split("/", 3)
    bucket = parts[2]
    key = parts[3]
    return bucket, key


class ExportStatusEnum(enum.Enum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ExportFormatEnum(enum.Enum):
    DYNAMODB_JSON = "DYNAMODB_JSON"
    ION = "ION"


@dataclasses.dataclass
class Export:
    """
    The DynamoDB export data model. Currently, it only supports the ``DYNAMODB_JSON``
    format.
    """

    arn: str = dataclasses.field()
    status: str = dataclasses.field()
    start_time: T.Optional[datetime] = dataclasses.field(default=None)
    end_time: T.Optional[datetime] = dataclasses.field(default=None)
    export_time: T.Optional[datetime] = dataclasses.field(default=None)
    table_arn: T.Optional[str] = dataclasses.field(default=None)
    table_id: T.Optional[str] = dataclasses.field(default=None)
    s3_bucket: T.Optional[str] = dataclasses.field(default=None)
    s3_prefix: T.Optional[str] = dataclasses.field(default=None)
    item_count: T.Optional[int] = dataclasses.field(default=None)
    export_format: T.Optional[str] = dataclasses.field(default=None)
    failure_code: T.Optional[str] = dataclasses.field(default=None)
    failure_message: T.Optional[str] = dataclasses.field(default=None)
    export_manifest: T.Optional[str] = dataclasses.field(default=None)

    def __post_init__(self):
        if self.s3_prefix is not None:
            if self.s3_prefix.endswith("/") is False:
                self.s3_prefix += "/"

    @classmethod
    def list_exports(
        cls,
        dynamodb_client,
        table_arn: str,
        page_size: int = 25,
        max_results: int = 1000,
        get_details: bool = False,
    ) -> T.List["Export"]:
        """
        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/list_exports.html
        """
        exports = list()
        next_token = None
        while 1:
            kwargs = dict(
                TableArn=table_arn,
                MaxResults=page_size,
            )
            if next_token is not None:
                kwargs["NextToken"] = next_token
            res = dynamodb_client.list_exports(**kwargs)
            for dct in res.get("ExportSummaries", []):
                export_arn = dct["ExportArn"]
                export_status = dct["ExportStatus"]
                if get_details:
                    export = cls.describe_export(dynamodb_client, export_arn)
                else:
                    export = cls(arn=export_arn, status=export_status)
                exports.append(export)
                if len(exports) >= max_results:
                    return exports

            _next_token = res.get("NextToken", "NOT_AVAILABLE")
            if _next_token == "NOT_AVAILABLE":
                break
            else:
                next_token = _next_token
        return exports

    @classmethod
    def _from_export_description(cls, desc: dict):
        return cls(
            arn=desc["ExportArn"],
            status=desc["ExportStatus"],
            start_time=desc.get("StartTime"),
            end_time=desc.get("EndTime"),
            export_time=desc.get("ExportTime"),
            table_arn=desc.get("TableArn"),
            table_id=desc.get("TableId"),
            s3_bucket=desc.get("S3Bucket"),
            s3_prefix=desc.get("S3Prefix"),
            item_count=desc.get("ItemCount"),
            export_format=desc.get("ExportFormat"),
            failure_code=desc.get("FailureCode"),
            failure_message=desc.get("FailureMessage"),
            export_manifest=desc.get("ExportManifest"),
        )

    @classmethod
    def describe_export(
        cls,
        dynamodb_client,
        export_arn: str,
    ) -> T.Optional["Export"]:
        """
        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_export.html
        """
        try:
            res = dynamodb_client.describe_export(ExportArn=export_arn)
        except Exception as e:
            if "not found" in str(e).lower():
                return None
            else:
                raise NotImplementedError

        desc = res["ExportDescription"]
        return cls._from_export_description(desc)

    def is_in_progress(self) -> bool:
        return self.status == ExportStatusEnum.IN_PROGRESS.value

    def is_completed(self) -> bool:
        return self.status == ExportStatusEnum.COMPLETED.value

    def is_failed(self) -> bool:
        return self.status == ExportStatusEnum.FAILED.value

    def is_dynamodb_json_format(self) -> bool:
        return self.export_format == ExportFormatEnum.DYNAMODB_JSON.value

    def is_ion_format(self) -> bool:
        return self.export_format == ExportFormatEnum.ION.value

    @property
    def export_short_id(self) -> str:
        """
        The short ID of the export, which is a compound of the export timestamp
        and random string. Example: ``1672531200000-a1b2c3d4``. 1672531200000
        is the timestamp of 2023-01-01 00:00:00
        """
        return self.arn.split("/")[-1]

    @property
    def s3uri_export(self) -> str:
        """
        The S3 folder you specified when you call the
        ``dynamodb_client.export_table_to_point_in_time(...)`` API.

        Example: s3://bucket/prefix/
        """
        return f"s3://{self.s3_bucket}/{self.s3_prefix}"

    @property
    def _s3uri_export_root(self) -> str:
        return f"{self.s3uri_export}AWSDynamoDB/{self.export_short_id}/"

    @property
    def s3uri_export_data(self) -> str:
        """
        Where the export data files are stored.

        Example: s3://bucket/prefix/AWSDynamoDB/1672531200000-a1b2c3d4/data/
        """
        return f"{self._s3uri_export_root}data/"

    @property
    def s3uri_export_manifest_files(self) -> str:
        """
        The S3 location of the manifest files.

        Example: s3://bucket/prefix/AWSDynamoDB/1672531200000-a1b2c3d4/manifest-files.json
        """
        return f"{self._s3uri_export_root}manifest-files.json"

    @property
    def s3uri_export_manifest_summary(self) -> str:
        """
        The S3 location of the manifest summary file.

        Example: s3://bucket/prefix/AWSDynamoDB/1672531200000-a1b2c3d4/manifest-summary.json
        """
        return f"{self._s3uri_export_root}manifest-summary.json"

    def get_details(self, dynamodb_client):
        """
        Get the details of the DynamoDB export, refresh it's attributes values.
        """
        export = self.describe_export(
            dynamodb_client=dynamodb_client, export_arn=self.arn
        )
        for field in dataclasses.fields(self.__class__):
            setattr(self, field.name, getattr(export, field.name))
        self.__post_init__()

    def _ensure_details(self, dynamodb_client):
        """
        Ensure that the details of the DynamoDB export are available.
        If not, call ``get_details`` to refresh the attributes values.
        """
        if self.s3_bucket is None:
            self.get_details(dynamodb_client=dynamodb_client)

    def get_manifest_summary(
        self,
        dynamodb_client,
        s3_client,
    ) -> ManifestSummary:
        """
        Get the manifest summary of the DynamoDB export.
        """
        self._ensure_details(dynamodb_client=dynamodb_client)
        bucket, key = parse_s3uri(self.s3uri_export_manifest_summary)
        res = s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )
        data = json.loads(res["Body"].read().decode("utf-8"))
        return ManifestSummary(
            table_id=data["tableId"],
            table_arn=data["tableArn"],
            s3_bucket=data["s3Bucket"],
            s3_prefix=data["s3Prefix"],
            item_count=data["itemCount"],
            output_format=data["outputFormat"],
            start_time_str=data["startTime"],
            end_time_str=data["endTime"],
            export_time_str=data["exportTime"],
            manifest_files_s3_key=data["manifestFilesS3Key"],
            billed_size_bytes=data["billedSizeBytes"],
        )

    def get_data_files(
        self,
        dynamodb_client,
        s3_client,
    ) -> T.List[DataFile]:
        """
        Get the list of data files of the DynamoDB export.
        """
        self._ensure_details(dynamodb_client=dynamodb_client)
        bucket, key = parse_s3uri(self.s3uri_export_manifest_files)
        res = s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )
        lines = res["Body"].read().decode("utf-8").splitlines()
        data_file_list = list()
        for line in lines:
            data = json.loads(line)
            data_file_list.append(
                DataFile(
                    item_count=data["itemCount"],
                    md5=data["md5Checksum"],
                    etag=data["etag"],
                    s3_bucket=bucket,
                    s3_key=data["dataFileS3Key"],
                )
            )
        return data_file_list

    def read_items(
        self,
        dynamodb_client,
        s3_client,
    ) -> T.Iterable[T_ITEM]:
        """
        Read the items of the DynamoDB export. This is a generator function.
        """
        data_file_list = self.get_data_files(
            dynamodb_client=dynamodb_client,
            s3_client=s3_client,
        )
        for data_file in data_file_list:
            for item in data_file.read_items(s3_client=s3_client):
                yield item

    @classmethod
    def export_table_to_point_in_time(
        cls,
        dynamodb_client,
        table_arn: str,
        s3_bucket: str,
        s3_prefix: T.Optional[str] = None,
        export_time: T.Optional[datetime] = None,
        s3_bucket_owner: T.Optional[datetime] = None,
        s3_sse_algorithm: T.Optional[datetime] = None,
        s3_sse_kms_key_id: T.Optional[datetime] = None,
        export_format: str = ExportFormatEnum.DYNAMODB_JSON.value,
        client_token: T.Optional[str] = None,
    ):
        """
        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/export_table_to_point_in_time.html
        """
        kwargs = dict(
            TableArn=table_arn,
            S3Bucket=s3_bucket,
            S3Prefix=s3_prefix,
            ExportTime=export_time,
            S3BucketOwner=s3_bucket_owner,
            S3SseAlgorithm=s3_sse_algorithm,
            S3SseKmsKeyId=s3_sse_kms_key_id,
            ExportFormat=export_format,
            ClientToken=client_token,
        )
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        res = dynamodb_client.export_table_to_point_in_time(**kwargs)
        desc = res["ExportDescription"]
        return cls._from_export_description(desc)