# -*- coding: utf-8 -*-

import typing as T
import dataclasses

from s3pathlib import S3Path

from ..._version import __version__

if T.TYPE_CHECKING:  # pragma: no cover
    from .main import Env


@dataclasses.dataclass
class AppMixin:
    username: T.Optional[str] = dataclasses.field(default=None)
    password: T.Optional[str] = dataclasses.field(default=None)

    @property
    def s3dir_source(self: "Env") -> S3Path:
        return self.s3dir_env_data.joinpath("source").to_dir()

    @property
    def s3dir_target(self: "Env") -> S3Path:
        return self.s3dir_env_data.joinpath("target").to_dir()

    @property
    def s3dir_sites(self: "Env") -> S3Path:
        return self.s3dir_env_data.joinpath("sites").to_dir()

    @property
    def s3dir_missav(self: "Env") -> S3Path:
        return self.s3dir_sites.joinpath("missav").to_dir()

    @property
    def s3path_missav_import_dynamodb_table_temp_json_gz(self: "Env") -> S3Path:
        return self.s3dir_missav.joinpath("import_dynamodb_table_temp.json.gz")

    @property
    def s3dir_missav_downloads(self: "Env") -> S3Path:
        return self.s3dir_missav.joinpath("downloads").to_dir()

    @property
    def s3dir_missav_dynamodb_exports(self: "Env") -> S3Path:
        return self.s3dir_missav.joinpath("dynamodb_exports").to_dir()

    @property
    def s3path_missav_crawler_sqlite(self: "Env") -> S3Path:
        return self.s3dir_missav.joinpath("missav_crawler.sqlite")

    def get_tracker_dynamodb_table_name(
        self: "Env",
        site: str,
        language: str,
    ) -> str:
        """
        每个 site 的每个语种都是一个单独的 DynamoDB 表.
        """
        return f"{self.prefix_name_snake}-{site}-{language}-status-tracker"

    @property
    def exponential_backoff_dynamodb_table_name(self: "Env") -> str:
        return f"{self.prefix_name_snake}-exponential-backoff"

    @property
    def env_vars(self: "Env") -> T.Dict[str, str]:
        """
        Common environment variable for all computational resources in this environment.
        It is primarily for "self awareness" (detect who I am, which environment I am in).
        """
        env_vars = super().env_vars
        env_vars["PACKAGE_VERSION"] = __version__
        return env_vars

    @property
    def devops_aws_tags(self: "Env") -> T.Dict[str, str]:
        """
        Common AWS resources tags for all resources in devops environment.
        """
        tags = super().devops_aws_tags
        tags["tech:package_version"] = __version__
        return tags

    @property
    def workload_aws_tags(self: "Env") -> T.Dict[str, str]:
        """
        Common AWS resources tags for all resources in workload environment.
        """
        tags = super().workload_aws_tags
        tags["tech:package_version"] = __version__
        return tags
