# -*- coding: utf-8 -*-

import typing as T
import aws_cdk as cdk

from aws_cdk import (
    aws_iam as iam,
)

if T.TYPE_CHECKING:  # pragma: no cover
    from .main import MainStack


class IamMixin:
    def mk_rg1_iam(self: "MainStack"):
        """
        IAM related resources.

        Ref:

        - IAM Object quotas: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html#reference_iam-quotas-entities
        """

        self.stat_parameter_store = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["ssm:GetParameter"],
            resources=[
                f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/{self.env.parameter_name}"
            ],
        )

        self.stat_s3_bucket_read = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:ListBucket",
                "s3:GetObject",
                "s3:GetObjectAttributes",
                "s3:GetObjectTagging",
            ],
            resources=[
                f"arn:aws:s3:::{self.env.s3dir_env_data.bucket}",
                f"arn:aws:s3:::{self.env.s3dir_env_data.bucket}/{self.env.s3dir_data.key}*",
                f"arn:aws:s3:::{self.env.s3dir_glue_artifacts.bucket}",
                f"arn:aws:s3:::{self.env.s3dir_glue_artifacts.bucket}/{self.env.s3dir_glue_artifacts.key}*",
            ],
        )

        self.stat_s3_bucket_write = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:PutObjectTagging",
                "s3:DeleteObjectTagging",
            ],
            resources=[
                f"arn:aws:s3:::{self.env.s3dir_env_data.bucket}",
                f"arn:aws:s3:::{self.env.s3dir_env_data.bucket}/{self.env.s3dir_data.key}*",
            ],
        )

        # declare iam role
        self.iam_role_for_glue = iam.Role(
            self,
            "IamRoleForGlue",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=f"{self.env.prefix_name_snake}-{cdk.Aws.REGION}-glue",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceNotebookRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "PowerUserAccess",
                ),
            ],
            inline_policies={
                f"{self.env.prefix_name_snake}-{cdk.Aws.REGION}-glue": iam.PolicyDocument(
                    statements=[
                        self.stat_parameter_store,
                        self.stat_s3_bucket_read,
                        self.stat_s3_bucket_write,
                    ]
                )
            },
        )

        self.output_iam_role_for_glue_arn = cdk.CfnOutput(
            self,
            "IamRoleForGlueArn",
            value=self.iam_role_for_glue.role_arn,
            export_name=f"{self.env.prefix_name_slug}-glue-role-arn",
        )
