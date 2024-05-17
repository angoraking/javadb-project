# -*- coding: utf-8 -*-

import typing as T
import contextlib

import sqlalchemy as sa
from pathlib_mate import Path
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager


def create_sqlite_engine(path_db: Path) -> sa.engine.Engine:
    return sa.create_engine(f"sqlite:///{path_db}")


@contextlib.contextmanager
def sqlite_round_trip(
    bsm: BotoSesManager,
    s3path: S3Path,
    path: Path,
) -> T.ContextManager[sa.Engine]:
    """
    由于 sqlite 并不是一个基于网络的数据库, 也不支持多进程同时写入. 所以我们需要一个机制来保证
    数据库文件的一致性. 我们在对数据库进行任何写操作的时, 我们会将数据库先从 S3 下载到本地, 然后
    进行写入, 结束后再上传回 S3. 这个函数用 context manager 的形式将这套逻辑封装好了.

    我们会自己保证同一时间只有一个 worker 在操作这个数据库. 所以不需要考虑多进程写入的问题.
    """
    try:
        # download from S3Path if the sqlite file exists on S3
        if s3path.exists(bsm=bsm):
            s3path_exists = True
            path.remove_if_exists()
            bsm.s3_client.download_file(
                Bucket=s3path.bucket, Key=s3path.key, Filename=str(path)
            )
        else:
            s3path_exists = False
        engine = create_sqlite_engine(path)
        yield engine
    except Exception as e:
        raise e
    finally:
        local_md5 = path.md5
        # only upload back if the local file has been modified.
        if s3path_exists:
            if s3path.metadata["md5"] != local_md5:
                s3path.upload_file(
                    path,
                    bsm=bsm,
                    overwrite=True,
                    extra_args={"Metadata": {"md5": local_md5}},
                )
        else:
            s3path.upload_file(
                path,
                bsm=bsm,
                overwrite=True,
                extra_args={"Metadata": {"md5": local_md5}},
            )
