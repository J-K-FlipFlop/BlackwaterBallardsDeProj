import datetime
from typing import TYPE_CHECKING, Any, Callable, Iterator, Literal, overload

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler.typing import ArrowDecryptionConfiguration, RayReadParquetSettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

def _pyarrow_parquet_file_wrapper(
    source: Any, coerce_int96_timestamp_unit: str | None = ...
) -> pa.parquet.ParquetFile: ...
def _read_parquet_metadata_file(
    s3_client: "S3Client" | None,
    path: str,
    s3_additional_kwargs: dict[str, str] | None,
    use_threads: bool | int,
    version_id: str | None = ...,
    coerce_int96_timestamp_unit: str | None = ...,
) -> pa.schema: ...
def _read_parquet(
    paths: list[str],
    path_root: str | None,
    schema: pa.schema | None,
    columns: list[str] | None,
    coerce_int96_timestamp_unit: str | None,
    use_threads: bool | int,
    parallelism: int,
    version_ids: dict[str, str] | None,
    s3_client: "S3Client" | None,
    s3_additional_kwargs: dict[str, Any] | None,
    arrow_kwargs: dict[str, Any],
    bulk_read: bool,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
def _read_parquet_metadata(
    path: str | list[str],
    path_suffix: str | None,
    path_ignore_suffix: str | list[str] | None,
    ignore_empty: bool,
    ignore_null: bool,
    dtype: dict[str, str] | None,
    sampling: float,
    dataset: bool,
    use_threads: bool | int,
    boto3_session: boto3.Session | None,
    s3_additional_kwargs: dict[str, str] | None,
    version_id: str | dict[str, str] | None = ...,
    coerce_int96_timestamp_unit: str | None = ...,
) -> tuple[dict[str, str], dict[str, str] | None, dict[str, list[str]] | None]: ...
def read_parquet_metadata(
    path: str | list[str],
    dataset: bool = ...,
    version_id: str | dict[str, str] | None = ...,
    path_suffix: str | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    ignore_empty: bool = ...,
    ignore_null: bool = ...,
    dtype: dict[str, str] | None = ...,
    sampling: float = ...,
    coerce_int96_timestamp_unit: str | None = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
) -> tuple[dict[str, str], dict[str, str] | None]: ...
@overload
def read_parquet(
    path: str | list[str],
    path_root: str | None = ...,
    dataset: bool = ...,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    ignore_empty: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    columns: list[str] | None = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: str | None = ...,
    schema: pa.Schema | None = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    version_id: str | dict[str, str] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: Literal[False] = ...,
    use_threads: bool | int = ...,
    ray_args: RayReadParquetSettings | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
    decryption_configuration: ArrowDecryptionConfiguration | None = ...,
) -> pd.DataFrame: ...
@overload
def read_parquet(
    path: str | list[str],
    *,
    path_root: str | None = ...,
    dataset: bool = ...,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    ignore_empty: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    columns: list[str] | None = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: str | None = ...,
    schema: pa.Schema | None = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    version_id: str | dict[str, str] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: Literal[True],
    use_threads: bool | int = ...,
    ray_args: RayReadParquetSettings | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
    decryption_configuration: ArrowDecryptionConfiguration | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_parquet(
    path: str | list[str],
    *,
    path_root: str | None = ...,
    dataset: bool = ...,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    ignore_empty: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    columns: list[str] | None = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: str | None = ...,
    schema: pa.Schema | None = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    version_id: str | dict[str, str] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: bool,
    use_threads: bool | int = ...,
    ray_args: RayReadParquetSettings | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
    decryption_configuration: ArrowDecryptionConfiguration | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_parquet(
    path: str | list[str],
    *,
    path_root: str | None = ...,
    dataset: bool = ...,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    ignore_empty: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    columns: list[str] | None = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: str | None = ...,
    schema: pa.Schema | None = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    version_id: str | dict[str, str] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: int,
    use_threads: bool | int = ...,
    ray_args: RayReadParquetSettings | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
    decryption_configuration: ArrowDecryptionConfiguration | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_parquet_table(
    table: str,
    database: str,
    *,
    filename_suffix: str | list[str] | None = ...,
    filename_ignore_suffix: str | list[str] | None = ...,
    catalog_id: str | None = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    columns: list[str] | None = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: Literal[False] = ...,
    use_threads: bool | int = ...,
    ray_args: RayReadParquetSettings | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
    decryption_configuration: ArrowDecryptionConfiguration | None = ...,
) -> pd.DataFrame: ...
@overload
def read_parquet_table(
    table: str,
    database: str,
    *,
    filename_suffix: str | list[str] | None = ...,
    filename_ignore_suffix: str | list[str] | None = ...,
    catalog_id: str | None = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    columns: list[str] | None = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: Literal[True],
    use_threads: bool | int = ...,
    ray_args: RayReadParquetSettings | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
    decryption_configuration: ArrowDecryptionConfiguration | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_parquet_table(
    table: str,
    database: str,
    *,
    filename_suffix: str | list[str] | None = ...,
    filename_ignore_suffix: str | list[str] | None = ...,
    catalog_id: str | None = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    columns: list[str] | None = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: bool,
    use_threads: bool | int = ...,
    ray_args: RayReadParquetSettings | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
    decryption_configuration: ArrowDecryptionConfiguration | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_parquet_table(
    table: str,
    database: str,
    *,
    filename_suffix: str | list[str] | None = ...,
    filename_ignore_suffix: str | list[str] | None = ...,
    catalog_id: str | None = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    columns: list[str] | None = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: int,
    use_threads: bool | int = ...,
    ray_args: RayReadParquetSettings | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
    decryption_configuration: ArrowDecryptionConfiguration | None = ...,
) -> Iterator[pd.DataFrame]: ...