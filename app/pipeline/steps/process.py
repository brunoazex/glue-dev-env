import logging
from datetime import datetime

import pipeline.config
import pipeline.report
import requests
from pipeline.helpers import deterministic_uuid
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from toolz.functoolz import pipe

from .schema import CACHE_SCHEMA, DELTA_COLUMNS, DELTA_SCHEMA

report = pipeline.report.ProcessSection()
logger = logging.getLogger("PROCESS")
deterministic_udf = F.udf(deterministic_uuid, T.StringType())


MAX_REQUEST_COUNT = 499


def enrich_integration(raw_data: DataFrame) -> DataFrame:
    def load_cache() -> DataFrame:
        cache = pipeline.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [pipeline.config.CACHE_S3_PATH]},
            format="parquet",
        ).toDF()
        if cache.isEmpty():
            cache = pipeline.glue_context.sparkSession.createDataFrame([], CACHE_SCHEMA)
        return cache

    new_ids = (
        raw_data.alias("raw")
        .join(load_cache().alias("cache"), ["id"], "leftanti")
        .select("id")
        .collect()
    )
    new_ids_count = len(new_ids)
    new_ids_pages = (new_ids_count // MAX_REQUEST_COUNT) + 1

    for page in range(0, new_ids_pages):
        start = page * MAX_REQUEST_COUNT
        end = start + MAX_REQUEST_COUNT
        page_items = new_ids[start:end]
        response = requests.post(
            f"{pipeline.config.API_URL}/integration",
            json={"ids": [row.id for row in page_items]},
        )
        if response.status_code == 200:
            response_df = (
                pipeline.glue_context.sparkSession.read.json(
                    pipeline.glue_context.sparkSession.sparkContext.parallelize(
                        [response.content]
                    )
                )
                .withColumn(
                    "partition_ref",
                    F.lit(datetime.now().strftime(f"%Y-%m-%d_{page+1}")),
                )
                .distinct()
            )
            response_df.write.parquet(
                pipeline.config.CACHE_S3_PATH, "append", ["partition_ref"]
            )
        elif response.status_code == 204:
            logger.warning(f"Page {page+1} does not have results")
        else:
            logger.error(
                f"Unknown API response {response.status_code}\n{response.json()}"
            )

    cache = load_cache()
    enriched = (
        raw_data.alias("raw")
        .join(cache.alias("cache"), ["id"], "outer")
        .withColumn(
            "state", F.when(F.col("cache.uuid").isNull(), "skipped").otherwise("save")
        )
    )

    matched = enriched.filter(F.col("state") == "save").drop("state")
    not_mached = enriched.filter(F.col("state") == "skipped")

    report.matched_count = matched.count()
    report.not_matched_count = not_mached.count()
    if report.not_matched_count > 0:
        sample = not_mached.take(20)
        logger.warning(f"Not matched: [{','.join([row.id for row in sample])}]")

    return matched


def filter_new_rows(raw_data: DataFrame) -> DataFrame:
    current_delta = pipeline.glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [pipeline.config.DELTA_S3_PATH]},
        format="parquet",
    ).toDF()

    if current_delta.isEmpty():
        current_delta = pipeline.glue_context.sparkSession.createDataFrame(
            [], DELTA_SCHEMA
        )

    calculated = (
        raw_data.withColumn("sha2", F.sha2(F.concat_ws("||", *DELTA_COLUMNS), 256))
        .alias("new")
        .join(
            current_delta.select("code", "id", F.col("sha2").alias("old_sha2")).alias(
                "current"
            ),
            raw_data.code == current_delta.code,
            "left",
        )
        .withColumn(
            "state",
            F.when(
                (F.col("old_sha2").isNull()) | (F.col("sha2") != F.col("old_sha2")),
                "save",
            ).otherwise("skip"),
        )
    )

    new_rows = calculated.filter(F.col("state") == "save").select("new.*")
    skipped_rows = calculated.filter(F.col("state") == "skip")

    report.new_count = new_rows.count()
    report.repeated_count = skipped_rows.count()
    return new_rows


def transform(raw_data: DataFrame) -> DataFrame:
    transformed = raw_data.withColumn(
        "uuid", deterministic_udf(F.concat_ws("||", "uuid", "code"))
    )
    report.transformed_count = transformed.count()
    return transformed


def run() -> pipeline.report.ProcessSection:
    # Arrange
    spark = pipeline.glue_context.sparkSession
    spark.udf.register("deterministic_uuid", deterministic_uuid, T.StringType())
    raw_data = spark.sql(f"SELECT * FROM {pipeline.config.VW_CLEANUP}")
    report.raw_count = raw_data.count()

    # Act
    processed = pipe(raw_data, filter_new_rows, enrich_integration, transform)
    processed.createOrReplaceTempView(pipeline.config.VW_PROCESS)

    # Report
    report.status = "success"
    report.valid_rows = processed.count()
    return report
