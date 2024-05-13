import logging

import pipeline
import pipeline.config
import pipeline.report
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from toolz.functoolz import pipe

from .schema import DELTA_SCHEMA

logger = logging.getLogger("PERSIST")
report = pipeline.report.PersistSection()


def persist(raw_data: DataFrame) -> DataFrame:
    report.valid_rows = raw_data.count()

    # TODO Select output fields and save it external
    persisted = raw_data.select("*")

    # Update delta
    current_delta = pipeline.glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [pipeline.config.DELTA_S3_PATH]},
        format="parquet",
    ).toDF()

    if current_delta.isEmpty():
        current_delta = pipeline.glue_context.sparkSession.createDataFrame(
            [], DELTA_SCHEMA
        )

    new_delta = raw_data.select("id", "code", "sha2")
    updated_delta = (
        current_delta.alias("current")
        .join(new_delta.alias("new"), "id", "outer")
        .withColumn("final_id", F.coalesce(F.col("new.id"), F.col("current.id")))
        .withColumn("final_code", F.coalesce(F.col("new.code"), F.col("current.code")))
        .withColumn("final_sha2", F.coalesce(F.col("new.sha2"), F.col("current.sha2")))
        .select(
            F.col("final_id").alias("id"),
            F.col("final_code").alias("code"),
            F.col("final_sha2").alias("sha2"),
        )
    )
    updated_delta.write.parquet(pipeline.config.DELTA_S3_PATH, "overwrite")
    return persisted


def run() -> pipeline.report.PersistSection:
    raw_data = pipeline.glue_context.sparkSession.sql(
        f"SELECT * FROM {pipeline.config.VW_PROCESS}"
    )
    report.raw_count = raw_data.count()
    persisted = pipe(raw_data, persist)
    persisted.createOrReplaceTempView(pipeline.config.VW_PERSISTED)

    report.status = "success"
    return report
