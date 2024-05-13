import logging

import pipeline.config
import pipeline.report
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from toolz.functoolz import pipe

report = pipeline.report.CleanupSection()
logger = logging.getLogger("CLEANUP")


def drop_duplicates(raw_data: DataFrame) -> DataFrame:
    # Act
    wnd = Window.orderBy(F.col("created_at").desc()).partitionBy("id", "code")
    calculated = raw_data.withColumn("row_num", F.row_number().over(wnd))
    de_duplicated = calculated.filter(F.col("row_num") == 1)

    # Report
    report.duplicated_count = calculated.filter(F.col("row_num") > 1).count()
    report.de_duplicated_count = de_duplicated.count()

    # Return
    return de_duplicated


def ensure_description(raw_data: DataFrame) -> DataFrame:
    # Act
    actual_count = raw_data.count()
    cleaned = raw_data.filter(F.col("description").isNotNull())
    cleaned_count = cleaned.count()

    # Report
    report.no_description = actual_count - cleaned_count
    report.description_count = cleaned_count

    # Return
    return cleaned


def ensure_code(raw_data: DataFrame) -> DataFrame:
    # Act
    actual_count = raw_data.count()
    cleaned = raw_data.filter(F.col("code").isNotNull())
    cleaned_count = cleaned.count()

    # Report
    report.no_code_count = actual_count - cleaned_count
    report.code_count = cleaned_count

    # Return
    return cleaned


def run() -> pipeline.report.CleanupSection:
    # Load
    spark = pipeline.glue_context.sparkSession
    raw_data = spark.sql(f"SELECT * FROM {pipeline.config.VW_RAW_DATA}")
    report.raw_count = raw_data.count()

    # Act
    cleaned = pipe(raw_data, drop_duplicates, ensure_description, ensure_code)
    cleaned.createOrReplaceTempView(pipeline.config.VW_CLEANUP)

    # Report
    report.valid_rows = cleaned.count()
    report.status = "success"
    return report
