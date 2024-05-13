import logging
from datetime import datetime, timedelta

import pipeline.config
import pipeline.report
from awswrangler import catalog

report = pipeline.report.LoaderSection()
logger = logging.getLogger("LOADER")


def build_predicate() -> str:
    # Act
    last_work_day = datetime.now().date()
    if last_work_day.weekday() > 4:
        time_delta = 1 if last_work_day.weekday() == 5 else 2
        last_work_day = last_work_day - timedelta(days=time_delta)

    min_date = last_work_day - timedelta(days=2)
    partitions = catalog.get_partitions(
        database=pipeline.config.CATALOG_DB,
        table=pipeline.config.CATALOG_TABLE,
        expression=f"creation_date > {min_date.strftime('%Y-%m-%d')}",
    )
    date_ref = next(
        iter(
            sorted(
                [
                    datetime(*tuple([int(val) for val in row]))
                    for row in partitions.values()
                ],
                reverse=True,
            )
        ),
        last_work_day,
    )

    # Report
    report.date_ref = date_ref.strftime("%Y-%m-%d")
    report.predicate = date_ref.strftime("year=%Y and month=%m and day=%d")

    # Return
    return report.predicate


def run() -> pipeline.report.LoaderSection:
    # Act
    predicate = build_predicate()
    raw_data = pipeline.glue_context.create_data_frame_from_catalog(
        database=pipeline.config.CATALOG_DB,
        table=pipeline.config.CATALOG_TABLE,
        push_down_predicate=predicate,
    ).toDF()
    raw_data.createOrReplaceTempView(pipeline.config.VW_RAW_DATA)

    # Report
    report.row_count = raw_data.count()
    report.status = "success"

    # Return
    return report
