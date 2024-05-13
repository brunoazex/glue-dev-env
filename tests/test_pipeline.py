import json
import unittest
import unittest.mock
from datetime import datetime

import pipeline
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql import types as T

RAW_FIXTURE = [
    {
        "id": 123456789,
        "code": "abcde12345",
        "group_id": 2,
        "created_at": datetime(2024, 5, 8),
        "description": "The Description",
    },
    # Duplicado
    {
        "id": 123456789,
        "code": "abcde12345",
        "group_id": 2,
        "created_at": datetime(2024, 5, 9),
        "description": "The Description",
    },
    {
        "id": 987654321,
        "code": "abcde12345",
        "group_id": 2,
        "created_at": datetime(2024, 5, 8),
        "description": "One more description",
    },
    # Sem cod unico
    {
        "id": 123454321,
        "code": None,
        "group_id": 2,
        "created_at": datetime(2024, 5, 10),
        "description": "This is a description",
    },
    # Sem dica
    {
        "id": 123456321,
        "code": "abcde12345",
        "group_id": 2,
        "created_at": datetime(2024, 5, 10),
        "description": None,
    },
]

RESPONSE_FIXTURE = [
    {
        "uuid": pipeline.helpers.deterministic_uuid(id),
        "id": id,
    }
    for id in list(set(map(lambda row: row["id"], RAW_FIXTURE)))
    if id in [123456789, 987654321]
]


class TestPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestPipeline, cls).setUpClass()
        cls.spark = SparkSession.builder.getOrCreate()

    @unittest.mock.patch("pipeline.process.requests")
    @unittest.mock.patch("pipeline.load.catalog")
    def test_run(self, catalog_mock, request_mock):
        # Arrange
        glue = unittest.mock.MagicMock()
        glue.sparkSession = TestPipeline.spark
        glue.context = GlueContext(glue.sparkSession)
        catalog_mock.get_partitions.return_value = {
            "s3://dummy/year=2024/month=5/day=10": ["2024", "5", "10"],
            "s3://dummy/year=2024/month=5/day=9": ["2024", "5", "9"],
        }
        glue.create_data_frame_from_catalog.return_value = DynamicFrame.fromDF(
            TestPipeline.spark.createDataFrame(RAW_FIXTURE), glue.context, "raw_data_1"
        )
        glue.create_dynamic_frame.from_options.side_effect = [
            DynamicFrame.fromDF(
                TestPipeline.spark.createDataFrame([], T.StructType([])),
                glue.context,
                "cache_1",
            ),
            DynamicFrame.fromDF(
                TestPipeline.spark.createDataFrame([], T.StructType([])),
                glue.context,
                "delta_1",
            ),
            DynamicFrame.fromDF(
                TestPipeline.spark.createDataFrame(
                    list(
                        map(
                            lambda row: row | {"partition_ref": "2024-05-13_1"},
                            RESPONSE_FIXTURE,
                        )
                    )
                ),
                glue.context,
                "cache_2",
            ),
        ]
        request_mock.post.return_value = unittest.mock.Mock(
            status_code=200, content=json.dumps(RESPONSE_FIXTURE)
        )
        expected = pipeline.report.Report(status="success")
        expected.cleanup = pipeline.report.CleanupSection(
            code_count=2,
            de_duplicated_count=4,
            description_count=3,
            duplicated_count=1,
            no_code_count=1,
            no_description=1,
            raw_count=5,
            status="success",
            valid_rows=2,
        )
        expected.load = pipeline.report.LoaderSection(
            date_ref="2024-05-10",
            predicate="year=2024 and month=05 and day=10",
            row_count=5,
            status="success",
        )
        expected.process = pipeline.report.ProcessSection(
            matched_count=2,
            new_count=2,
            not_matched_count=0,
            raw_count=2,
            repeated_count=0,
            status="success",
            transformed_count=2,
            valid_rows=2,
        )

        # Act
        actual = pipeline.run(glue)

        # Assert
        self.assertEqual(actual, expected)
