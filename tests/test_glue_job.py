import os
import signal
import subprocess
import sys
import types
import unittest

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession


class TestGlueJob(unittest.TestCase):
    """
    This test class setup a test environment to test our glue job,
    runs the glue job and checks the result.
    """

    @classmethod
    def setUpClass(cls):
        """
        the setup class starts a moto server, creates an S3 bucket,
        configures PySpark and Spark and dumps the source dataframe to S3.
        """
        S3_MOCK_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://127.0.0.1:5113")
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "mock")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "mock")

        # setup moto server
        cls.process = subprocess.Popen(
            "moto_server -p 5113",
            stdout=subprocess.PIPE,
            shell=True,
            preexec_fn=os.setsid,
        )

        # create s3 client to moto server
        cls.s3 = boto3.resource(
            "s3", region_name="sa-east-1", endpoint_url=S3_MOCK_ENDPOINT
        )

        # Setup spark to use s3, and point it to the moto server.
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            """--packages "org.apache.hadoop:hadoop-aws:3.3.4" pyspark-shell"""
        )
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.dagGraph.retainedRootRDDs", "1")
            .config("spark.ui.retainedJobs", "1")
            .config("spark.ui.retainedStages", "1")
            .config("spark.ui.retainedTasks", "1")
            .config("spark.sql.ui.retainedExecutions", "1")
            .config("spark.worker.ui.retainedExecutors", "1")
            .config("spark.worker.ui.retainedDrivers", "1")
            .getOrCreate()
        )
        hadoop_conf = cls.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        hadoop_conf.set("fs.s3a.endpoint", S3_MOCK_ENDPOINT)

    def setUp(self) -> None:
        def fake_start_glue(self):
            self._args = getResolvedOptions(
                args=sys.argv, options=["JOB_NAME"] + self._custom_args
            )
            self._sc = TestGlueJob.spark.sparkContext
            self._glue = GlueContext(sparkContext=self._sc)
            self._job = Job(glue_context=self._glue)
            self._job.init(self._args["JOB_NAME"], self._args)

        super().setUp()
        # self.glue_job = GlueJob()
        # self.glue_job._start_glue = types.MethodType(fake_start_glue, self.glue_job)

    def test_glue_job_runs_successfully(self):
        """
        we run our glue job and assert if the result is what we expect.
        """
        # arrange

        # act
        actual = self.glue_job.run()

        # assert
        self.assertTrue(actual is not None)

    @classmethod
    def tearDownClass(cls):
        # shut down moto server
        os.killpg(os.getpgid(cls.process.pid), signal.SIGTERM)


if __name__ == "__main__":
    try:
        unittest.main()
    except Exception:
        TestGlueJob().tearDownClass()
