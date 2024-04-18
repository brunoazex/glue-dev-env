import sys
from typing import Any
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


class GlueJob:
    def __init__(self, custom_args=[]):
        self._custom_args = custom_args

    def _start_glue(self):
        self._args = getResolvedOptions(
            args=sys.argv, options=["JOB_NAME"] + self._custom_args
        )
        self._sc = SparkContext.getOrCreate()
        self._glue = GlueContext(sparkContext=self._sc)
        self._job = Job(glue_context=self._glue)
        self._job.init(self._args["JOB_NAME"], self._args)

    def run(self) -> Any:
        self._start_glue()
        print("It Works!")
        self._job.commit()
        return True


if __name__ == "__main__":
    glue_job = GlueJob()
    glue_job.run()
