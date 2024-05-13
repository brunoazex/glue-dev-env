import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import pipeline
import json
from dataclasses import asdict


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
glue_context = GlueContext(sparkContext=sc)
job = Job(glue_context=glue_context)
glue_args = getResolvedOptions(args=sys.argv, options=["JOB_NAME"])
job.init(glue_args["JOB_NAME"], glue_args)

report = pipeline.run(glue_context)
print(json.dumps(asdict(report), indent=4))
