import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("step_trainer_landing_to_trusted", getResolvedOptions(sys.argv, ['JOB_NAME']))

step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing"
)

customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated"
)

step_trainer_trusted = Join.apply(
    frame1=step_trainer_landing,
    frame2=customer_curated,
    keys1=["serialNumber"],
    keys2=["serialnumber"]
)

glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_trusted,
    connection_type="s3",
    connection_options={"path": "s3://cd0030bucket/trusted/step_trainer/"},
    format="parquet"
)

job.commit()

# Updated execution time: 7m 6s, 28,680 rows processed
