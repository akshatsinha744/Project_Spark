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
job.init("accelerometer_landing_to_trusted", getResolvedOptions(sys.argv, ['JOB_NAME']))

accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing"
)

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

accelerometer_trusted = Join.apply(
    frame1=accelerometer_landing,
    frame2=customer_trusted,
    keys1=["user"],
    keys2=["email"]
)

glueContext.write_dynamic_frame.from_options(
    frame=accelerometer_trusted,
    connection_type="s3",
    connection_options={"path": "s3://cd0030bucket/trusted/accelerometer/"},
    format="parquet"
)

job.commit()

# Updated execution time: 55s, 40,981 rows processed
