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
job.init("customer_landing_to_trusted", getResolvedOptions(sys.argv, ['JOB_NAME']))

customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing"
)

customer_trusted = Filter.apply(
    frame=customer_landing,
    f=lambda x: x["sharewithresearchasofdate"] is not None
)

glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted,
    connection_type="s3",
    connection_options={"path": "s3://cd0030bucket/trusted/customers/"},
    format="parquet"
)

job.commit()

# Updated execution time: 56s, 482 rows processed
