import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="dimdb", table_name="customer_trusted", transformation_ctx="S3bucket_node1"
)

# Script generated for node Amazon S3
AmazonS3_node1682279946453 = glueContext.create_dynamic_frame.from_catalog(
    database="dimdb",
    table_name="accelerometer_landing",
    transformation_ctx="AmazonS3_node1682279946453",
)

# Script generated for node Privacy Join
PrivacyJoin_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1682279946453,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="PrivacyJoin_node2",
)

# Script generated for node Filter
Filter_node1682280041092 = Filter.apply(
    frame=PrivacyJoin_node2,
    f=lambda row: (not (row["timestamp"] == 0)),
    transformation_ctx="Filter_node1682280041092",
)

# Script generated for node Drop Fields
DropFields_node1682280695132 = DropFields.apply(
    frame=Filter_node1682280041092,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1682280695132",
)

# Script generated for node Customer Curated
CustomerCurated_node1682280445708 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682280695132,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dim-lake-house/customers/customer_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1682280445708",
)

job.commit()
