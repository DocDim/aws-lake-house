import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1682276011556 = glueContext.create_dynamic_frame.from_catalog(
    database="dimdb",
    table_name="accelerometer_landing",
    transformation_ctx="AmazonS3_node1682276011556",
)

# Script generated for node Amazon S3
AmazonS3_node1682276081568 = glueContext.create_dynamic_frame.from_catalog(
    database="dimdb",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1682276081568",
)

# Script generated for node PrivacyJoin
PrivacyJoin_node2 = Join.apply(
    frame1=AmazonS3_node1682276011556,
    frame2=AmazonS3_node1682276081568,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyJoin_node2",
)

# Script generated for node Drop Fields
DropFields_node1682276289005 = DropFields.apply(
    frame=PrivacyJoin_node2,
    paths=[
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "lastupdatedate",
        "registrationdate",
        "serialnumber",
        "birthday",
        "phone",
        "email",
        "customername",
    ],
    transformation_ctx="DropFields_node1682276289005",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682276289005,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dim-lake-house/accelerometer/accelerometer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
