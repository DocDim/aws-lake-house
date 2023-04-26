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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="dimdb",
    table_name="step_trainer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1682282518791 = glueContext.create_dynamic_frame.from_catalog(
    database="dimdb",
    table_name="customer_curated",
    transformation_ctx="AmazonS3_node1682282518791",
)

# Script generated for node Join Customer to step trainer
JoinCustomertosteptrainer_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1682282518791,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="JoinCustomertosteptrainer_node2",
)

# Script generated for node Drop Fields
DropFields_node1682282720205 = DropFields.apply(
    frame=JoinCustomertosteptrainer_node2,
    paths=[
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "lastupdatedate",
        "registrationdate",
        "birthday",
        "phone",
        "email",
        "customername",
    ],
    transformation_ctx="DropFields_node1682282720205",
)

# Script generated for node Amazon S3
AmazonS3_node1682282792742 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682282720205,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dim-lake-house/step_trainer/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1682282792742",
)

job.commit()
