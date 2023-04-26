import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 accelerometer_trusted
S3accelerometer_trusted_node1682526501275 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="dimdb",
        table_name="accelerometer_trusted",
        transformation_ctx="S3accelerometer_trusted_node1682526501275",
    )
)

# Script generated for node S3 step_trainer_trusted
S3step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="dimdb",
    table_name="step_trainer_trusted",
    transformation_ctx="S3step_trainer_trusted_node1",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=S3step_trainer_trusted_node1,
    frame2=S3accelerometer_trusted_node1682526501275,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node2",
)

# Script generated for node Aggregate
Aggregate_node1682526771311 = sparkAggregate(
    glueContext,
    parentFrame=Join_node2,
    groups=[" serialnumber"],
    aggs=[["timestamp", "countDistinct"], ["user", "countDistinct"]],
    transformation_ctx="Aggregate_node1682526771311",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Aggregate_node1682526771311,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dim-lake-house/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
