import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1699339244015 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dhht-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1699339244015",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699339246263 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dhht-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1699339246263",
)

# Script generated for node SQL Query
SqlQuery0 = """
select distinct email, * from cstTrusted 
inner join accTrusted
on cstTrusted.email = accTrusted.user
"""
SQLQuery_node1699376660338 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accTrusted": AccelerometerTrusted_node1699339244015,
        "cstTrusted": CustomerTrusted_node1699339246263,
    },
    transformation_ctx="SQLQuery_node1699376660338",
)

# Script generated for node Drop Fields
DropFields_node1699339402121 = DropFields.apply(
    frame=SQLQuery_node1699376660338,
    paths=[],
    transformation_ctx="DropFields_node1699339402121",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1699378610176 = DynamicFrame.fromDF(
    DropFields_node1699339402121.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1699378610176",
)

# Script generated for node Customer Curated
CustomerCurated_node1699339253797 = glueContext.getSink(
    path="s3://dhht-lakehouse/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1699339253797",
)
CustomerCurated_node1699339253797.setCatalogInfo(
    catalogDatabase="dhht-lakehouse", catalogTableName="customer_curated"
)
CustomerCurated_node1699339253797.setFormat("json")
CustomerCurated_node1699339253797.writeFrame(DropDuplicates_node1699378610176)
job.commit()
