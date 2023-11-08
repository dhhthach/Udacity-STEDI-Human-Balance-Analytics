import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


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

# Script generated for node Customer Curated
CustomerCurated_node1699339244015 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dhht-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1699339244015",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1699339246263 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dhht-lakehouse/ step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1699339246263",
)

# Script generated for node SQL Query
SqlQuery0 = """
select sensor.serialNumber, sensor.sensorReadingTime, sensor.distanceFromObject from customer
inner join sensor 
on customer.serialNumber = sensor.serialNumber
"""
SQLQuery_node1699381692390 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "sensor": StepTrainerLanding_node1699339246263,
        "customer": CustomerCurated_node1699339244015,
    },
    transformation_ctx="SQLQuery_node1699381692390",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1699339253797 = glueContext.getSink(
    path="s3://dhht-lakehouse/ step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1699339253797",
)
StepTrainerTrusted_node1699339253797.setCatalogInfo(
    catalogDatabase="dhht-lakehouse", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1699339253797.setFormat("json")
StepTrainerTrusted_node1699339253797.writeFrame(SQLQuery_node1699381692390)
job.commit()
