import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession


# Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

spark = SparkSession.builder. \
    config("spark.sql.autoBroadcastJoinThreshold", "-1"). \
    config("spark.driver.maxResultSize", "10g"). \
    config("fs.s3.maxRetries" "20"). \
    config("spark.driver.memory", "20g"). \
    getOrCreate()

job = Job(glue_context)


args = getResolvedOptions(sys.argv, ['JOB_NAME','INPUT_DATENUM', 'OUTPUT_DATENUM'])
s3_acct_read_path = "s3://com-fngn-prod-datalake-raw/irr/" + "datenum=" + args['INPUT_DATENUM'] + "/acctPerformance*"
s3_client_read_path = "s3://com-fngn-prod-datalake-raw/irr/" + "datenum=" + args['INPUT_DATENUM'] + "/clientPerformance*"


job.init(args['JOB_NAME'], args)

# Parameters
s3_write_acct_path = "s3://com-fngn-prod-dataeng/raw/" + args['OUTPUT_DATENUM'] + "/irr/acct/"
s3_write_client_path = "s3://com-fngn-prod-dataeng/raw/" + args['OUTPUT_DATENUM'] + "/irr/client/"

# Log starting time
dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_start)

inputAcctDF = spark.read.json(s3_acct_read_path)
inputClientDF = spark.read.json(s3_client_read_path)

print("After inputDF")

transformedAcctDF = inputAcctDF.select("account_id",
                                    col("client_id").alias("customer_id"),
                                    col("performanceStartBalance.amount").alias("perf_start_balance"),
                                    col("performanceEndBalance.amount").alias("perf_end_balance"),
                                    col("netContributions.amount").alias("net_contributions"),
                                    col("fees.amount").alias("fees"),
                                    col("investmentGainLoss.amount").alias("investment_gain_or_loss"),
                                    col("periodStart").alias("period_start"),
                                    col("periodEnd").alias("period_end"),
                                    col("startDate").alias("start_date"),
                                    col("twrrNof").alias("twrr_nof"),
                                    col("twrrGof").alias("twrr_gof"),
                                    "irr",
                                    "periodLabel",
                                    "start_date_num",
                                    "end_date_num"
                                    )

transformedClientDF = inputClientDF.select(
    col("client_id").alias("customer_id"),
    col("performanceStartBalance.amount").alias("perf_start_balance"),
    col("performanceEndBalance.amount").alias("perf_end_balance"),
    col("netContributions.amount").alias("net_contributions"),
    col("fees.amount").alias("fees"),
    col("investmentGainLoss.amount").alias("investment_gain_or_loss"),
    col("periodStart").alias("period_start"),
    col("periodEnd").alias("period_end"),
    col("startDate").alias("start_date"),
    col("twrrNof").alias("twrr_nof"),
    col("twrrGof").alias("twrr_gof"),
    "irr",
    "periodLabel",
    "start_date_num",
    "end_date_num"
)

print("After transformedDF ")

# Create just 1 partition, because there is so little data
outputAcctDF = transformedAcctDF.repartition(1)
outputClientDF = transformedClientDF.repartition(1)

print("Started writing the file in Output folder")

# Convert back to dynamic frame
dynamic_frame_write_acct = DynamicFrame.fromDF(outputAcctDF, glue_context, "dynamic_frame_write")
dynamic_frame_write_client = DynamicFrame.fromDF(outputClientDF, glue_context, "dynamic_frame_write")

print("After dynamic_frame_write ")

# Write data back to S3
glue_context.write_dynamic_frame.from_options(frame=dynamic_frame_write_acct,
                                              connection_type="s3",
                                              connection_options={"path": s3_write_acct_path},
                                              format="csv")

glue_context.write_dynamic_frame.from_options(frame=dynamic_frame_write_client,
                                              connection_type="s3",
                                              connection_options={"path": s3_write_client_path},
                                              format="csv")

job.commit()

print("Job Completed Successfully")

# Log end time
dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("End time:", dt_end)
