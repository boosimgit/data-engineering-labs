from pyspark.sql import SparkSession, functions as F
import argparse
import time
from datetime import datetime

#import os
#print("---",os.getcwd())
#INPUT_PATH = "sales.csv"

def create_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def parse_args():
    parser = argparse.ArgumentParser(description="Customer daily Sales ETL Job")
    parser.add_argument("--input",required=True, help="Input CSV Path")
    parser.add_argument("--output", required=True, help="Output CSV Path")
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append", "errorifexists", "ignore"],
        help="Write mode"
    )
    return parser.parse_args()

def log_metric(name: str, value):
    print(f"[METRIC] {name}={value}", flush=True)

def timed(label: str):
    """Simple timer helper. Usage: t = timed('read')"""
    start = time.time()

    def done():
        log_metric(f"{label}_seconds",f"{time.time() - start:.2f}")
    
    return done

def main():
    args = parse_args()
    input_path = args.input
    output_path = args.output
    write_mode = args.mode
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[RUN] run_id={run_id}")
    print(f"[RUN] input_path={input_path}")
    print(f"[RUN] output_path={output_path}")

    spark = create_spark("SalesETLJob")
    # we can write code here
    print(f"[INFO] Reading input: {input_path}")
    t_read = timed("read")

    df = (
        spark.read
        .option("header","true")
        .option("inferSchema","true")
        .csv(input_path)
    )

    row_cnt = df.count()
    log_metric("input_rows = ",row_cnt)
    t_read()
# Validatons
    t_validate = timed("validate")
# Validation 1: Make sure File is not empty
    if row_cnt == 0:
        raise ValueError("Input file is empty, Nothing to process")

# validation 2: Make sure all columns present in the file
    expected_cols = {'sale_id','customer_id','product_id','sale_date','amount'}
    actual_cols = set(df.columns)
    missing_cols = expected_cols - actual_cols
    if missing_cols:
        raise ValueError(f"Missing Required Columns {sorted(missing_cols)}")

# validation 3: Ensure mandaroty fileds has data ( We are raising error and failiing here)
    non_nullable_cols = ['sale_id','customer_id','sale_date','amount']
    for col in non_nullable_cols:
        null_cnt = df.filter(f"{col} is NULL").count()
        if null_cnt > 0:
            raise ValueError(f"Column {col} has NULL values")
    t_validate()
# Transformations
    t_transformfilter = timed("transform+filter")
# Transformation 1: Covert dates from CSV strings to Dates

    df = df.withColumn(
        "sale_date",
        F.to_date("sale_date")
    )

# Separate bad data
    bad_data_condition_filter = (
        F.col("sale_id").isNull()|
        F.col("customer_id").isNull()|
        F.col("product_id").isNull()|
        F.col("sale_date").isNull()|
        F.col("amount").isNull()|
        (F.col("amount") <= 0)
    )
    df_bad = df.filter(bad_data_condition_filter)
    df_good = df.filter(~bad_data_condition_filter)

    log_metric("bad rows counts is ",df_bad.count())
    log_metric("good data count is ",df_good.count())

    # write bad data to a file
    badfile_path = f"{output_path}_bad"
    print(f"writing bad data into {badfile_path}")
    t_writebad = timed("wrute bad data")
    df_bad.write.mode(write_mode).partitionBy("sale_date").parquet(badfile_path)
    t_writebad()

    #Data quality summary on bad data
    bad_sale_id_cnt = df.filter(F.col("sale_id").isNull()).count()
    bad_customer_id_cnt = df.filter(F.col("customer_id").isNull()).count()
    bad_product_id_cnt = df.filter(F.col("product_id").isNull()).count()
    bad_sale_date_cnt = df.filter(F.col("sale_date").isNull()).count()
    bad_amount_cnt = df.filter(F.col("amount") <= 0).count()
    
    dq_summary = {
        "null_sale_id":bad_sale_id_cnt,
        "null_customer_id":bad_customer_id_cnt,
        "null_product_id":bad_product_id_cnt,
        "null_sale_date_cnt":bad_sale_date_cnt,
        "null_bad_amount_cnt":bad_amount_cnt
    }

    for i,j in dq_summary.items():
        log_metric(i, j)

# Business logic
# Filter 1 : Ignore Amount is 0 or -ve
# Aggregate: Give me total sales per customer per day

    #df = df_good.filter("amount > 0")

    df_agg = (
        df_good
        .groupBy("customer_id","sale_date")
        .agg(
            F.sum("amount").alias("total_sales"),
            F.count("*").alias("txt_count")
        )
    )

    df_final = df_agg.orderBy("customer_id","sale_date")
    log_metric("final rows",df_final.count())
    t_transformfilter()
# Final step: Store into a durable file    
    #ßOUTPUT_PATH = "out/customer_sales_daily"
    #df_final.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"[INFO] Writing paritioned parquet to {output_path}")
    t_writeout = timed("writetooutfile")
    df_final.write.mode(write_mode).partitionBy("sale_date").parquet(output_path)
    t_writeout()
    print("[INFO] Write is complete")
   
    # df.printSchema()
    # df.show(5)
    # df_agg.show()
    # #df_agg.show(truncate=False) -- If a column is bigger and you want to see entire of it.
    # df_final.show()
    df_check = spark.read.parquet(output_path)
    df_check.filter(F.col("sale_date") == "2025-01-10").show()
    df_check.printSchema()
    df_check.show(5)

    spark.stop()

if __name__ == "__main__":
    main()