from pyspark.sql import functions as F, SparkSession
from utils import create_table_ddl, format_date, apply_masking, get_column_valid_vals
from pyspark.sql.types import  StringType, DoubleType, IntegerType

def process(spark, source_path, target_path, target_schema, table_name, table_schema, merge_key, cdc_key, timestmp, file_name):
    """
    The main function that does data ingestion
    parameters:
        spark: spark context
        source_path: the pasth to the loaction of source file
        target_path: the pasth to the destination location
        target_schema: the data based name of the target table
        table_name: the target table name
        merge_key: the key used for delta loading
        timestmp: the time stamp of execution
        s3_bucket: the s3 bucket
        source_prefix: the folder for source files 
    ACTION:
        -create stage table and target table
        -applies data validation on delta data
        -loads both table
    """
    #create stage and target table
    stage_ddl, target_ddl, mask = create_table_ddl(target_schema,table_name, table_schema, target_path)
    spark.sql(stage_ddl)
    spark.sql(target_ddl)
    #load source data
    df_old = spark.sql(f"select * from {target_schema}.{table_name}" )
    schema= df_old.schema
    df_new = (
        spark.read
        .option("delimiter", ',') 
        .option('header','True')
        .csv(source_path)
        )
    # apply masking
    df_new = apply_masking(spark,df_new, mask)
    #write the new data into new partition
    df_stage = (
                df_new
                .withColumn('filename',F.lit(file_name))
                .withColumn('ts',F.lit(timestmp))
            )
    
    (
        df_stage
        .write
        .mode('overwrite')
        .insertInto(f'{target_schema}.{table_name}_stage')
    )
    #==========delta load to the target table===========
    #1.apply date formatting
    format_date_ = F.udf(format_date,StringType())
    for col, typ in df_old.dtypes:
        if typ == 'date':
            df_new = (
                df_new
                .withColumn(col, F.to_date(format_date_(F.col(col))))
                )
        elif typ =='double':
            df_new = (
                df_new
                .withColumn(col, F.col(col).cast(DoubleType()))
                )
        elif typ =='int':
            df_new = (
                df_new
                .withColumn(col, F.col(col).cast(IntegerType()))
                )
    #2 enforce schema
    df_new = spark.createDataFrame(df_new.rdd, schema)
    #3 find delta
    df_old.createOrReplaceTempView('df_old')
    df_new.createOrReplaceTempView('df_new')
    delta_sql = f"""
                    SELECT o.* 
                    FROM df_old o
                    LEFT JOIN df_new n
                    ON o.{merge_key} = n.{merge_key}
                    WHERE 
                        n.{merge_key} IS NULL
                    UNION ALL
                    select * from df_new
                """
    delta_df = spark.sql(delta_sql)
    print(delta_sql)

    #4. implement validations
    """
    a. if you find duplicate records, keep the latest one
    b. check column values are in allowed value list
    """
    #a.
    delta_df.createOrReplaceTempView('delta_df')
    delta_dedup_sql = f"""
                    WITH rank_cte as (
                    SELECT * , ROW_NUMBER() OVER(PARTITION BY {merge_key} ORDER BY {cdc_key} DESC ) rn
                    FROM delta_df
                    )
                    SELECT * FROM rank_cte
                    WHERE rn =1 
                """
    if cdc_key:
        delta_dedup = (
                spark
                .sql(delta_dedup_sql)
                .drop('rn')
                )
    else:
        delta_dedup = delta_df
    #b.
    col_list = get_column_valid_vals(spark, table_schema).value
    for col in col_list:
        vals = col_list[col]
        delta_dedup=(
            delta_dedup.withColumn(col,F.when(F.lower(F.col(col)).isin(vals),F.col(col)))
        )
    #5. write to target
    (
        delta_dedup
        .write
        .mode('overwrite')
        .insertInto(f'{target_schema}.{table_name}')
    )
