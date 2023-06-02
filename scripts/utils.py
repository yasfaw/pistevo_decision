import yaml

def parse_schema_yaml():
    with open("../ddl/tables_schema.yaml", 'r') as f:
        tables_schema = yaml.safe_load(f)['tables']
    return tables_schema

def create_table_ddl(target_schema,table_name, table_schema, path):
    columns = table_schema['columns']
    mask = table_schema['mask']
    target_col_schema =''
    stage_col_schema =''
    for col in columns:
        if col in mask:
            target_col_schema += f"{col} string comment '{columns[col]['desc']}', "
        else:
            target_col_schema += f"{col} {columns[col]['type']} comment '{columns[col]['desc']}', "
        stage_col_schema += f"{col} string comment '{columns[col]['desc']}', "
    target_col_schema = target_col_schema[:-2] 
    stage_col_schema = stage_col_schema[:-2] 
    stage_path = f"{path}/{table_name}_stage"
    target_path = f"{path}/{table_name}"
    stage_ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS {target_schema}.{table_name}_stage ( {stage_col_schema} ) PARTITIONED BY ( file_name STRING, ts STRING) LOCATION '{stage_path}'"
    target_ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS {target_schema}.{table_name} ( {target_col_schema} ) LOCATION '{target_path}'"
    return stage_ddl, target_ddl , mask

def get_config_values():
    with open("../conf/config.yaml", 'r') as f:
        configs = yaml.safe_load(f)
    return configs
 
def format_date(date_col):
    if  not date_col:
        return None
    date_str = date_col.replace('/','-').split("-")
    if len(date_str) <3:
        return None
    date_str =[s.rjust(2,'0') for s in [date_str[2],date_str[0],date_str[1]]]
    return '-'.join(date_str)

def s3_util(s3_bucket,boto3, source_prefix, ts, target_prefix=None, getfilename=None, archivefile=None):
    """
    Utility to perform file system operation on S3 bucket
    - Helps to obtain file name
    - copies processed file to archive location 
    -remove original files from source location
    """
    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket= s3_bucket, Prefix = source_prefix)
    source_key = response["Contents"][0]["Key"]
    file_name= response["Contents"][1]["Key"].split('/')[-1]+f'_{ts}'
    if getfilename:
        return file_name
    if archivefile:
        copy_source = {'Bucket': s3_bucket, 'Key': source_key}
        client.copy_object(Bucket = s3_bucket, CopySource = copy_source, Key = target_prefix + file_name)
        client.delete_object(Bucket = s3_bucket, Key = source_key)
    return

def apply_masking(spark,df, mask):
    select_cols = ','.join([f'CAST(NULL as STRING) as {col}' if col in mask else col for col in df.columns])
    df.createOrReplaceTempView('df_mask')
    df_new = spark.sql(f'select {select_cols} from df_mask')
    return df_new

def get_column_valid_vals(spark,table_schema):
    columns = table_schema['columns']
    col_list ={}
    for col in columns:
        if 'valid_vals' in columns[col]:
            vals = columns[col]['valid_vals']
            if columns[col]['type'] =='int':
                vals = [int(x) for x in vals]
            col_list[col] = vals
    return spark.sparkContext.broadcast(col_list)

