# pistevo_decision
This project ingests three datasets: patient, provider and claim

# Code Organization:
conf : conntains a yaml file holding config properties datset level and project level
schema: folder contain a yaml file dictating the schema of each data set
scripts: holds the python scripts
sql_scripts: holding sql queries

# Scripts:
1. ingest_dataset.py: is the mian pyspark script the ingestion process starts. It is parameterized to run for each data set.
    - parameter:
        - dataset_name: the name of the data set the script is running for values: patients, provider or claim

    - the script defines spark context and loads config properties for the given data set.
    
2. process.py: is responsible for actual data ingestion
3. utils.py: holds many helper function being used in the two previous scripts

# Enviroment set up:
1. AWS EMR:
    - create AWS EMR cluster : use custome option to get bundles containing spark and hive
        - choose the default setting

    - set up SSH key (.pem file) for connecting to the master node

    - follow the ssh steps in EMR cluster board to connect to EMR
        it is something like:
        ssh -i ~/pistevo_decision_ssh.pem hadoop@ec2-3-143-173-147.us-east-2.compute.amazonaws.com

        where ~/pistevo_decision_ssh.pem  is the location of your pem file created in above step

    - connect the master node and install boto3
        pin install boto3
3. S3 bucket: please create S3 bucket with the following structure:
    bucket name: 
        - logs
        - archive
            - patient
            - provider
            - claim
        - source
            - patient
            - provider
            - claim
        - target
4. load the three files in the approparite folder under source directory

5. upload process.py, utils.py , ingest_dataset.py and config.yaml,tables_schema.yaml files to the master node.
        - one can follow any approach but the approach worked for me is:
            - firts upload the files to s3 bucket
            - then copy them by running the following in the master:
                aws s3 cp s3://bucket_name/folder/file_name .

6. launch hive in from the master.
    - you can open a new window and ssh to the master node
    _ then run: hive

7. create the schema/ databse by running the following :
    create database billing;

# Test Run: on EMR:
    - please upload all files mentioned above in work directory
    - Run as:
        spark-submit --py-files process.py,utils.py --files config.yaml,tables_schema.yaml ingest_dataset.py --dataset_name patients 

# In your README.md, add a prioritized list of five or more development tasks you woulddo next to improve your solution to the code challenge

# Prioritized list:
1. Adding functionality to control access to pii/phi columns: as this is patient data, my first priority would be to implement access control.
    - It can be plain masking or apply KMS or ranger policy

2. Adding detailed DQ step: this required working with project owner to define qhat kind of quality checks need to be added
    - The reason I included a staging table in the design is because I wanted to have a DQ step applyied on staging table
    -  Final/ target table would be populated only if all DQ are passed

3. Adding job alert:
    - Job could fail because of exceptions and/or DQ 
    -  Job failure because of file delay and/or infra

    I would add or ocnfigure proper alerting system for each cases.

4. Analyse best storage for final target table:
    - depending on query latency requirement, I would have picked low latency RDMS data bases like mysql over hive tables
    - 
5. Job scheduler and logging:
    - I would use best schedulers like airflow for efficient parallalization , run/re-run and other job ochestartion functionalities. 


