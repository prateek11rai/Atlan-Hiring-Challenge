# import all
import os
from dotenv import load_dotenv
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Process, Table, S3Object
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# pyhton function imports
from atlan_create_s3_connection import create_s3_connection
from atlan_create_s3_bucket import create_s3_bucket
from atlan_create_s3_object import create_s3_object
from atlan_search_table_guid import search_table_guid
from atlan_search_s3_object_guid import search_s3_object_guid
from atlan_search_process_exists import search_process_exist



# load creds
load_dotenv('.env')

# boto3 creds from loaded file
bucket_name = os.getenv('bucket_name')
region_name = os.getenv('region_name')

# Create an S3 client with anonymous credentials
s3_client = boto3.client('s3', region_name=region_name, config=Config(signature_version=UNSIGNED))

# List objects in the bucket
response = s3_client.list_objects_v2(Bucket=bucket_name)

# lists for object details
s3_object_keys=[]
s3_object_sizes=[]
s3_object_content_types=[]
s3_object_storage_classes=[]
s3_object_last_modified_ats=[]
s3_object_version_ids=[]
s3_object_content_dispositions=[]

# get Object List and details
for obj in response.get('Contents', []):

    s3_object_keys.append(obj.get('Key'))

    s3_object_sizes.append(obj.get('Size'))

    s3_object_content_types.append(obj.get('ContentType'))  

    s3_object_storage_classes.append(obj.get('StorageClass'))

    s3_object_last_modified_ats.append(obj.get('LastModified'))

    s3_object_version_ids.append(obj.get('VersionId'))
    # Get object metadata including Content-Disposition
    metadata_response = s3_client.head_object(Bucket=bucket_name, Key=obj.get('Key'))
    s3_object_content_dispositions.append(metadata_response.get('ContentDisposition'))

# Get the count of objects in the bucket
s3_object_count=len(s3_object_keys)





# atlan client set up
client = AtlanClient(
    base_url=os.getenv('base_url'),
    api_key=os.getenv('api_key')
)

# Start Connectoin Asset Creation Process - searches for present connection, updates if present or creates ad updates if not
print("Starting S3 Connection Asset Creation....")
s3_connection_asset_details=create_s3_connection(client,os.getenv('atlan_s3_connection_asset_name'),os.getenv('atlan_owner_user'))
print("S3 Connection Asset Created...")


# Start Bucket Asset Creation Process - search for present bucket asset with same bucket-arn, if present update or else create a new one and update some parmeters
print(f"Starting S3 Bucket Asset Creation inside the S3 Connection : {s3_connection_asset_details[0]} ...")
s3_bucket_asset_details=create_s3_bucket(client,os.getenv('atlan_s3_bucket_asset_name'),s3_connection_asset_details[1],os.getenv('atlan_aws_s3_bucket_arn'),os.getenv('atlan_owner_user'),os.getenv('region_name'),s3_object_count)
print(f"S3 Bucket asset created...")


# Start Creating S3 Objects inside the Bucket Asset - search for the s3 object by its name and arn, if exists then update or else create
print(f"Creating S3 Object Assets that exist in the bucket : {bucket_name} ...")

# iterate over all the bucket keys and other details
All_s3_object_asset_details=[]
for i in range(0,len(s3_object_keys)):
    print(f"Trying to create the object {i+1}")
    s3_object_asset_arn=s3_bucket_asset_details[3]+"/"+s3_object_keys[i]
    s3_object_asset_details=create_s3_object(client,s3_object_keys[i],s3_connection_asset_details[1],s3_object_asset_arn,s3_bucket_asset_details[1],os.getenv('atlan_owner_user'),region_name,s3_object_sizes[i],s3_object_storage_classes[i],s3_object_content_types[i],s3_object_last_modified_ats[i],s3_object_version_ids[i],s3_object_content_dispositions[i])
    print(f"Object {i+1} created")

print("All S3 Objects created...")



# starting with the lineage creation for know source and sink connection assets
# read the gsheet to get the table names in all three connections
print("Started creating lineage between the 3 connection assets")

scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

creds=ServiceAccountCredentials.from_json_keyfile_name("secret-key.json",scopes)

file=gspread.authorize(creds)

workbook=file.open("Atlan Tech Challenge")

sheet=workbook.sheet1

import pandas as pd
data = sheet.get_all_values()

# Convert the data to a DataFrame, skipping the header row
df = pd.DataFrame(data[1:], columns=data[0])

# Get the DataFrame
postgres_tables=df['postgres table'].tolist()
s3_objects=df['s3'].tolist()
snowflake_tables=df['snowflake'].tolist()


# start creating processes by iterating over the table and object names
for i in range(0,len(postgres_tables)):
    
    # creating postgres to s3 object process using the s3 connection

    # before creating check if the process already exists or not
    if search_process_exist(client,f"{postgres_tables[i]} (postgresql) -> {s3_objects[i]} (S3)",s3_connection_asset_details[1])==False:
        process = Process.create( #  
            name=f"{postgres_tables[i]} (postgresql) -> {s3_objects[i]} (S3)", # 
            connection_qualified_name=s3_connection_asset_details[1], # s3 qualified name 
            inputs=[ # 
                Table.ref_by_guid(guid=search_table_guid(client,os.getenv('atlan_postgres_connection_qualified_name'),postgres_tables[i]))
            ],
            outputs=[ # 
                S3Object.ref_by_guid(guid=search_s3_object_guid(client,s3_connection_asset_details[1],s3_objects[i]))
            ],
        ) # 
        process.sql = f"SELECT * FROM {postgres_tables[i]};" # 
        response = client.asset.save(process) # 

    # creating s3 object to snowflake process using the s3 connection
    
    # before creating check if the process already exists or not\
    if search_process_exist(client,f"{s3_objects[i]} (S3) -> {snowflake_tables[i]} (snowflake)",s3_connection_asset_details[1])==False:
        process = Process.create( #  
            name=f"{s3_objects[i]} (S3) -> {snowflake_tables[i]} (snowflake)", # 
            connection_qualified_name=s3_connection_asset_details[1], # 
            inputs=[ # 
                S3Object.ref_by_guid(guid=search_s3_object_guid(client,s3_connection_asset_details[1],s3_objects[i]))
            ],
            outputs=[ # 
                Table.ref_by_guid(guid=search_table_guid(client,os.getenv('atlan_snowflake_connection_qualified_name'),snowflake_tables[i]))
            ],
        ) # 
        process.sql = f"SELECT * FROM {s3_objects[i]};" # 
        response = client.asset.save(process) # 

    print(f"Process created for {s3_objects[i]}")


print("Process created for all s3 assets and lineage present in the gsheet...")