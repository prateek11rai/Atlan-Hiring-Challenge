# Atlan-Hiring-Challenge
Create S3 Assets and Lineage for these assets using Atlan python SDK

## To install dependencies :
```shell
pip3 install -r requirements.txt
```

## To Run:
```shell
python3 main.py
```

### Remember:
Add the env variables in a .env file before running.
The file can look something like this : 

```.env
base_url=https://tech-challenge.atlan.com
api_key=<atlan_api_key>
# boto3 requirements
bucket_name=atlan-tech-challenge
region_name=us-east-2
# asset creation
atlan_owner_user=prateek11rai
atlan_s3_connection_asset_name=aws-s3-connection-test-pr
atlan_s3_bucket_asset_name=atlan-tech-challenge-test
atlan_aws_s3_bucket_arn=arn:aws:s3:::atlan-tech-challenge-test
# lineage - creation
atlan_postgres_connection_qualified_name=default/postgres/1709731987
atlan_snowflake_connection_qualified_name=default/snowflake/1709731839
```

#### Generate your own google-service-account secret-key.json for sheet reading capabilities: 
```link
https://developers.google.com/sheets/api/reference/rest?apix=true
```
