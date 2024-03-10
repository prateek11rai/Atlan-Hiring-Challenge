from pyatlan.model.assets import S3Bucket
from atlan_search_s3_bucket import search_s3_bucket
from atlan_update_s3_bucket import update_s3_bucket

def create_s3_bucket(client,atlan_s3_bucket_asset_name,s3_connection_asset_qualified_name,atlan_aws_s3_bucket_arn,atlan_owner_user,s3_bucket_aws_region,s3_bucket_asset_object_count):

    # searching for existing bucket asset with same name and arn
    search_s3_bucket_asset_details=search_s3_bucket(client,atlan_s3_bucket_asset_name,atlan_aws_s3_bucket_arn)

    if len(search_s3_bucket_asset_details)==4:
        update_s3_bucket(client,search_s3_bucket_asset_details[0],search_s3_bucket_asset_details[1],atlan_owner_user,s3_bucket_aws_region,s3_bucket_asset_object_count,atlan_aws_s3_bucket_arn)

    else:

        s3bucket = S3Bucket.create(  # 
            name=atlan_s3_bucket_asset_name,  # "atlan-tech-challenge-pr"
            connection_qualified_name=s3_connection_asset_qualified_name  # "default/s3/1709919425"
            # aws_arn=atlan_aws_s3_bucket_arn  # "arn:aws:s3:::atlan-tech-challenge"
        )
        response = client.asset.save(s3bucket)  # 
        s3_bucket_asset_qualified_name = response.assets_created(asset_type=S3Bucket)[0].qualified_name
        s3_bucket_asset_guid = response.assets_created(asset_type=S3Bucket)[0].guid

        search_s3_bucket_asset_details.append(atlan_s3_bucket_asset_name)
        search_s3_bucket_asset_details.append(s3_bucket_asset_qualified_name)
        search_s3_bucket_asset_details.append(s3_bucket_asset_guid)
        search_s3_bucket_asset_details.append(atlan_aws_s3_bucket_arn)

        update_s3_bucket(client,search_s3_bucket_asset_details[0],search_s3_bucket_asset_details[1],atlan_owner_user,s3_bucket_aws_region,s3_bucket_asset_object_count,atlan_aws_s3_bucket_arn)
    
    return search_s3_bucket_asset_details