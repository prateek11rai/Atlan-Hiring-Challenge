from pyatlan.model.assets import S3Object
from atlan_search_s3_object import search_s3_object
from atlan_update_s3_object import update_s3_object


def create_s3_object(client,s3_object_asset_key, s3_connection_asset_qualified_name, s3_object_asset_arn, s3_bucket_asset_qualified_name,atlan_owner_user,s3_object_asset_aws_region,s3_object_asset_size,s3_object_asset_storage_class,s3_object_asset_content_type,s3_object_asset_last_modified_at,s3_object_asset_version_id,s3_object_asset_content_disposition):

    # searching for existing object with same asset name and arn 
    search_s3_object_asset_details=search_s3_object(client,s3_object_asset_key,s3_object_asset_arn)

    if len(search_s3_object_asset_details)==4:
        update_s3_object(client,search_s3_object_asset_details[1],search_s3_object_asset_details[0],atlan_owner_user,s3_object_asset_aws_region,s3_object_asset_key,s3_object_asset_size,s3_object_asset_storage_class,s3_object_asset_content_type,s3_object_asset_last_modified_at,s3_object_asset_version_id,s3_object_asset_content_disposition,s3_object_asset_arn)

    else:
        s3object = S3Object.create(  #  # 
            name=s3_object_asset_key,  # "EMPLOYEE.csv"
            connection_qualified_name=s3_connection_asset_qualified_name,  # "default/s3/1709919425"
            aws_arn=s3_object_asset_arn,  # "arn:aws:s3:::atlan-tech-challenge-test/EMPLOYEE.csv"
            s3_bucket_qualified_name=s3_bucket_asset_qualified_name  # "default/s3/1709919425/arn:aws:s3:::atlan-tech-challenge-test"
        )
        response = client.asset.save(s3object)
        s3_object_asset_qualified_name = response.assets_created(asset_type=S3Object)[0].qualified_name
        s3_object_asset_guid = response.assets_created(asset_type=S3Object)[0].guid

        search_s3_object_asset_details.append(s3_object_asset_key)
        search_s3_object_asset_details.append(s3_object_asset_qualified_name)
        search_s3_object_asset_details.append(s3_object_asset_guid)
        search_s3_object_asset_details.append(s3_object_asset_arn)

        # update for data that can not be passed in creation
        update_s3_object(client,search_s3_object_asset_details[1],search_s3_object_asset_details[0],atlan_owner_user,s3_object_asset_aws_region,s3_object_asset_key,s3_object_asset_size,s3_object_asset_storage_class,s3_object_asset_content_type,s3_object_asset_last_modified_at,s3_object_asset_version_id,s3_object_asset_content_disposition,s3_object_asset_arn) 
    
    return search_s3_object_asset_details