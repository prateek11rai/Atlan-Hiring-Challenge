from pyatlan.model.assets import S3Bucket


def update_s3_bucket(client,atlan_s3_bucket_asset_name,s3_bucket_asset_qualified_name,atlan_owner_user,s3_bucket_aws_region,s3_bucket_asset_object_count,s3_bucket_asset_aws_arn):

    term = S3Bucket.updater(
        qualified_name=s3_bucket_asset_qualified_name, # "default/s3/1709919425/arn:aws:s3:::atlan-tech-challenge-test"
        name=atlan_s3_bucket_asset_name, # "atlan-tech-challenge-test"
    )
    term.owner_users = [atlan_owner_user]
    term.aws_region = s3_bucket_aws_region
    term.name=atlan_s3_bucket_asset_name
    term.s3_object_count=s3_bucket_asset_object_count
    term.aws_arn=s3_bucket_asset_aws_arn

    try:
        response = client.asset.update_merging_cm(term)  # 
        if updated := response.assets_updated(asset_type=S3Bucket):  # 
            term = updated[0]
            print(f"Updated the S3 bucket Asset called {atlan_s3_bucket_asset_name}.")
    except:  # 
        print("No existing asset to update, so nothing changed or created.")