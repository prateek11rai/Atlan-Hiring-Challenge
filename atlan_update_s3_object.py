from pyatlan.model.assets import S3Object 


def update_s3_object(client,s3_object_asset_qualified_name, s3_object_asset_name,atlan_owner_user,s3_object_asset_aws_region, s3_object_asset_key, s3_object_asset_size, s3_object_asset_storage_class, s3_object_asset_content_type, s3_object_asset_last_modified_at, s3_object_asset_version_id, s3_object_asset_content_disposition,s3_object_asset_aws_arn):

    term = S3Object.updater(
        qualified_name=s3_object_asset_qualified_name, # "default/s3/1709919425/arn:aws:s3:::atlan-tech-challenge-test/EMPLOYEE.csv"
        name=s3_object_asset_name, # "EMPLOYEE.csv"
    )
    term.aws_arn = s3_object_asset_aws_arn
    term.owner_users = [atlan_owner_user] #"prateek11rai"
    term.aws_region = s3_object_asset_aws_region #"us-east-2"
    term.s3_object_key = s3_object_asset_key #"EMPLOYEE.csv"
    term.name=s3_object_asset_name
    term.s3_object_size=s3_object_asset_size
    term.s3_object_storage_class=s3_object_asset_storage_class #"STANDARD"
    term.s3_object_content_type=s3_object_asset_content_type
    term.s3_object_last_modified_time=s3_object_asset_last_modified_at
    term.s3_object_version_id=s3_object_asset_version_id
    term.s3_object_content_disposition=s3_object_asset_content_disposition

    try:
        response = client.asset.update_merging_cm(term)  # 
        if updated := response.assets_updated(asset_type=S3Object):  # 
            term = updated[0]
            print(f"Updated the S3 object asset called {s3_object_asset_name}")
    except:  # 
        print("No existing asset to update, so nothing changed or created.")
