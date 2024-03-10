from pyatlan.model.assets import S3Object 
from pyatlan.model.fluent_search import FluentSearch, CompoundQuery



def search_s3_object(client,s3_object_asset_name, s3_object_asset_arn):

    index = (
        FluentSearch()  # 
        .where(CompoundQuery.active_assets())  # 
        .where(CompoundQuery.asset_type(S3Object))
        .where(S3Object.NAME.eq(s3_object_asset_name)) # "EMPLOYEE.csv"
    ).to_request()

    results = client.asset.search(index)
    search_s3_object_asset_details=[]
    for asset in results:
        if isinstance(asset, S3Object):
            search_s3_object_asset_name = asset.attributes.name
            search_s3_object_asset_qualified_name = asset.attributes.qualified_name
            search_s3_object_asset_guid = asset.guid
            search_s3_object_asset_arn = asset.aws_arn
            
            if search_s3_object_asset_name == s3_object_asset_name and search_s3_object_asset_arn==s3_object_asset_arn:
                search_s3_object_asset_details.append(search_s3_object_asset_name)
                search_s3_object_asset_details.append(search_s3_object_asset_qualified_name)
                search_s3_object_asset_details.append(search_s3_object_asset_guid)
                search_s3_object_asset_details.append(search_s3_object_asset_arn)
                break
    
    return search_s3_object_asset_details