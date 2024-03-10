from pyatlan.model.assets import S3Bucket
from pyatlan.model.fluent_search import FluentSearch, CompoundQuery



def search_s3_bucket(client,atlan_s3_bucket_asset_name,atlan_aws_s3_bucket_arn):

    index = (
        FluentSearch()  # 
        .where(CompoundQuery.active_assets())  # 
        .where(CompoundQuery.asset_type(S3Bucket))
        .where(S3Bucket.NAME.eq(atlan_s3_bucket_asset_name))  #"atlan-tech-challenge-pr"
    ).to_request()

    results = client.asset.search(index)
    search_s3_bucket_asset_details=[]
    for asset in results:
        if isinstance(asset, S3Bucket):
            search_s3_bucket_asset_name = asset.attributes.name
            search_s3_bucket_asset_qualified_name = asset.attributes.qualified_name
            search_s3_bucket_asset_guid = asset.guid
            search_s3_bucket_asset_arn = asset.aws_arn
            
            if search_s3_bucket_asset_name == atlan_s3_bucket_asset_name and search_s3_bucket_asset_arn==atlan_aws_s3_bucket_arn:
                search_s3_bucket_asset_details.append(search_s3_bucket_asset_name)
                search_s3_bucket_asset_details.append(search_s3_bucket_asset_qualified_name)
                search_s3_bucket_asset_details.append(search_s3_bucket_asset_guid)
                search_s3_bucket_asset_details.append(search_s3_bucket_asset_arn)
                break

    return search_s3_bucket_asset_details