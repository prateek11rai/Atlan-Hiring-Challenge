from pyatlan.model.assets import S3Object
from pyatlan.model.fluent_search import FluentSearch


def search_s3_object_guid(client,connection_asset_qualified_name,s3_object_name):

    request = (
        FluentSearch()
        .where(FluentSearch.asset_type(S3Object))
        .where(FluentSearch.active_assets())
        .where(S3Object.NAME.eq(s3_object_name)) #"EMPLOYEES.csv"
        .include_on_results(S3Object.CONNECTION_QUALIFIED_NAME)  # 
    ).to_request()

    for asset in client.asset.search(request):
        search_connection_asset_qualified_name=asset.connection_qualified_name
        search_s3_object_asset_guid=asset.guid

        if search_connection_asset_qualified_name==connection_asset_qualified_name: #"default/s3/1709731987"
            return search_s3_object_asset_guid
        
    return None