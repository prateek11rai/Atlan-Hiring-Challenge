from pyatlan.model.assets import Connection
from pyatlan.model.fluent_search import CompoundQuery, FluentSearch

def search_s3_connection(client,s3_connection_asset_name):

    index = (
        FluentSearch()  # 
        .where(CompoundQuery.active_assets())  # 
        .where(CompoundQuery.asset_type(Connection))
        .where(Connection.NAME.eq(s3_connection_asset_name))  #"s3-connection-pr" 
    ).to_request()

    results = client.asset.search(index)
    search_s3_connection_asset_details=[]
    for asset in results:
        if isinstance(asset, Connection):
            search_s3_connection_asset_name = asset.attributes.name
            search_s3_connection_asset_qualified_name = asset.attributes.qualified_name
            search_s3_connection_asset_guid = asset.guid
            
            if search_s3_connection_asset_name == s3_connection_asset_name: #"s3-connection-pr"
                search_s3_connection_asset_details.append(search_s3_connection_asset_name)
                search_s3_connection_asset_details.append(search_s3_connection_asset_qualified_name)
                search_s3_connection_asset_details.append(search_s3_connection_asset_guid)
                break
                
    return search_s3_connection_asset_details