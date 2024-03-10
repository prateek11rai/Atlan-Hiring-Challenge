from pyatlan.model.assets import Process
from pyatlan.model.fluent_search import FluentSearch


def search_process_exist(client,process_name,s3_connection_asset_qualified_name):

    request = (
        FluentSearch()
        .where(FluentSearch.asset_type(Process))
        .where(FluentSearch.active_assets())
        .where(Process.NAME.eq(process_name)) 
        .include_on_results(Process.CONNECTION_QUALIFIED_NAME)  # 
    ).to_request()

    for asset in client.asset.search(request):
        search_connection_asset_qualified_name=asset.connection_qualified_name
        search_process_asset_guid=asset.guid
        if search_connection_asset_qualified_name==s3_connection_asset_qualified_name: #"default/s3/1709731987"
            return True
    
    return False
