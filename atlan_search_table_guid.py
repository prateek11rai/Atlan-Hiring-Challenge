from pyatlan.model.assets import Table
from pyatlan.model.fluent_search import FluentSearch


def search_table_guid(client,connection_asset_qualified_name,table_name):

    request = (
        FluentSearch()
        .where(FluentSearch.asset_type(Table))
        .where(FluentSearch.active_assets())
        .where(Table.NAME.eq(table_name)) #"EMPLOYEES"
        .include_on_results(Table.CONNECTION_QUALIFIED_NAME)  # 
    ).to_request()

    for asset in client.asset.search(request):
        search_connection_asset_qualified_name=asset.connection_qualified_name
        search_table_asset_guid=asset.guid

        if search_connection_asset_qualified_name==connection_asset_qualified_name: #"default/postgres/1709731987"
            return search_table_asset_guid
        
    return None