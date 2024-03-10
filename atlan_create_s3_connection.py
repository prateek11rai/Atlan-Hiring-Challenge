from pyatlan.model.assets import Connection
from pyatlan.model.enums import AtlanConnectorType
from atlan_search_s3_connection import search_s3_connection
from atlan_update_s3_connection import update_s3_connection



def create_s3_connection(client,s3_connection_asset_name,atlan_owner_user):

    search_s3_connection_asset_details=search_s3_connection(client,s3_connection_asset_name)

    if len(search_s3_connection_asset_details)==3:
        # found connection, update the details
        update_s3_connection(client,search_s3_connection_asset_details[0],search_s3_connection_asset_details[1],atlan_owner_user)

    else:
        connection = Connection.create(  # 
            name=s3_connection_asset_name,  # s3-connection-pr
            connector_type=AtlanConnectorType.S3,
            admin_users=[atlan_owner_user],  # prateek11rai
        )
        response = client.asset.save(connection)  # 
        s3_connection_asset_qualified_name = response.assets_created(asset_type=Connection)[0].qualified_name  
        s3_connection_asset_guid = response.assets_created(asset_type=Connection)[0].guid
        search_s3_connection_asset_details.append(s3_connection_asset_name)
        search_s3_connection_asset_details.append(s3_connection_asset_qualified_name)
        search_s3_connection_asset_details.append(s3_connection_asset_guid)

        update_s3_connection(client,search_s3_connection_asset_details[0],search_s3_connection_asset_details[1],atlan_owner_user)

        
    return search_s3_connection_asset_details