from pyatlan.model.assets import Connection



def update_s3_connection(client,s3_connection_asset_name,s3_connection_asset_qualified_name,atlan_owner_user):
    term = Connection.updater(
        qualified_name=s3_connection_asset_qualified_name, # "default/s3/1709919425"
        name=s3_connection_asset_name, # "s3-connection-pr"
    )
    term.owner_users = [atlan_owner_user]
    term.name = s3_connection_asset_name
    try:
        response = client.asset.update_merging_cm(term)  # 
        if updated := response.assets_updated(asset_type=Connection):  # 
            term = updated[0]
            print(f"S3 Connection Asset Updated Called : {s3_connection_asset_name}")
    except :  # 
        print("No existing asset to update, so nothing changed or created.")
