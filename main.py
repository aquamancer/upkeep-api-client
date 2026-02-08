import atexit
import sys
import os
import json
import requests
import pandas


def on_exit(auth_token):
    print("Signing out- deleting auth token")
    print(
        requests.delete("https://api.onupkeep.com/api/v2/auth/",
                        headers={"Session-Token": auth_token}).json()
    )


# request for auth token had its response piped into this program
auth_response = json.load(sys.stdin)

if auth_response["success"] is False:
    print("Authentication token response failed")
    print(auth_response)
    sys.exit(1)

auth_token = auth_response["result"]["sessionToken"]
# to be sent in http requests to api
auth_token_header = {"Session-Token": auth_token}
atexit.register(on_exit, auth_token)  # deactivate auth token on exit
print("Auth token successfully generated")
print(f"Auth token will expire on {auth_response["result"]["expiresAt"]}")
del auth_response  # raw response no longer needed

work_orders_raw = requests.get("https://api.onupkeep.com/api/v2/work-orders",
                               headers=auth_token_header).json()

if work_orders_raw["success"] is False:
    print("All Work Orders response failed")
    print(work_orders_raw)
    sys.exit(1)

# drill down into results array for less boilerplate
work_orders = work_orders_raw["results"]
del work_orders_raw

print(f"Loaded {len(work_orders)} work orders")


def replace_ids_with_child_field(work_order,
                                 id_keys,
                                 child_field_replacement_key,
                                 cached_ids,
                                 auth_token_header,
                                 api_subpage_name):
    for id_key in id_keys:
        if id_key not in work_order:
            # print(f"Key '{id_key}' not in work order {work_order["id"]}")
            continue

        # replace work_order.asset with work_order.asset.result.{replace_with}
        id = work_order[id_key]
        if id == "" or id is None:
            continue

        if id in cached_ids:
            # need this conditional since we cache subpages even if
            # the desired key/value pair doesn't exist
            if child_field_replacement_key in cached_ids[id]:
                work_order[id_key] = cached_ids[id][child_field_replacement_key]
        else:
            child_api_url = f"https://api.onupkeep.com/api/v2/{
                api_subpage_name}/{id}"
            fetched = requests.get(
                child_api_url,
                headers=auth_token_header
            ).json()

            if fetched["success"] is False:
                if "not found" in fetched["message"]:
                    # data for id doesn't exist
                    print(
                        f"No page/data exists for {id_key} ID: {id}", sys.stderr)
                else:
                    print(f"{id_key} response failed for: {
                          child_api_url}", sys.stderr)
                    print(fetched, sys.stderr)
                continue

            print(f"Fetched new page: {api_subpage_name}/{id}")
            # need this conditional since the fetched subpage could just not
            # have the key/value pair
            if child_field_replacement_key in fetched["result"]:
                work_order[id_key] = fetched["result"][child_field_replacement_key]
            # add to cache regardless if the key/value pair exists
            # there is a conditional on a cache hit to check if the key/value
            # pair exists so this is ok
            # also there could be an id that would make use of the cache wants
            # to be replaced with a child key that actually exists
            cached_ids[id] = fetched["result"]


# todo clean this up
def replace_user_id_with_fullname(work_order,
                                  id_keys,
                                  child_field_replacement_keys,
                                  cached_ids,
                                  auth_token_header,
                                  api_subpage_name):
    for id_key in id_keys:
        if id_key not in work_order:
            # print(f"Key '{id_key}' not in work order {work_order["id"]}")
            continue

        # replace work_order.asset with work_order.asset.result.{replace_with}
        id = work_order[id_key]
        if id == "" or id is None:
            continue

        if id in cached_ids and "fullName" in cached_ids[id] and cached_ids[id]["fullName"] != "":
            work_order[id_key] = cached_ids[id]["fullName"]
        else:
            child_api_url = f"https://api.onupkeep.com/api/v2/{
                api_subpage_name}/{id}"
            fetched = requests.get(
                child_api_url,
                headers=auth_token_header
            ).json()

            if fetched["success"] is False:
                if "not found" in fetched["message"]:
                    # data for id doesn't exist
                    print(
                        f"No page/data exists for {id_key} ID: {id}", sys.stderr)
                else:
                    print(f"{id_key} response failed for: {
                          child_api_url}", sys.stderr)
                    print(fetched, sys.stderr)
                continue

            print(f"Fetched new page: {api_subpage_name}/{id}")
            # need this conditional since the fetched subpage could just not
            # have the key/value pair
            replacement_value = ""
            first_word = True
            for child_key in child_field_replacement_keys:
                if child_key in fetched["result"]:
                    if first_word:
                        first_word = False
                    else:
                        replacement_value += " "

                    replacement_value += fetched["result"][child_key]

            if replacement_value != "":
                work_order[id_key] = replacement_value
            cached_ids[id] = fetched["result"]
            # add fullName to cache to avoid recombining first and last name
            cached_ids[id]["fullName"] = replacement_value


asset_cache = {}
location_cache = {}
user_cache = {}

i = 1
for work_order in work_orders:
    # replace asset ids
    replace_ids_with_child_field(
        work_order,
        ["asset"],
        "category",
        asset_cache,
        auth_token_header,
        "assets"
    )
    # replace location ids
    replace_ids_with_child_field(
        work_order,
        ["location", "objectLocationForWorkOrder"],
        "name",
        location_cache,
        auth_token_header,
        "locations"
    )
    # replace user ids
    replace_user_id_with_fullname(
        work_order,
        ["completedByUser", "assignedByUser", "assignedToUser", "updatedBy"],
        ["firstName", "lastName"],
        user_cache,
        auth_token_header,
        "users"
    )

    i = i + 1
    if (i % 100) == 0:
        print(f"Progress: {i}/{len(work_orders)}")


export = pandas.DataFrame(work_orders)
export_path = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), "export3.csv")
with open(export_path, "w") as csv_path:
    export.to_csv(csv_path)
