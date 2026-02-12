import atexit
import getpass
from datetime import datetime
import sys
from pathlib import Path
import time
import json
import math
import requests
import pandas


def on_exit(auth_token):
    print("Signing out- deleting auth token")
    print(
        requests.delete("https://api.onupkeep.com/api/v2/auth/",
                        headers={"Session-Token": auth_token}).json()
    )


print("Upkeep Work Order Downloader")
email = input("Enter email: ")
password = getpass.getpass(prompt="Enter password: ")

auth_response = requests.post("https://api.onupkeep.com/api/v2/auth",
                              data={"email": email, "password": password}
                              ).json()

if auth_response["success"] is False:
    print("Authentication token response failed")
    print(auth_response)
    sys.exit(1)

auth_token = auth_response["result"]["sessionToken"]
# to be sent in http requests to upkeep api
auth_token_header = {"Session-Token": auth_token}
atexit.register(on_exit, auth_token)  # deactivate auth token on exit
print("Auth token successfully generated")
print(f"Auth token will expire on {auth_response["result"]["expiresAt"]}")
del email, password, auth_response

print("Fetching all work orders...")
work_orders_raw = requests.get("https://api.onupkeep.com/api/v2/work-orders",
                               headers=auth_token_header,
                               params={"limit": 5000}).json()

if work_orders_raw["success"] is False:
    print("All Work Orders response failed")
    print(work_orders_raw)
    sys.exit(1)

# drill down into results array for less boilerplate
work_orders = work_orders_raw["results"]
del work_orders_raw

print(f"Loaded {len(work_orders)} work orders")


def get_data_for_id(id, api_folder_name, folder_cache, auth_token_header):
    if id == "" or id is None:
        return None

    if id in folder_cache[api_folder_name]:
        return folder_cache[api_folder_name][id]

    subpage_url = f"https://api.onupkeep.com/api/v2/{api_folder_name}/{id}"
    subpage = requests.get(
        subpage_url,
        headers=auth_token_header
    ).json()

    if subpage["success"] is False:
        print(f"Response failed for: {subpage_url}", file=sys.stderr)
        print(subpage, file=sys.stderr)
        return None

    print(f"Fetched new page: {api_folder_name}/{id}")
    folder_cache[api_folder_name][id] = subpage["result"]
    return subpage["result"]


def replace_ids_with_full_data(work_order, id_field_name_to_api_folder_name, folder_cache, auth_token_header):
    for id_field_name in id_field_name_to_api_folder_name:
        if id_field_name not in work_order:
            continue

        # since these replace functions convert key: "id" into key: {...},
        # we must initially convert key: "id" to key: {"id": "id"}
        # s.t. in case the replace function fails, there are no occurrences
        # left behind of key: string that would create a (mostly) empty column
        # when converting work orders to CSV.
        # since all subpage data has "id" field, said failures will not make
        # a new column
        if not isinstance(work_order[id_field_name], dict):
            work_order[id_field_name] = {"id": work_order[id_field_name]}

        if "id" not in work_order[id_field_name]:
            continue

        api_folder_name = id_field_name_to_api_folder_name[id_field_name]
        subpage = get_data_for_id(work_order[id_field_name]["id"],
                                  api_folder_name,
                                  folder_cache,
                                  auth_token_header)
        if subpage is None:
            continue

        work_order[id_field_name] = subpage


def replace_ids_with_select_fields(work_order, id_field_name_to_api_folder_name, replacement_fields, folder_cache, auth_token_header):
    for id_field_name in id_field_name_to_api_folder_name:
        if id_field_name not in work_order:
            continue

        # since these replace functions convert key: "id" into key: {...},
        # we must initially convert key: "id" to key: {"id": "id"}
        # s.t. in case the replace function fails, there are no occurrences
        # left behind of key: string that would create a (mostly) empty column
        # when converting work orders to CSV.
        # since all subpage data has "id" field, said failures will not make
        # a new column
        if not isinstance(work_order[id_field_name], dict):
            work_order[id_field_name] = {"id": work_order[id_field_name]}

        if "id" not in work_order[id_field_name]:
            continue

        api_folder_name = id_field_name_to_api_folder_name[id_field_name]
        subpage = get_data_for_id(work_order[id_field_name]["id"],
                                  api_folder_name,
                                  folder_cache,
                                  auth_token_header)
        if subpage is None:
            continue

        replacement = {}
        for replacement_field in replacement_fields:
            if replacement_field not in subpage:
                continue

            replacement[replacement_field] = subpage[replacement_field]

        work_order[id_field_name] = replacement


def replace_user_ids_with_fullname(work_order, user_id_field_names, folder_cache, auth_token_header):
    for user_id_field_name in user_id_field_names:
        if user_id_field_name not in work_order:
            continue

        # since these replace functions convert key: "id" into key: {...},
        # we must initially convert key: "id" to key: {"id": "id"}
        # s.t. in case the replace function fails, there are no occurrences
        # left behind of key: string that would create a (mostly) empty column
        # when converting work orders to CSV.
        # since all subpage data has "id" field, said failures will not make
        # a new column
        if not isinstance(work_order[user_id_field_name], dict):
            work_order[user_id_field_name] = {
                "id": work_order[user_id_field_name]}

        if "id" not in work_order[user_id_field_name]:
            continue

        api_folder_name = "users"
        subpage = get_data_for_id(work_order[user_id_field_name]["id"],
                                  api_folder_name,
                                  folder_cache,
                                  auth_token_header)
        if subpage is None:
            continue

        replacement = {}
        if "id" in subpage:
            replacement["id"] = subpage["id"]

        fullname = ""
        if "firstName" in subpage:
            fullname = subpage["firstName"]
        if "lastName" in subpage:
            fullname += " "
            fullname += subpage["lastName"]
        replacement["fullName"] = fullname
        work_order[user_id_field_name] = replacement


def prompt_load_file_cache(folder_cache):
    root_dir = Path(__file__).parent
    cache_dir = root_dir / "upkeep-api-cache"
    if not root_dir.is_dir():
        return
    cache_dir.mkdir(exist_ok=True)
    if not cache_dir.is_dir():
        return
    # grab a single file in the cache to determine the cache age
    oldest_file = None
    oldest_file_mtime = None
    files_in_cache = 0
    for folder_name in folder_cache:
        folder = cache_dir / folder_name
        folder.mkdir(exist_ok=True)
        if not folder.is_dir():
            continue
        for child in folder.glob("*.json"):
            if not child.is_file:
                continue
            child_mtime = child.stat().st_mtime
            if oldest_file_mtime is None or child_mtime < oldest_file_mtime:
                oldest_file_mtime = child_mtime
                oldest_file = child
            files_in_cache = files_in_cache + 1

    if oldest_file is None:
        print("No file cache found")
        return False

    cache_age_seconds = time.time() - oldest_file_mtime
    hours = math.floor(cache_age_seconds / 3600)
    minutes = math.floor((cache_age_seconds % 3600) / 60)
    seconds = math.floor(cache_age_seconds % 60)

    use_cache = input(f"""Subpage cache found {files_in_cache} files with \
oldest age of: {hours}h{minutes}m{seconds}s
Would you like to use it? (y/N): """)

    if use_cache.lower() != "y":
        print("Cache will be updated on this run")
        return False

    cached_files_loaded = 0
    for folder_name in folder_cache:
        folder = cache_dir / folder_name
        for file in folder.glob("*.json"):
            with file.open("r") as f:
                data = json.load(f)
                if "id" in data and data["id"] != "" and data["id"] is not None:
                    folder_cache[folder_name][data["id"]] = data
                    cached_files_loaded = cached_files_loaded + 1

    print(f"Files loaded from cache: {cached_files_loaded}")
    return True


def save_cache(folder_cache):
    root_dir = Path(__file__).parent
    cache_dir = root_dir / "upkeep-api-cache"
    if not root_dir.is_dir():
        return
    cache_dir.mkdir(exist_ok=True)
    if not cache_dir.is_dir():
        return

    # remove all existing cached files
    for folder_name in folder_cache:
        folder = cache_dir / folder_name
        for file in folder.glob("*.json"):
            file.unlink()

    files_cached = 0
    for folder_name in folder_cache:
        folder = cache_dir / folder_name
        folder.mkdir(exist_ok=True)
        if not folder.is_dir():
            continue
        for id in folder_cache[folder_name]:
            export = folder / f"{id}.json"
            with export.open("w") as f:
                f.write(json.dumps(folder_cache[folder_name][id], indent=4))
            files_cached = files_cached + 1
    print(f"Subpages cached and saved to disk: {files_cached}")


# keys must exactly match upkeep's api subpage name
folder_cache = {
    "assets": {},
    "locations": {},
    "users": {}
}

file_cache_used = prompt_load_file_cache(folder_cache)

i = 1
for work_order in work_orders:
    # <id field name>: <upkeep api folder name it is under>
    fields_to_be_replaced_with_full_data = {
        "asset": "assets",
        "location": "locations",
        "objectLocationForWorkOrder": "locations"
    }
    replace_ids_with_full_data(work_order,
                               fields_to_be_replaced_with_full_data,
                               folder_cache,
                               auth_token_header
                               )

    # fields_to_be_replaced_with_select_subpage_fields = {
    #     "location": "locations",
    #     "objectLocationForWorkOrder": "locations"
    # }
    # select_fields_replacement = ["id", "name"]
    # replace_ids_with_select_fields(work_order,
    #                                fields_to_be_replaced_with_select_subpage_fields,
    #                                select_fields_replacement,
    #                                folder_cache,
    #                                auth_token_header
    #                                )

    user_fields_to_be_replaced_with_fullname = ["completedByUser",
                                                "assignedByUser",
                                                "assignedToUser",
                                                "updatedBy"]
    replace_user_ids_with_fullname(work_order,
                                   user_fields_to_be_replaced_with_fullname,
                                   folder_cache,
                                   auth_token_header)

    i = i + 1
    if (i % 100) == 0:
        print(f"Progress: {i}/{len(work_orders)}")


root_dir = Path(__file__).parent
export_dir = root_dir / "upkeep-csv-exports"
if not root_dir.is_dir():
    print("Could not export csv: parent dir of python file does not exist")
    sys.exit(1)
export_dir.mkdir(exist_ok=True)
if not export_dir.is_dir():
    print(f"Could not export csv: {export_dir} could not be created")

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
export_path = export_dir / f"{timestamp}.csv"

export = pandas.json_normalize(work_orders, sep='.')
with export_path.open("w") as csv_path:
    export.to_csv(csv_path)
    print(f"Exported csv to: {export_path}")
if not file_cache_used:
    save_cache(folder_cache)
