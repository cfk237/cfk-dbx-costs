"""
Log in with Azure CLI (az login) or Azure PowerShell (Connect-AzAccount) prior to execution
OR (Preferred) Use Az CLI or AzPowerShell ADO Tasks with a Service Connection
"""

import gc, sys, os, argparse, requests, re, time
from azure.core.credentials import AccessToken

from pathlib import Path
from fabric_cicd import (
    FabricWorkspace,
    publish_all_items,
    unpublish_all_orphan_items,
    change_log_level,
    append_feature_flag,
)
from azure.identity import *

import fabric_cicd.constants as constants
from fabric_cicd._parameter._utils import validate_parameter_file


def _set_pipeline_var(name: str, value: str) -> None:
    """Set a job-scoped output variable.

    GitHub Actions : appends NAME=value to $GITHUB_ENV so the variable is
                     available to all subsequent steps in the same job.
    """
    github_env_file = os.getenv("GITHUB_ENV")
    if github_env_file:
        with open(github_env_file, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")
    else:
        print(f"##vso[task.setvariable variable={name};issecret=false]{value}")


# ── Fabric API endpoints ───────────────────────────────────────────────────────
FABRIC_API_RESOURCE = "https://api.fabric.microsoft.com/"  # OAuth2 resource URL
FABRIC_API_BASE_URL = "https://api.fabric.microsoft.com/v1"  # REST API base URL


class Style:
    RED = "\033[31m"
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    RESET = "\033[0m"


# Secure TokenCredential wrapper for the raw token
class SimpleTokenCredential:
    def __init__(self, token):
        self._token = token

    def get_token(self, *scopes, **kwargs):
        return AccessToken(self._token, int(time.time()) + 3600)


# function to generate Fabric API token
def get_fabric_token(token_credential):
    """
    Generate and return a token for calling the Fabric REST API.

    Args:
        token_credential: Azure credential object for authentication

    Returns:
        Token object for Fabric API authorization
    """
    # generating the token used to call the Fabric REST API
    resource = FABRIC_API_RESOURCE
    scope = f"{resource}.default"
    print(f"scope set to {scope}")
    return token_credential.get_token(scope).token


# function to return the workspace ID
def get_workspace_id(p_ws_name, p_token):
    url = f"{FABRIC_API_BASE_URL}/workspaces"
    headers = {"Authorization": f"Bearer {p_token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers)
    ws_id = ""
    if response.status_code == 200:
        workspaces = response.json()["value"]
        for workspace in workspaces:
            if workspace["displayName"] == p_ws_name:
                ws_id = workspace["id"]
                return workspace["id"]
        if ws_id == "":
            return f"Error: Workspace {p_ws_name} could not be found."
    else:
        return f"Error: {response.status_code}, {response.text}"


# Force unbuffered output like `python -u`
sys.stdout.reconfigure(line_buffering=True, write_through=True)
sys.stderr.reconfigure(line_buffering=True, write_through=True)

# Enable debugging if defined in Azure DevOps pipeline
debug_is_enabled = os.getenv("SYSTEM_DEBUG", "false").lower() == "true"
if debug_is_enabled:
    change_log_level("DEBUG")

# Accept parsed arguments
parser = argparse.ArgumentParser(description="Process Azure Pipeline arguments.")
parser.add_argument(
    "--validate_deployment",
    action="store_true",
    help="Validate parameter.yml for deployment",
)
parser.add_argument(
    "--token_from_env",
    action="store_true",
    help="Whether to use FABRIC_TOKEN from environment variable instead of Azure Credential",
)
parser.add_argument(
    "--environment", type=str, help="Target environment for deployment", default="UAT"
)
parser.add_argument(
    "--workspace_name",
    type=str,
    help="Name of the Fabric workspace to deploy to without environment",
    default="MyFabricWorkspace",
)
parser.add_argument(
    "--root_directory",
    type=str,
    help="Root directory of the calling repository ($(Build.SourcesDirectory))",
    default=None,
)
parser.add_argument(
    "--repository_directory",
    type=str,
    help="Path to the local repository directory",
    default="fabric",
)
parser.add_argument(
    "--items_type_in_scope",
    type=str,
    help="List of item types in scope as a string representation of a list, e.g. '[Lakehouse, SQLDatabase]'",
    default="[VariableLibrary, Notebook, DataPipeline]",
)
parser.add_argument(
    "--deploy_items", action="store_true", help="indicates if deployment should be done"
)
parser.add_argument(
    "--enable_shortcut_publish",
    action="store_true",
    help="indicates if shortcuts should be published after deployments",
)
parser.add_argument(
    "--continue_on_shortcut_failure",
    action="store_true",
    help="indicates if deployment should continue even if shortcut publish fails",
)
args = parser.parse_args()

# Use explicit root_directory if provided, else fall back to $GITHUB_WORKSPACE (CI),
# or walk up three levels from .github/deploy/<script> to reach the repo root (local).
if args.root_directory:
    root_directory = Path(args.root_directory).resolve()
elif os.getenv("GITHUB_WORKSPACE"):
    root_directory = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
else:
    root_directory = Path(__file__).resolve().parent.parent.parent
print(f"Root directory set to {Style.GREEN}{root_directory}{Style.RESET}")

# define validation mode based on argument, this will only validate the parameter.yml file without deploying to Fabric if set to true
validate_deployment = args.validate_deployment
print(f"Validation set to {Style.GREEN}{validate_deployment}{Style.RESET}")

# define whether to use FABRIC_TOKEN from environment variable or Azure Credential for authentication
token_from_env = args.token_from_env
print(
    f"Token from environment variable set to {Style.GREEN}{token_from_env}{Style.RESET}"
)

# define the repository directory — normalize slashes for Windows compatibility
repository_directory = str(Path(f"{root_directory}/{args.repository_directory}"))
print(f"Repository directory set to {Style.GREEN}{repository_directory}{Style.RESET}")

# define the environment to deploy to
environment = args.environment
print(f"Environment set to {Style.GREEN}{environment}{Style.RESET}")

# define the workspace name to be deployed to
workspace_name = args.workspace_name
print(f"Workspace name set to {Style.GREEN}{workspace_name}{Style.RESET}")

# define the item types in scope by converting the string representation of list to an actual list
item_type_in_scope = re.findall(r"\w+", args.items_type_in_scope)  # ["VariableLibrary"]
print(f"Item types in scope set to {Style.GREEN}{item_type_in_scope}{Style.RESET}")

# define the deploy_items flag
deploy_items = args.deploy_items
print(f"deploy_items set to {Style.GREEN}{deploy_items}{Style.RESET}")

# define the enable_shortcut_publish flag
enable_shortcut_publish = args.enable_shortcut_publish
print(
    f"enable_shortcut_publish set to {Style.GREEN}{enable_shortcut_publish}{Style.RESET}"
)

# define the continue_on_shortcut_failure flag
continue_on_shortcut_failure = args.continue_on_shortcut_failure
print(
    f"continue_on_shortcut_failure set to {Style.GREEN}{args.continue_on_shortcut_failure}{Style.RESET}"
)

if validate_deployment:
    print(f"{Style.BLUE}Validating deployment...{Style.RESET}")
    # Here you would add your validation logic, for example:
    # - Check if parameter.yml files are correctly formatted
    # - Check if required parameters are present
    # - Run any schema validation against the parameter files
    # For demonstration, we'll just print a message
    validate_parameter_file(
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        # Comment to exclude target environment in validation
        environment=environment,
    )
    print(f"{Style.GREEN}Validation completed successfully!{Style.RESET}")
    sys.exit(0)  # Exit after validation without proceeding to deployment

# get the token from environment variable
if token_from_env:
    print("Obtaining token from environment variable...")
    token = os.environ.get("FABRIC_TOKEN")
    if not token:
        raise Exception("FABRIC_TOKEN environment variable not set.")
    token_credential = SimpleTokenCredential(token)
else:
    print("Obtaining token from Azure Credential...")
    # Use DefaultCredential to let ability to switch between Azure CLI or Azure PowerShell credential to authenticate
    token_credential = DefaultAzureCredential()
    print("Get Credential for authentication")
    # Get the token for Fabric API calls
    token = get_fabric_token(token_credential)

# determine the variable group which stores the workspace name with the naming convention "[branch]WorkspaceName"
ws_name = f"{workspace_name} [{environment}]"
# define workspace name to be deployed to based on value in variable group based on branch name. This variable group is not linked to a Key Vault hence the values can be access through os.environ
print(f"Obtaining GUID for {ws_name}...")

# call the workspace ID lookup function
lookup_response = get_workspace_id(ws_name, token)
if lookup_response.startswith("Error"):
    errmsg = f"{lookup_response}. Perhaps workspace name is set incorrectly..."
    raise ValueError(errmsg)
else:
    workspace_id = lookup_response
    print(f"Workspace ID for {workspace_name} set to {workspace_id}")

# Enable feature flags as needed
append_feature_flag("enable_lakehouse_unpublish")
append_feature_flag("enable_warehouse_unpublish")
append_feature_flag("enable_sqldatabase_unpublish")
append_feature_flag("enable_environment_variable_replacement")

if enable_shortcut_publish:
    append_feature_flag("enable_shortcut_publish")

if continue_on_shortcut_failure:
    append_feature_flag("continue_on_shortcut_failure")

if deploy_items:
    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    unpublish_all_orphan_items(target_workspace)


# --- Get Warehouse and Lakehouse Info with Connection String ---
def get_all_warehouse_connection_strings(workspace_id, token):
    url = f"{FABRIC_API_BASE_URL}/workspaces/{workspace_id}/items"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.get(url, headers=headers)
    items_info = []
    connection_string = None

    if response.status_code == 200:
        items = response.json().get("value", [])
        warehouses = [
            item for item in items if str(item.get("type")).lower() == "warehouse"
        ]
        lakehouses = [
            item for item in items if str(item.get("type")).lower() == "lakehouse"
        ]

        if not warehouses and not lakehouses:
            print("No warehouses or lakehouses found in workspace.", file=sys.stderr)
            return []

        # Get connection string once from the first warehouse (if available)
        if warehouses:
            print("Warehouses found in workspace:")
            wh = warehouses[0]  # Use first warehouse to get connection string
            wh_name = wh.get("displayName")
            wh_id = wh.get("id")
            print(f"  {wh_name} (id: {wh_id})")
            warehouse_url = (
                f"{FABRIC_API_BASE_URL}/workspaces/{workspace_id}/warehouses/{wh_id}"
            )
            warehouse_resp = requests.get(warehouse_url, headers=headers)

            if warehouse_resp.status_code == 200:
                warehouse_json = warehouse_resp.json()
                connection_string = warehouse_json.get("properties", {}).get(
                    "connectionString"
                )
                if not connection_string:
                    print(
                        f"Connection string not found for warehouse '{wh_name}'.",
                        file=sys.stderr,
                    )
            else:
                print(
                    f"Error getting details for warehouse '{wh_name}': {warehouse_resp.text}",
                    file=sys.stderr,
                )

            # Add all warehouses to the info list
            for wh in warehouses:
                wh_name = wh.get("displayName")
                items_info.append(
                    {
                        "type": "warehouse",
                        "name": wh_name,
                        "connection_string": connection_string,
                    }
                )

        # Add all lakehouses to the info list
        if lakehouses:
            print("Lakehouses found in workspace:")
            for lh in lakehouses:
                lh_name = lh.get("displayName")
                lh_id = lh.get("id")
                print(f"  {lh_name} (id: {lh_id})")
                items_info.append(
                    {
                        "type": "lakehouse",
                        "name": lh_name,
                        "connection_string": connection_string,
                    }
                )
    else:
        print(f"Error listing items: {response.text}", file=sys.stderr)
        return []

    return items_info


# Call the function to get all warehouse and lakehouse info as an array
items_array = get_all_warehouse_connection_strings(workspace_id, token)

# Filter warehouses and lakehouses
warehouse_items = [item for item in items_array if item["type"] == "warehouse"]
lakehouse_items = [item for item in items_array if item["type"] == "lakehouse"]

# Export the arrays as pipeline variables (semicolon-separated for YAML compatibility)
warehouse_names = ";".join([item["name"] for item in warehouse_items if item["name"]])
lakehouse_names = ";".join([item["name"] for item in lakehouse_items if item["name"]])
# We only need one connection string since it's the same for the workspace
connection_string = (
    items_array[0]["connection_string"]
    if items_array and items_array[0]["connection_string"]
    else ""
)

_set_pipeline_var("FABRIC_DWH_NAMES", warehouse_names)
_set_pipeline_var("FABRIC_LAKEHOUSE_NAMES", lakehouse_names)
_set_pipeline_var("WORKSPACE_SQL_ENDPOINT", connection_string)

# Optionally print the arrays for debug
print("ITEMS_ARRAY_START")
print("Warehouses:")
for item in warehouse_items:
    print(f"Name: {item['name']}")
print("Lakehouses:")
for item in lakehouse_items:
    print(f"Name: {item['name']}")
print(f"Connection String available: {'Yes' if connection_string else 'No'}")
print("ITEMS_ARRAY_END")


# Function to get all warehouse and lakehouse names and connection string
def get_workspace_items_and_connection_string(workspace_id, token):
    url = f"{FABRIC_API_BASE_URL}/workspaces/{workspace_id}/items"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.get(url, headers=headers)
    items_info = []
    connection_string = None
    lakehouse_conn_string = None
    warehouse_conn_string = None
    sqldb_conn_string = None
    sqldbname_conn_string = None
    sqldbs = []

    if response.status_code == 200:
        items = response.json().get("value", [])

        if debug_is_enabled:
            # Debug: Print raw items from API
            print(
                "DEBUG: Raw items from API:",
                [
                    {
                        "id": item.get("id"),
                        "name": item.get("displayName"),
                        "type": item.get("type"),
                    }
                    for item in items
                ],
            )

        warehouses = [
            item for item in items if str(item.get("type")).lower() == "warehouse"
        ]
        lakehouses = [
            item for item in items if str(item.get("type")).lower() == "lakehouse"
        ]
        sqldbs = [
            item for item in items if str(item.get("type")).lower() == "sqldatabase"
        ]
        # Get Lakehouse connection string if available
        if lakehouses:
            lh = lakehouses[0]
            lh_id = lh.get("id")
            lakehouse_url = (
                f"{FABRIC_API_BASE_URL}/workspaces/{workspace_id}/lakehouses/{lh_id}"
            )
            lakehouse_resp = requests.get(lakehouse_url, headers=headers)
            if lakehouse_resp.status_code == 200:
                lakehouse_json = lakehouse_resp.json()
                lakehouse_conn_string = (
                    lakehouse_json.get("properties", {})
                    .get("sqlEndpointProperties", {})
                    .get("connectionString")
                )
            else:
                print(
                    f"Error getting lakehouse details: {lakehouse_resp.text}",
                    file=sys.stderr,
                )
        # Get Warehouse connection string if no Lakehouse
        if not lakehouse_conn_string and warehouses:
            wh = warehouses[0]
            wh_id = wh.get("id")
            warehouse_url = (
                f"{FABRIC_API_BASE_URL}/workspaces/{workspace_id}/warehouses/{wh_id}"
            )
            warehouse_resp = requests.get(warehouse_url, headers=headers)
            if warehouse_resp.status_code == 200:
                warehouse_json = warehouse_resp.json()
                warehouse_conn_string = warehouse_json.get("properties", {}).get(
                    "connectionString"
                )
            else:
                print(
                    f"Error getting warehouse details: {warehouse_resp.text}",
                    file=sys.stderr,
                )
        # Get SQLDatabase serverFqdn if no Lakehouse or Warehouse
        if sqldbs:
            sqldb = sqldbs[0]
            sqldb_id = sqldb.get("id")
            sqldb_url = f"{FABRIC_API_BASE_URL}/workspaces/{workspace_id}/sqldatabases/{sqldb_id}"
            sqldb_resp = requests.get(sqldb_url, headers=headers)
            if sqldb_resp.status_code == 200:
                sqldb_json = sqldb_resp.json()
                sqldb_conn_string = sqldb_json.get("properties", {}).get("serverFqdn")
                sqldbname_conn_string = sqldb_json.get("properties", {}).get(
                    "databaseName"
                )
            else:
                print(
                    f"Error getting SQLDatabase details: {sqldb_resp.text}",
                    file=sys.stderr,
                )
        # Add all warehouses, lakehouses, and sqldatabases to the info list
        for wh in warehouses:
            wh_name = wh.get("displayName")
            items_info.append({"type": "warehouse", "name": wh_name})
        for lh in lakehouses:
            lh_name = lh.get("displayName")
            items_info.append({"type": "lakehouse", "name": lh_name})
        for sqldb in sqldbs:
            sqldb_name = sqldb.get("displayName")
            items_info.append({"type": "sqldatabase", "name": sqldb_name})
    else:
        print(f"Error listing items: {response.text}", file=sys.stderr)
        return [], None
    # Prefer Lakehouse > Warehouse > SQLDatabase
    dwae_connection_string = lakehouse_conn_string or warehouse_conn_string or ""
    sql_connection_string = sqldb_conn_string or ""
    return (
        items_info,
        dwae_connection_string,
        sql_connection_string,
        sqldbname_conn_string,
    )


# Get all warehouse and lakehouse names and the connection string
items_array, dwae_connection_string, sql_connection_string, sqldbname_conn_string = (
    get_workspace_items_and_connection_string(workspace_id, token)
)

# Debug: Print all the items returned from the API
print("DEBUG: All items from API:", items_array)

warehouse_items = [item for item in items_array if item["type"] == "warehouse"]
lakehouse_items = [item for item in items_array if item["type"] == "lakehouse"]
sqldb_items = [item for item in items_array if item["type"] == "sqldatabase"]
warehouse_names = ";".join([item["name"] for item in warehouse_items if item["name"]])
lakehouse_names = ";".join([item["name"] for item in lakehouse_items if item["name"]])
sqldb_names = ";".join([item["name"] for item in sqldb_items if item["name"]])
_set_pipeline_var("FABRIC_DWH_NAMES", warehouse_names)
_set_pipeline_var("FABRIC_LAKEHOUSE_NAMES", lakehouse_names)
_set_pipeline_var("FABRIC_SQLDB_NAMES", sqldb_names)
_set_pipeline_var("WORKSPACE_SQL_ENDPOINT", dwae_connection_string)
_set_pipeline_var("SRC_LAKEHOUSE_CONNECTION", dwae_connection_string)
_set_pipeline_var("SQLDB_ENDPOINT", sql_connection_string)
_set_pipeline_var("SQLDB_NAME", sqldbname_conn_string)

# Clear sensitive variables from memory after use
try:
    del token
    del SimpleTokenCredential
    gc.collect()
except Exception:
    pass