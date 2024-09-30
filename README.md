# Databricks UC Privilege Viewer

## Overview
The Databricks UC Privilege Viewer is a tool designed to simplify the process of viewing and managing user permissions in Databricks Unity Catalog. This notebook-based solution allows administrators to quickly retrieve and display a comprehensive list of privileges for a specific user, including permissions inherited from group memberships.

## Features
- Retrieve user information based on email address
- Identify direct and indirect group memberships
- List objects owned by the user or their groups
- Display privileges granted to the user or their groups across various securable objects in Unity Catalog

## Files
All notebooks are located in the `notebooks/` directory.

| File Name | Description |
|-----------|-------------|
| `databricks_sp_secret_manager.py` | Notebook for setting up secret scope and registering necessary credentials |
| `databricks_user_privileges_viewer.py` | Main notebook for viewing user privileges |
| `consts.py` | Constants used across notebooks |

## Quick Start
1. Clone this repository to your Databricks workspace.
2. Run `databricks_sp_secret_manager.py` to set up required secrets.
3. Open `databricks_user_privileges_viewer.py` and follow the instructions within the notebook.

## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support
For support, please open an issue in the GitHub repository.
