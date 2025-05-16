🧊 snow-control

snow-control is a Python-based tool for managing Snowflake-related workflows in a reproducible and configurable environment.

# 🚀 Prerequisites

## Create a .env File
Create a .env file in the project root with the following environment variables:
```bash
CONTROL_CONFIG_DIR=/absolute/path/to/control/config
SNOWFLAKE_USER="test@example.com"
SNOWFLAKE_ACCOUNT="your_account"
SNOWFLAKE_ORGANIZATION="your_org"
```

## Install Local Development Tools
Run the following command to set up the project dependencies in a virtual environment:
```shell
make install
```
# 🐚 Start a Virtual Environment Shell

Start an interactive shell with all environment variables and dependencies loaded:
```shell
make shell
```
## ▶️ Run the Tool

Use the following command to execute the tool:
```shell
python -m snow_control.control
```
## 📦 Build Artifacts

To package the project for distribution:
```shell
python -m build
```
