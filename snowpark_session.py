"""
Snowpark session helper.

Uses ~/.snowflake/connections.toml (Snowflake CLI config).
Override connection name via $SNOWFLAKE_CONNECTION_NAME env var.
"""

import os
from snowflake.snowpark import Session


def create_snowpark_session() -> Session:
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    return Session.builder.configs({"connection_name": connection_name}).create()
