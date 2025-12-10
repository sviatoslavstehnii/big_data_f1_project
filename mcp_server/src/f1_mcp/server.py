"""FastMCP server entry point for F1 Databricks Gateway."""

import sys
from typing import Optional

from fastmcp import FastMCP

from f1_mcp.tools import (
    register_sql_tools,
    register_schema_tools,
    register_visualization_tools,
)


def create_server() -> FastMCP:
    mcp = FastMCP(
        name="f1-databricks-gateway",
    )

    register_sql_tools(mcp)
    register_schema_tools(mcp)
    register_visualization_tools(mcp)

    return mcp


mcp = create_server()


def main(transport: Optional[str] = None) -> None:
    transport = transport or "stdio"

    if transport == "sse":
        mcp.run(transport="sse")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    transport = "stdio"
    if len(sys.argv) > 1:
        if sys.argv[1] in ("--sse", "-s"):
            transport = "sse"
        elif sys.argv[1] in ("--help", "-h"):
            print("F1 Databricks MCP Server")
            print()
            print("Usage: f1-mcp [OPTIONS]")
            print()
            print("Options:")
            print("  --sse, -s    Use SSE transport (for web-based agents)")
            print("  --help, -h   Show this help message")
            print()
            print("By default, uses stdio transport for Claude Desktop.")
            sys.exit(0)

    main(transport)

