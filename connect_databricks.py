import os
import sys

from databricks.sdk import WorkspaceClient


def main() -> int:
    missing = [
        name
        for name in ("DATABRICKS_HOST", "DATABRICKS_TOKEN")
        if not os.getenv(name)
    ]
    if missing:
        print(
            "Missing required environment variable(s): " + ", ".join(missing),
            file=sys.stderr,
        )
        return 1

    token = os.environ["DATABRICKS_TOKEN"]
    if len(token) < 20:
        print("DATABRICKS_TOKEN is too short; paste the full token at the secure prompt.", file=sys.stderr)
        return 1

    client = WorkspaceClient()
    list(client.workspace.list("/"))
    print(f"Connected to {os.environ['DATABRICKS_HOST']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())