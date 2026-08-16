from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .discovery import publish_endpoint, retire_endpoint
from .server import ControlApiServer
from ascendop_control.storage import ControlStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AscendOP Flow V4 management API")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--endpoint-file", type=Path)
    parser.add_argument("--generation", required=True)
    args = parser.parse_args(argv)
    store = ControlStore(args.database)
    store.assert_compatible()
    token_document = json.loads(args.token_file.read_text(encoding="utf-8"))
    tokens = {
        str(item["token_sha256"]): str(item["capability"])
        for item in token_document.get("tokens", [])
    }
    server = ControlApiServer(
        (args.host, args.port),
        store=store,
        token_capabilities=tokens,
        code_generation=args.generation,
    )
    if args.endpoint_file is not None:
        publish_endpoint(
            args.endpoint_file,
            host=str(server.server_address[0]),
            port=int(server.server_address[1]),
            generation=args.generation,
            pid=os.getpid(),
        )
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if args.endpoint_file is not None:
            retire_endpoint(
                args.endpoint_file,
                generation=args.generation,
                pid=os.getpid(),
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
