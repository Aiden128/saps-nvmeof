#!/usr/bin/env python3
"""Minimal JSON-RPC client for a local SPDK Unix-domain socket.

This client intentionally avoids importing SPDK's Python package.  It is used
by experiment harnesses whose setup timeout must measure the RPC operation,
not Python package discovery and module import time.
"""

import argparse
import json
import socket
import sys
import time


def call(socket_path: str, method: str, params: object, timeout_s: float) -> object:
    request_id = int(time.time_ns() & 0x7FFFFFFF)
    request = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
    }
    if params is not None:
        request["params"] = params

    payload = json.dumps(request, separators=(",", ":")).encode() + b"\n"
    decoder = json.JSONDecoder()
    received = bytearray()

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout_s)
        client.connect(socket_path)
        client.sendall(payload)
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            received.extend(chunk)
            try:
                response, _ = decoder.raw_decode(received.decode())
                break
            except json.JSONDecodeError:
                continue
        else:
            response = None

    if not received:
        raise RuntimeError("SPDK RPC peer closed without a response")
    if "response" not in locals():
        response, _ = decoder.raw_decode(received.decode())
    if response.get("id") != request_id:
        raise RuntimeError(
            f"SPDK RPC response id {response.get('id')} does not match {request_id}"
        )
    if "error" in response:
        error = response["error"]
        raise RuntimeError(
            f"SPDK RPC {method} failed: {error.get('code')} {error.get('message')}"
        )
    if "result" not in response:
        raise RuntimeError(f"SPDK RPC {method} returned no result")
    return response["result"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--socket", required=True)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("method")
    parser.add_argument("--params-json")
    args = parser.parse_args()

    params = json.loads(args.params_json) if args.params_json is not None else None
    result = call(args.socket, args.method, params, args.timeout)
    json.dump(result, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1)
