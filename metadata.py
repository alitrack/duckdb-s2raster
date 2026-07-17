#!/usr/bin/env python3
"""Append DuckDB extension metadata footer."""

import argparse
import os
import platform
import shutil


def detect_platform():
    system = platform.system()
    machine = platform.machine()
    if system == "Linux":
        return "linux_amd64" if machine == "x86_64" else "linux_arm64"
    elif system == "Darwin":
        return "osx_arm64" if machine == "arm64" else "osx_amd64"
    elif system == "Windows":
        return "windows_amd64"
    return "linux_amd64"


def start_signature():
    encoded = "".encode("ascii")
    encoded += int(0).to_bytes(1, "big")
    encoded += int(147).to_bytes(1, "big")
    encoded += int(4).to_bytes(1, "big")
    encoded += int(16).to_bytes(1, "big")
    encoded += b"duckdb_signature"
    encoded += int(128).to_bytes(1, "big")
    encoded += int(4).to_bytes(1, "big")
    return encoded


def padded_byte_string(s):
    encoded = s.encode("ascii")
    return encoded + b"\x00" * (32 - len(encoded))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Input shared library")
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("--platform", default=None)
    parser.add_argument("--duckdb-version", default="v1.5.4")
    parser.add_argument("--extension-version", default="0.1.0")
    args = parser.parse_args()

    platform_str = args.platform or detect_platform()
    shutil.copyfile(args.input, args.output)

    with open(args.output, "ab") as f:
        f.write(start_signature())
        f.write(padded_byte_string(""))
        f.write(padded_byte_string(""))
        f.write(padded_byte_string(""))
        f.write(padded_byte_string("C_STRUCT"))
        f.write(padded_byte_string(args.extension_version))
        f.write(padded_byte_string(args.duckdb_version))
        f.write(padded_byte_string(platform_str))
        f.write(padded_byte_string("4"))
        f.write(b"\x00" * 256)

    size_mb = os.path.getsize(args.output) / (1024 * 1024)
    print(f"Extension written: {args.output} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
