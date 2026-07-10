#!/usr/bin/env python3
"""
Generate an API key + SHA-256 hash pair for the scanner API.

The plaintext key is shown once and handed to the client; only the hash
goes into the server's environment (API_KEY_HASHES).
"""

from api.auth import generate_api_key


def cli() -> None:
    """Entry point for the `wash-genkey` console script."""
    key, key_hash = generate_api_key()
    print("API key (give this to the client - it is shown only once):")
    print(f"  {key}")
    print()
    print("SHA-256 hash (append to API_KEY_HASHES in the server environment):")
    print(f"  {key_hash}")


if __name__ == "__main__":
    cli()
