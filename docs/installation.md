# Installation

## Prerequisites

- Python 3.11 or 3.12
- Docker (for PostgreSQL) or a running PostgreSQL instance
- Access to blockchain RPC endpoints (public or private)

## Steps

1. Clone the repository:

        git clone https://github.com/Yog-Sotho/Wash-Trade-Scanner.git
        cd Wash-Trade-Scanner

2. Create a virtual environment:

        python -m venv venv
        source venv/bin/activate   # Windows: venv\Scripts\activate

3. Install the hash-locked dependencies, then the package:

        make install       # runtime only: pip install --require-hashes -r requirements.txt
                           #               + pip install --no-deps .
        make install-dev   # with test and lint tooling (editable install)

   The lock files are generated from `pyproject.toml`; after changing a
   dependency there, run `make lock` (needs `uv`: `pip install uv`). A test
   fails if `pyproject.toml` and the locks disagree.

4. Set up environment variables:

   Copy `.env.example` to `.env` and fill in the required values (see `docs/configuration.md`).

5. Start the database:

        docker run -d --name wash_detector_db \
          -e POSTGRES_USER=wash_user \
          -e POSTGRES_PASSWORD=wash_pass \
          -e POSTGRES_DB=wash_detector \
          -p 5432:5432 postgres:15-alpine

6. The schema is created and upgraded by Alembic migrations whenever a command that
   uses the database starts. To run them explicitly:

        alembic upgrade head

   A database created by an earlier release (before migrations existed) is
   adopted automatically, keeping its data.

7. Verify installation by running a quick audit (you need a valid RPC and pool address):

        wash-audit --chain-id 1 --pool 0x... --start-block <N> --end-block <M> --no-ml

## Docker

See the "Docker" section of the README: `docker compose up -d` runs the API
and panel; `docker compose run --rm audit wash-audit ...` runs one-shot
commands.
