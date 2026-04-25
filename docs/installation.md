# Installation

## Prerequisites

- Python 3.10 or higher
- Docker (for PostgreSQL) or a running PostgreSQL instance
- Access to blockchain RPC endpoints (public or private)

## Steps

1. Clone the repository:

        git clone https://github.com/Yog-Sotho/Wash-Trade-Scanner.git
        cd Wash-Trade-Scanner

2. Create a virtual environment:

        python -m venv venv
        source venv/bin/activate   # Windows: venv\Scripts\activate

3. Install the package in editable mode:

        pip install -e .

4. Set up environment variables:

   Copy `.env.example` to `.env` and fill in the required values (see `docs/configuration.md`).

5. Start the database:

   Using the provided Docker command:

        docker run -d --name wash_detector_db \
          -e POSTGRES_USER=wash_user \
          -e POSTGRES_PASSWORD=wash_pass \
          -e POSTGRES_DB=wash_detector \
          -p 5432:5432 postgres:15-alpine

6. Initialize the database schema:

        python -c "from core.storage import Storage; import asyncio; asyncio.run(Storage().initialize())"

7. Verify installation by running a quick audit (you need a valid RPC and pool address):

        python scripts/run_audit.py --chain-id 1 --pool 0x... --no-sync --no-ml
