#!/usr/bin/env python3
"""
Train the ML model for wash trade detection.

This script trains an Isolation Forest anomaly detection model using
historical swap trade data. It supports:
- Multi-pool training data
- Heuristic-based labeling for training set generation
- Cross-validation for model evaluation
- Graceful shutdown handling

The trained model can be used for real-time wash trade detection
in the audit pipeline.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from typing import List, Optional

from core.storage import Storage
from core.feature_engineer import FeatureEngineer
from core.ml_detector import MLDetector
from config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ==============================================================================
# Graceful Shutdown Handler
# ==============================================================================

class GracefulShutdown:
    """
    Context manager for graceful shutdown handling.

    Handles SIGINT and SIGTERM signals to ensure clean shutdown
    of async resources like database connections.
    """

    def __init__(self) -> None:
        """Initialize shutdown handler."""
        self._shutdown_event = asyncio.Event()
        self._shutdown_initiated = False
        self._original_handlers: dict[int, Optional[signal.Handler]] = {}

    def __enter__(self) -> "GracefulShutdown":
        """Enter context manager - register signal handlers."""
        self._shutdown_event.set()  # Not shutdown yet
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: any) -> None:
        """Exit context manager - restore original handlers."""
        for sig, handler in self._original_handlers.items():
            if handler is not None:
                signal.signal(sig, handler)
            else:
                signal.signal(sig, signal.SIG_DFL)

    def _initiate_shutdown(self, signum: int, frame: any) -> None:
        """Signal handler callback."""
        sig_name = signal.Signals(signum).name
        logger.info(f"Received {sig_name}, initiating graceful shutdown...")

        if not self._shutdown_initiated:
            self._shutdown_initiated = True
            self._shutdown_event.clear()

    def register(self) -> None:
        """Register signal handlers for graceful shutdown."""
        for sig in (signal.SIGINT, signal.SIGTERM):
            self._original_handlers[sig] = signal.signal(sig, self._initiate_shutdown)

    @property
    def shutdown_requested(self) -> asyncio.Event:
        """Check if shutdown has been requested."""
        return self._shutdown_event

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()


# ==============================================================================
# Training Logic
# ==============================================================================

async def train_model(
    chain_id: int,
    pool_addresses: List[str],
    use_heuristic_labels: bool = True,
    save_model: bool = True,
    shutdown_event: Optional[asyncio.Event] = None,
) -> dict:
    """
    Train ML model for wash trade detection.

    Args:
        chain_id: Blockchain network ID
        pool_addresses: List of pool addresses for training data
        use_heuristic_labels: Use heuristic detection to generate labels
        save_model: Save the trained model to disk
        shutdown_event: Event to check for shutdown requests

    Returns:
        Dictionary containing training results
    """
    logger.info(f"Starting model training for chain {chain_id}")
    logger.info(f"Training pools: {len(pool_addresses)}")

    # Initialize storage
    storage = Storage()
    await storage.initialize()

    try:
        feature_engineer = FeatureEngineer(storage)
        ml_detector = MLDetector(storage, feature_engineer)

        # Check for shutdown before training
        if shutdown_event and shutdown_event.is_set():
            logger.info("Shutdown requested before training")
            return {"status": "shutdown", "message": "Shutdown requested"}

        logger.info("Building training features...")
        await ml_detector.train(
            chain_id=chain_id,
            pool_addresses=pool_addresses,
            use_heuristic_labels=use_heuristic_labels,
        )

        # Check for shutdown after feature building
        if shutdown_event and shutdown_event.is_set():
            logger.info("Shutdown requested during training")
            return {"status": "shutdown", "message": "Shutdown requested during training"}

        if save_model:
            ml_detector.save_model()
            logger.info(f"Model saved to {settings.ML_MODEL_PATH}")

        logger.info("Training complete")
        return {
            "status": "success",
            "chain_id": chain_id,
            "pools_trained": len(pool_addresses),
            "model_path": settings.ML_MODEL_PATH,
        }

    finally:
        # Clean up storage connection
        await storage.close()
        logger.info("Storage connection closed")


# ==============================================================================
# Main Entry Point
# ==============================================================================

async def async_main(args: argparse.Namespace) -> int:
    """
    Async main entry point with graceful shutdown support.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    shutdown = GracefulShutdown()
    shutdown.register()

    try:
        results = await train_model(
            chain_id=args.chain_id,
            pool_addresses=args.pools,
            use_heuristic_labels=not args.no_labels,
            save_model=not args.no_save,
            shutdown_event=shutdown.shutdown_requested,
        )

        if results.get("status") == "shutdown":
            logger.info("Training aborted due to shutdown request")
            return 0  # Graceful exit

        return 0  # Success

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130  # Standard exit code for SIGINT
    except Exception as e:
        logger.exception(f"Training failed with error: {e}")
        return 1  # Error exit code


def main() -> int:
    """
    Main entry point with signal handling setup.

    Returns:
        Exit code
    """
    parser = argparse.ArgumentParser(
        description="Train ML model for wash trade detection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --chain-id 1 --pools 0x... 0x...
  %(prog)s --chain-id 137 --pools 0x... --no-labels
  %(prog)s --chain-id 56 --pools 0x... --no-save
        """
    )
    parser.add_argument(
        "--chain-id",
        type=int,
        required=True,
        help="Blockchain chain ID"
    )
    parser.add_argument(
        "--pools",
        nargs="+",
        required=True,
        help="Pool addresses for training"
    )
    parser.add_argument(
        "--no-labels",
        action="store_true",
        help="Train without heuristic labels (unsupervised)"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save the model (test training)"
    )

    args = parser.parse_args()

    return asyncio.run(async_main(args))


if __name__ == "__main__":
    sys.exit(main())
