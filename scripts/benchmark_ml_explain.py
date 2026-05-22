
import asyncio
import time
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, AsyncMock
from core.ml_detector import MLDetector
from core.storage import Storage
from core.feature_engineer import FeatureEngineer

async def benchmark_explain():
    # Setup
    storage = MagicMock(spec=Storage)
    fe = MagicMock(spec=FeatureEngineer)
    detector = MLDetector(storage, fe)

    # Mock model
    detector.is_trained = True
    detector.model = MagicMock()

    def mock_decision_function(X):
        # Simulate some work
        # time.sleep(0.001) # too slow for 1000 calls
        return -np.sum(X, axis=1)

    detector.model.decision_function.side_effect = mock_decision_function

    # Create a large features_df
    num_trades = 5000
    num_features = len(detector.feature_columns)
    data = np.random.rand(num_trades, num_features)
    df = pd.DataFrame(data, columns=detector.feature_columns)

    print(f"--- Benchmarking explain_prediction with {num_trades} trades ---")

    start_time = time.perf_counter()
    explanation = await detector.explain_prediction(df, idx=2500)
    end_time = time.perf_counter()

    duration = end_time - start_time
    print(f"explain_prediction took {duration:.4f} seconds")

if __name__ == "__main__":
    asyncio.run(benchmark_explain())
