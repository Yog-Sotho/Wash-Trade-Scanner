"""
Detection-quality evaluation harness.

Measures every detector against labelled data instead of asserting that it
works: real swap histories recorded from chain (``fixtures/``), wash patterns
injected into them with exact labels, and a documented real wash-trading
market. ``wash-eval`` runs it and regenerates ``docs/benchmarks.md``.
"""
