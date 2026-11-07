"""Separate BTC 30M/1H event research execution boundary.

This package has no real order-transport implementation.
"""
from .paper import PaperEvidenceError, PaperLedger, assess_signal

__all__ = ["PaperEvidenceError", "PaperLedger", "assess_signal"]
