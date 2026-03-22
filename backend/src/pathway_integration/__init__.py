"""
Pathway Integration Module for AEGIS Trading System

Provides batch aggregation and consensus scoring for multi-agent trading decisions.
Uses Pathway's schema, groupby, and reduce operations without streaming.
"""

from .aggregator import PathwayAggregator

__all__ = ['PathwayAggregator']
