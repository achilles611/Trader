"""Paper-first public-wallet copy-trading research subsystem.

The package intentionally has no private-key or exchange-order dependency.  It
records public source data and can only simulate executions in this alpha.
"""

from .config import CopyTradeConfig
from .models import RawFill, Target

__all__ = ["CopyTradeConfig", "RawFill", "Target"]
