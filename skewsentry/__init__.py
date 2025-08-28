"""SkewSentry: Catch training ↔ serving feature skew before you ship.

SkewSentry validates feature parity between offline training pipelines and 
online serving pipelines to prevent ML model degradation in production.

Public API will be expanded in future versions. For now we expose the
package version to support import smoke tests.
"""

__all__ = ["__version__"]
__version__ = "0.1.0"

