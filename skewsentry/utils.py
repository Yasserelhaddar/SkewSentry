"""Utility functions for SkewSentry framework.

This module provides common utilities used throughout SkewSentry, including
standardized logging configuration and other shared functionality.
"""

from __future__ import annotations

import logging


def get_logger(name: str = "skewsentry") -> logging.Logger:
    """Get a configured logger for SkewSentry modules.
    
    Creates a logger with consistent formatting and configuration across
    all SkewSentry modules. Loggers are cached to avoid duplicate handlers.
    
    Args:
        name: Logger name, typically the module name (default: "skewsentry")
        
    Returns:
        Configured logger instance with StreamHandler and standard formatting
        
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing batch of %d records", batch_size)
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

