"""Test utils module."""

import logging

from skewsentry.utils import get_logger


def test_get_logger_default_name():
    """Test get_logger with default name."""
    logger = get_logger()
    assert logger.name == "skewsentry"
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1


def test_get_logger_custom_name():
    """Test get_logger with custom name."""
    logger = get_logger("custom")
    assert logger.name == "custom"
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1


def test_get_logger_idempotent():
    """Test that get_logger doesn't add duplicate handlers."""
    logger1 = get_logger("test")
    logger2 = get_logger("test")
    assert logger1 is logger2
    assert len(logger1.handlers) == 1