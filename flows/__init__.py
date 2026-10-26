"""NYC TLC ELT package."""

import logging

_logger = logging.getLogger("flows")
if not _logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    )
    _logger.addHandler(_handler)
    _logger.setLevel(logging.INFO)
    _logger.propagate = False
