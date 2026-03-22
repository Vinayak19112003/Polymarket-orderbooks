"""Structured rotating log configuration."""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .config import CollectorConfig


def setup_logging(config: CollectorConfig):
    log_dir = config.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Console
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    console.setLevel(logging.INFO)
    root.addHandler(console)

    # Rotating file
    fh = RotatingFileHandler(
        log_dir / "collector.log",
        maxBytes=50 * 1024 * 1024,  # 50 MB
        backupCount=5,
    )
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)
    root.addHandler(fh)

    # Quiet noisy libs
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
