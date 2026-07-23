"""
Logger padronizado do projeto.

Uso:
    from config.logger import get_logger
    log = get_logger("bronze")
"""

import logging
import sys

FORMATO = "%(asctime)s | %(levelname)-7s | %(name)-8s | %(message)s"
DATA_FORMATO = "%Y-%m-%d %H:%M:%S"


def get_logger(nome: str, nivel: int = logging.INFO) -> logging.Logger:
    """Devolve um logger configurado, sem duplicar handlers."""
    logger = logging.getLogger(nome)

    if logger.handlers:
        return logger

    logger.setLevel(nivel)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(FORMATO, datefmt=DATA_FORMATO))
    logger.addHandler(handler)
    logger.propagate = False

    return logger
