import logging

log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt='%Y-%m-%dT%H:%M:%S',
)

logger = logging.getLogger()
