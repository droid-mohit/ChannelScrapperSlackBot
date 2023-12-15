import logging
from enum import Enum

from persistance.db_utils import create_token_config
from utils.utils import clean_string

logger = logging.getLogger(__name__)


class TokenSources(Enum):
    UNKNOWN = 0
    SENTRY = 1


def get_token_source(token_source):
    if token_source == 'sentry':
        return TokenSources.SENTRY
    else:
        return TokenSources.UNKNOWN


def handler_source_token_registration(user_email, source, token_config):
    source = clean_string(source)
    token_source = get_token_source(source)
    if token_source == TokenSources.UNKNOWN:
        logger.error(f"Invalid token source: {source}")
        return None
    if token_source == TokenSources.SENTRY:
        if 'bearer_token' not in token_config:
            logger.error(f"Invalid token config: {token_config}: bearer_token not found")
            return None
        if 'organization_slug' not in token_config:
            logger.error(f"Invalid token config: {token_config}: organization_slug not found")
            return None
        token_config = create_token_config(user_email, token_source.name, token_config)
        return token_config
    return None
