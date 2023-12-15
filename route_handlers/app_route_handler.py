from enum import Enum


class TokenSources(Enum):
    UNKNOWN = 0
    SLACK = 1
    GOOGLECHAT = 2
    SENTRY = 3
