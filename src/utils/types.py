from enum import Enum


class FetcherType(Enum):
    EPIDEMIOLOGY = 'epidemiology'
    GOVERNMENT_RESPONSE = 'government_response'
    MOBILITY = 'mobility'
    WEATHER = 'weather'
