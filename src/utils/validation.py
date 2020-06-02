import logging
from utils.adapters import DataAdapter
from utils.email import send_email
from utils.fetcher_abstract import FetcherType

logger = logging.getLogger(__name__)


def validate_incoming_data(data_adapter: DataAdapter, fetcher_type: FetcherType, source_name: str):
    # TODO: Add fetcher type support, currently works only for epidemiology
    compare_result = data_adapter.call_db_function_compare(source_name)

    if compare_result[0] == 0:
        data_adapter.call_db_function_send_data(source_name)
        return True
    else:
        message = f"Validation failed for {source_name}, please check"
        try:
            send_email(source_name, message)
        except Exception as ex:
            logger.error(f'Unable to send an email {source_name}, {ex}')

        return False
