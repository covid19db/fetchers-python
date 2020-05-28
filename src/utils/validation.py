import logging
from utils.adapters import DataAdapter
from utils.email import send_email

logger = logging.getLogger(__name__)


def validate_incoming_data(data_adapter: DataAdapter, source_name: str):
    compare_result = data_adapter.call_db_function_compare(source_name)

    if compare_result[0] == 0:
        data_adapter.call_db_function_send_data(source_name)
        return True
    else:
        message = "validation failed for {pname} / {source_code}. please check"
        try:
            send_email(source_name, message)
        except Exception as ex:
            logger.error(f'Unable to send an email {source_name}, {ex}')

        return False
