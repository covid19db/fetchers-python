import httplib2
import pandas as pd
from pandas import DataFrame
from apiclient import discovery


def parse_date(data: str) -> str:
    parts = data.split('.')
    if len(parts) == 2:
        return f"2020-{parts[1]}-{parts[0]}"

    raise Exception(f'Data format not supported! {data}')


class GoogleSpreadsheet:

    def __init__(self, logger, api_key):
        self.logger = logger
        self.api_key = api_key

    def get_spreadsheet_data(self, spreadsheetId, rangeName) -> DataFrame:
        self.logger.debug(f'Getting spreadsheet data')
        discovery_url = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
        service = discovery.build(
            'sheets',
            'v4',
            http=httplib2.Http(),
            discoveryServiceUrl=discovery_url,
            cache_discovery=False,
            developerKey=self.api_key)

        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheetId, range=rangeName).execute()
        values = result.get('values', [])

        if not values:
            self.logger.debug(f'No data found.')
            return None
        else:
            df = pd.DataFrame(values)
            header_row = 0
            df.columns = df.iloc[header_row]
            df = df.drop(header_row)
            return df
