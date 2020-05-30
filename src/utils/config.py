import os


class Config:
    __instance = None

    def __init__(self):
        if Config.__instance:
            raise Config.__instance
        self.load_config_from_env_variables()
        Config.__instance = self

    def load_config_from_env_variables(self):
        validate_input_data = os.environ.get("VALIDATE_INPUT_DATA", "").lower() == 'true'
        setattr(self, 'VALIDATE_INPUT_DATA', validate_input_data)

        sliding_window_days = os.environ.get("SLIDING_WINDOW_DAYS")
        sliding_window_days = int(sliding_window_days) if sliding_window_days else None
        setattr(self, 'SLIDING_WINDOW_DAYS', sliding_window_days)
        setattr(self, 'RUN_ONLY_PLUGINS', os.environ.get("RUN_ONLY_PLUGINS"))

        loglevel = os.environ.get("LOGLEVEL", "DEBUG")
        setattr(self, 'LOGLEVEL', loglevel)
        setattr(self, 'SYS_EMAIL', os.getenv("SYS_EMAIL"))
        setattr(self, 'SYS_EMAIL_PASS', os.getenv("SYS_EMAIL_PASS"))

        setattr(self, 'DB_USERNAME', os.getenv('DB_USERNAME'))
        setattr(self, 'DB_PASSWORD', os.getenv('DB_PASSWORD'))
        setattr(self, 'DB_ADDRESS', os.getenv('DB_ADDRESS'))
        setattr(self, 'DB_NAME', os.getenv('DB_NAME'))

        port = int(os.getenv('DB_PORT', 5432))
        setattr(self, 'DB_PORT', port)
        setattr(self, 'SQLITE', os.getenv('SQLITE'))
        setattr(self, 'CSV', os.getenv('CSV'))


config = Config()
