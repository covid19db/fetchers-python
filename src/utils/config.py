import os


class Config:
    __instance = None

    def __init__(self):
        if Config.__instance:
            raise Config.__instance
        self.load_config_from_env_variables()
        Config.__instance = self

    def load_env_variable(self, name, default=None, fun=None):
        value = os.environ.get(name, default)
        if fun:
            value = fun(value)
        setattr(self, name, value)
        if value:
            if 'pass' in name.lower():
                value = '*****'
            print(f'Env {name} = {value}')

    def load_config_from_env_variables(self):
        self.load_env_variable("VALIDATE_INPUT_DATA", "", fun=lambda x: x.lower() == 'true')
        self.load_env_variable("SLIDING_WINDOW_DAYS", fun=lambda x: int(x) if x else None)
        self.load_env_variable("RUN_ONLY_PLUGINS")
        self.load_env_variable("LOGLEVEL", "DEBUG")
        self.load_env_variable("SYS_EMAIL")
        self.load_env_variable("SYS_EMAIL_PASS")
        self.load_env_variable("DB_USERNAME")
        self.load_env_variable("DB_PASSWORD")
        self.load_env_variable("DB_ADDRESS")
        self.load_env_variable("DB_NAME")
        self.load_env_variable("DB_PORT", 5432, fun=lambda x: int(x))
        self.load_env_variable("SQLITE")
        self.load_env_variable("CSV")


config = Config()
