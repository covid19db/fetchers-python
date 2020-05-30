import os


class Config:
    VALIDATE_INPUT_DATA = os.environ.get("VALIDATE_INPUT_DATA", "").lower() == 'true'
    SLIDING_WINDOW_DAYS = os.environ.get("SLIDING_WINDOW_DAYS", None)
    RUN_ONLY_PLUGINS = os.environ.get("RUN_ONLY_PLUGINS", None)
    LOGLEVEL = os.environ.get("LOGLEVEL", "DEBUG")
    SYS_EMAIL = os.getenv("SYS_EMAIL", None)
    SYS_EMAIL_PASS = os.getenv("SYS_EMAIL_PASS", None)

    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_ADDRESS = os.getenv('DB_ADDRESS')
    DB_NAME = os.getenv('DB_NAME')
    DB_PORT = int(os.getenv('DB_PORT', 5432))

    SQLITE = os.getenv('SQLITE')

    CSV = os.getenv('CSV')
