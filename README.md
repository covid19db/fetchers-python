
# Oxford COVID-19 (OxCOVID19) Data Fetcher Repository 

This is the data fetcher-python repository for the **OxCOVID19 Database**, a large, single-centre, multimodal database consisting of information relating to COVID-19 pandemic. 

**OxCOVID19 Project** [https://covid19.eng.ox.ac.uk/](https://covid19.eng.ox.ac.uk/)  aims to increase our understanding of the Covid-19 pandemic and elaborate possible strategies to reduce the impact on the society through the combined power of Statistical and Mathematical Modelling, and Machine Learning techniques.
OxCOVID19 data source fetchers written in Python3.


## List of fetchers

Available [here](https://docs.google.com/spreadsheets/d/1GHN1b_uZq4swxyN6dhPqjmjLtYGhONZ_yWlJoleTCjY/edit?usp=sharing)

## Database schema

See [here](https://docs.google.com/spreadsheets/d/1GYyxsod4DG9aEN28nm3L6XWgnlZMWrBXtEzT6nuZUtQ/edit?usp=sharing)

## Develop and test

You need:
- Python3
- (optional) Running instance of a PostgreSQL database
- (optional) Docker


## Run locally

1. Add the `DB_ADDRESS`, `DB_PORT`, `DB_NAME`, `DB_USERNAME` and `DB_PASSWORD` environment variables
2. Install requirements ```pip install -r requirements.txt```
3. Run fetcher `python3 ./main.py`

## Run locally using Docker

1. Add the `STAGE=test`, `DB_ADDRESS`, `DB_PORT`, `DB_NAME`, `DB_USERNAME` and `DB_PASSWORD` environment variables
2. Run ```docker-compose up```

---

__Cite as:__ Adam Mahdi, Piotr Błaszczyk, Paweł Dłotko, Dario Salvi, Tak-Shing Chan, John Harvey, Davide Gurnari, Yue Wu, Ahmad Farhat, Niklas Hellmer, Alexander Zarebski, Lionel Tarassenko,
Oxford COVID-19 Database: multimodal data repository for understanding the global impact of COVID-19.University of Oxford, 2020.

## Environmental variables

| Variable name       | Default value |             Description      |
----------------------|---------|------------------------------|
| DB_USERNAME         |         | Postgres database adapter user name |
| DB_PASSWORD         |         | Postgres database adapter password |
| DB_ADDRESS          |         | Postgres database adapter address |
| DB_NAME             |         | Postgres database adapter name |
| DB_PORT             | 5432    | Postgres database adapter port |
| SQLITE              |         | SQLITE adapter file path  |
| CSV                 |         | CSV adapter file path |
| VALIDATE_INPUT_DATA | False   | Validate input data |
| SLIDING_WINDOW_DAYS |         | Sliding window, number of days in the past to process |
| RUN_ONLY_PLUGINS    | ALL     | Run selected plugins from given list, run all plugins if empty |
| LOGLEVEL            | DEBUG   | Log level |
| SYS_EMAIL           |         | Notifications SMTP username |
| SYS_EMAIL_PASS      |         | Notifications SMTP password |

## Contribute

**We need fetchers!**

Create a fetcher for a country that is not listed yet and send us a pull request.
Use only official sources, or sources derived from official sources.

You can find example code for fetcher in /src/plugins/_EXAMPLE/example_fetcher.py
