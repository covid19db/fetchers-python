# fetchers-python
COVID19 data source fetchers written in Python3

Currently implemented fetchers:

| Name     | Country | [Code](https://www.nationsonline.org/oneworld/country_code_list.htm) | Source | Status | Regional levels mapping |
|----------|---------|------|--------|--------|-------------------------|
| POL_WIKI | Poland | POL  | [Wikipedia](https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Poland) | candidate | adm_area_1: NA or Voivodeship |
| ESP_MSVP | Spain | ESP  | [Ministerio de Sanidad](https://raw.githubusercontent.com/victorvicpal/COVID19_es/master/data/final_data/dataCOVID19_es.csv) | candidate | adm_area_1: Comunidades autónomas |
| WRD_ECDC | World | several  | [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) | candidate | NA |

Explanation of status:
- Draft: being developed, should not be tested yet
- Candidate: development complete, being tested on a private test database
- Release: tested, data are fed into the official public database


## Database structure

Infections table: see https://covid19db.github.io/data.html

## Develop and test

You need:
- Python3
- Running instance of a PostgreSQL database
- (optional) Docker


## Run locally

1. Add the `DB_ADDRESS`, `DB_PORT`, `DB_NAME`, `DB_USERNAME` and `DB_PASSWORD` environment variables
2. Install requirements ```pip install -r requirements.txt```
3. Run fetcher `python3 ./main.py`

## Run locally using Docker

1. Add the `DB_ADDRESS`, `DB_PORT`, `DB_NAME`, `DB_USERNAME` and `DB_PASSWORD` environment variables
2. Run ```docker-compose up```

## Contribute

**We need fetchers!**

Create a fetcher for a country that is not listed yet and send us a pull request.
Use only official sources, or sources derived from official sources.

You can find example code for fetcher in /src/plugins/EXAMPLE/example_fetcher.py
