# fetchers-python
COVID19 data source fetchers written in Python3

Currently implemented fetchers:

| Name     | Country | [Code](https://www.nationsonline.org/oneworld/country_code_list.htm) | Source | Status | Regional levels mapping |
|----------|---------|------|--------|--------|-------------------------|
| POL_WIKI | Poland | POL  | [Wikipedia](https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Poland) | candidate | adm_area_1: NA or Voivodeship |
| ESP_MSVP | Spain | ESP  | [Ministerio de Sanidad](https://raw.githubusercontent.com/victorvicpal/COVID19_es/master/data/final_data/dataCOVID19_es.csv) | candidate | adm_area_1: Comunidades autónomas |
| WRD_ECDC | World | several  | [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) | candidate | NA |
| ZAF_DSFSI | South Africa | ZAF  | [Data Science for Social Impact research group, the University of Pretoria](https://github.com/dsfsi/covid19za) | candidate | adm_area_1: province |
| BRA_MSHM | Brazil | BRA  | [github: elhenrico](https://github.com/elhenrico/covid19-Brazil-timeseries) | candidate | adm_area_1: province|
| SWE_GM | Sweden | SWE  | [github: elinlutz](https://github.com/elinlutz/gatsby-map/tree/master/src/data/time_series) | candidate | adm_area_1: province|
| KOR_DS4C | South Korea | KOR | [Data Science for COVID-19 in South Korea](https://github.com/jihoo-kim/Data-Science-for-COVID-19) | candidate | adm_area_1: NA or Province |
| AUS_C1A | Australia | AUS | [The Real-time COVID-19 Status in Australia](https://github.com/covid-19-au/covid-19-au.github.io) | candidate | adm_area_1: NA or State |
| POR_MSDS | Portugal | POR | [Data Science for Social Good Portugal](https://github.com/dssg-pt/covid19pt-data) | candidate | adm_area_1: NA or province |
| GBR_PHTW | United Kingdom | GBR | [Coronavirus (COVID-19) UK Historical Data](https://github.com/tomwhite/covid-19-uk-data) | candidate | adm_area_1: NA or country, adm_area_2: NA or upper tier/health boards |
| CHE_OPGOV | Switzerland | CHE | [Kanton Zürich Statistisches Amt](https://github.com/openZH/covid_19) | candidate | adm_area_1: canton or Liechtenstein
| TUR_XXX | Turkey | TUR | [github:ozanerturk](https://github.com/ozanerturk/covid19-turkey-api) | candidate | adm_area_1: NA
| BEL_ZH | Belgium | BEL | [github:eschnou](https://github.com/eschnou/covid19-be/blob/master/covid19-belgium.csv) | candidate | adm_area_1: NA
| IND_COVIND | India | IND | [COVID19-India API](https://api.covid19india.org/) | candidate | adm_area_1: NA or State |
| THA_STAT | Thailand | THA | [Covid-19 Infected Situation Reports](https://covid19.th-stat.com/en/api) | candidate | adm_area_1: NA or Province |
| CAN_GOV | Canada | CAN | [Government of Canada](https://health-infobase.canada.ca/covid-19/) | candidate | adm_area_1: Province
| IDN_GTPPC | Indonesia | IDN | [Government of Indonesia - Coronavirus Disease Response Acceleration Task Force](http://covid19.go.id/) | candidate | adm_area_1: Province
| NLD_ZH | Netherlands | NLD| [CoronaWatchNL](https://github.com/J535D165/CoronaWatchNL) | candidate | adm_area_1: NA
| IRL_ZH | Ireland | IRL| [European Novel Coronavirus Outbreak - Chinese Channel (COVID19-EU-ZH)](https://github.com/covid19-eu-zh) | candidate | adm_area_1: County

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
