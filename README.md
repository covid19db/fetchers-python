
# Oxford COVID-19 (OxCOVID19) Data Fetcher Repository 

This is the data fetcher-python repository for the **OxCOVID19 Database**, a large, single-centre database consisting of information relating to COVID-19 pandemic. 

**OxCOVID19 Project** [https://covid19.eng.ox.ac.uk/](https://covid19.eng.ox.ac.uk/)  aims to increase our understanding of the Covid-19 pandemic and elaborate possible strategies to reduce the impact on the society through the combined power of Statistical and Mathematical Modelling, and Machine Learning techniques.
OxCOVID19 data source fetchers written in Python3.

Currently implemented fetchers:

| Name     | Country | [Country Code](https://www.nationsonline.org/oneworld/country_code_list.htm) | Data source | Status | Regional levels mapping | Terms of Use |
|----------|---------|------|--------|--------|-------------------------|--------|
| GOOGLE_MOBILITY | World | several  | [COVID-19 Community Mobility Reports - Google](https://www.google.com/covid19/mobility/) | release | adm_area_1, adm_area_2: depending on the country |
| APPLE_MOBILITY | World | several  | [COVID‑19 Mobility Trends Reports - Apple](https://www.apple.com/covid19/mobility) | release | adm_area_1, adm_area_2: depending on the country |
| GOVTRACK | World | several  | [Oxford COVID-19 Government Response Tracker](https://covidtracker.bsg.ox.ac.uk/) | release | NA  |
| WEATHER | World | several  | [MET Informatics Lab](https://www.informaticslab.co.uk/) | release | adm_area_1, adm_area_2, adm_area_3: depending on the country |
| WRD_ECDC | World | several  | [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) | release | NA |
| POL_WIKI | Poland | POL  | [Wikipedia](https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Poland) | release | adm_area_1: NA or voivodeship |
| ESP_MSVP | Spain | ESP  | [Ministerio de Sanidad](https://raw.githubusercontent.com/victorvicpal/COVID19_es/master/data/final_data/dataCOVID19_es.csv) | release | adm_area_1: comunidades autónomas |
| ZAF_DSFSI | South Africa | ZAF  | [Data Science for Social Impact research group, the University of Pretoria](https://github.com/dsfsi/covid19za) | release | adm_area_1: province |
| BRA_MSHM | Brazil | BRA  | [github: elhenrico](https://github.com/elhenrico/covid19-Brazil-timeseries) | release | adm_area_1: province|
| SWE_GM | Sweden | SWE  | [github: elinlutz](https://github.com/elinlutz/gatsby-map/tree/master/src/data/time_series) | release | adm_area_1: province|
| KOR_DS4C | South Korea | KOR | [Data Science for COVID-19 in South Korea](https://github.com/jihoo-kim/Data-Science-for-COVID-19) | release | adm_area_1: NA or province |
| AUS_C1A | Australia | AUS | [The Real-time COVID-19 Status in Australia](https://github.com/covid-19-au/covid-19-au.github.io) | release | adm_area_1: NA or state |
| POR_MSDS | Portugal | POR | [Data Science for Social Good Portugal](https://github.com/dssg-pt/covid19pt-data) | release | adm_area_1: NA or province |
| GBR_PHTW | United Kingdom | GBR | [Coronavirus (COVID-19) UK Historical Data](https://github.com/tomwhite/covid-19-uk-data) | release | adm_area_1: NA or country, adm_area_2: NA or upper tier/health boards |
| CHE_OPGOV | Switzerland | CHE | [Kanton Zürich Statistisches Amt](https://github.com/openZH/covid_19) | release | adm_area_1: canton
| TUR_MHOE | Turkey | TUR | [github:ozanerturk](https://github.com/ozanerturk/covid19-turkey-api) | release | adm_area_1: NA
| BEL_WY | Belgium | BEL | [github:eschnou](https://github.com/eschnou/covid19-be/blob/master/covid19-belgium.csv) | release | adm_area_1: NA
| IND_COVIND | India | IND | [COVID19-India API](https://api.covid19india.org/) | release | adm_area_1: NA or state |
| CAN_GOV | Canada | CAN | [Government of Canada](https://health-infobase.canada.ca/covid-19/) | release | adm_area_1: province
| IDN_GTPPC | Indonesia | IDN | [Government of Indonesia - Coronavirus Disease Response Acceleration Task Force](http://covid19.go.id/) | release | adm_area_1: province
| NLD_WY | Netherlands | NLD| [CoronaWatchNL](https://github.com/J535D165/CoronaWatchNL) | release | adm_area_1: NA
| LAT_DSRP | Latin America | several | [Latin America Covid-19 Data Repository by DSRP](https://github.com/DataScienceResearchPeru/covid-19_latinoamerica) | release | adm_area_1: subdivision |
| EU_ZH | Norway | NOR | [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | release | adm_area_1: county |
| EU_ZH | Slovenia | SVN | [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | release | adm_area_1: NA |
| EU_ZH | Sweden | SWE | [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | release | adm_area_1: province |
| EU_ZH | Poland | POL | [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | release | adm_area_1: voivodeship |
| EU_ZH | Ireland | IRL | [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | release | adm_area_1: county |
| EU_ZH | Hungary | HUN | [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | release | adm_area_1: NA |
| EU_ZH | Germany | DEU | [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | release | adm_area_1: state |
| EU_ZH | Austria | AUT | [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | release | adm_area_1: state |
| EU_ZH | Czech Republic | CZE | [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | release | adm_area_1: region |
| NGA_SO | Nigeria | NGA | [Covid-19 Nigeria API](https://github.com/sink-opuba/covid-19-nigeria-api) | release | adm_area_1: state |
| NGS_CDC | Nigeria | NGA | [Nigeria Centre for Disease Control](https://covid19.ncdc.gov.ng/) | release | adm_area_1: state |
| RUS_GOV | Russia | RUS | [Russian Government](https://xn--80aesfpebagmfblc0a.xn--p1ai/) | release | adm_area_1: federal subjects |
| ITA_PC | Italy | ITA | [Protezione Civile](https://github.com/pcm-dpc/COVID-19) | release | adm_area_1: [italian regions](https://en.wikipedia.org/wiki/Regions_of_Italy), adm_area_2: [italian provinces](https://en.wikipedia.org/wiki/Provinces_of_Italy)|
| ITA_PCDM | Italy | ITA | [Davide Magno, from Protezione Civile](https://github.com/DavideMagno/ItalianCovidData) | release | adm_area_1: [italian region](https://en.wikipedia.org/wiki/Regions_of_Italy) |
| USA_NYT | United States | USA  | [New York Times](https://github.com/nytimes/covid-19-data) | release | adm_area_1: US State, adm_area_2: county (exception is New York City, which includes more counties) |
| FRA_SPFCG | France | FRA | [Cedric Guadalupe from Santé Publique France](https://github.com/cedricguadalupe/FRANCE-COVID-19) | release | adm_area_1: [France "régions"](https://en.wikipedia.org/wiki/Regions_of_France) |
| DEU_JPGG | Germany | DEU | [Jan-Philip Gehrcke, from the Public Health Offices (Gesundheitsaemter)](https://gehrcke.de/2020/03/covid-19-sars-cov-2-resources/) | release | adm_area_1: [German "länder"](https://en.wikipedia.org/wiki/States_of_Germany) |
| PAK_GOV | Pakistan | PAK | [Government of Pakistan](http://covid.gov.pk/) | release | adm_area_1: Province |
| GBR_PHE | United Kingdom | GBR | [Public Health England](https://coronavirus.data.gov.uk/) | release | adm_area_3: English lower tier local authority |
| GBR_PHW | United Kingdom | GBR | [Public Health Wales](https://public.tableau.com/profile/public.health.wales.health.protection#!/vizhome/RapidCOVID-19virology-Public/Headlinesummary) | candidate | adm_area_2: Welsh health board for deaths, local authority for tests |
| SWE_SIR | Sweden | SWE | [Svenska Intensivvårdsregistret (SIR)](https://portal.icuregswe.org/siri/report/corona.inrapp) | release | adm_area_1: [Swedish counties (Län)](https://en.wikipedia.org/wiki/Counties_of_Sweden) |
| MYS_MHYS | Malysia | MYS | [ynshung](https://github.com/ynshung/covid-19-malaysia) | release | adm_area_1: NA or province|
| JPN_C1JACD | Japan | JPN | [COVID-19 Japan Anti-Coronavirus Dashboard](https://github.com/code4sabae/covid19) | release | adm_area_1: prefecture |
| USA_CTP | United States | USA | [The COVID Tracking Project](https://covidtracking.com/api) | release | adm_area_1: state |
| EU_ZH | Belgium | BEL| [Novel Coronavirus Outbreak in Europe - Chinese language](https://github.com/covid19-eu-zh/covid19-eu-data) | tested | adm_area_1: region; adm_area_2: province |


Explanation of status:
- Draft: being developed, should not be tested yet
- Candidate: development complete, being tested on a private test database
- Release: tested, data are fed into the official public database


## Database structure

See https://covid19db.github.io/data.html

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
