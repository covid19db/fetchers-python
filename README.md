# Oxford COVID-19 (OxCOVID19) Data Fetcher Repository

This is the data fetcher-python repository for the **OxCOVID19 Database**, a large, single-centre, multimodal database consisting of information relating to COVID-19 pandemic.

**OxCOVID19 Project** [https://covid19.eng.ox.ac.uk/](https://covid19.eng.ox.ac.uk/)  aims to increase our understanding of the Covid-19 pandemic and elaborate possible strategies to reduce the impact on the society through the combined power of Statistical and Mathematical Modelling, and Machine Learning techniques.
OxCOVID19 data source fetchers written in Python3.

---

__Cite as:__ Adam Mahdi, Piotr Błaszczyk, Paweł Dłotko, Dario Salvi, Tak-Shing Chan, John Harvey, Davide Gurnari, Yue Wu, Ahmad Farhat, Niklas Hellmer, Alexander Zarebski, Lionel Tarassenko,
Oxford COVID-19 Database: multimodal data repository for understanding the global impact of COVID-19. University of Oxford, 2020 [medRxiv doi: 10.1101/2020.08.18.20177147](https://doi.org/10.1101/2020.08.18.20177147).

---

## List of fetchers

| Name            | Country | Data source | Terms of Use |
|-----------------|---------|------|--------|
| GOOGLE_MOBILITY | World   | [Google COVID-19 Community Mobility Reports](https://www.google.com/covid19/mobility/) | [Attribution required](https://www.google.com/help/terms_maps/?hl=en) |
| APPLE_MOBILITY  | World   | [COVID‑19 Mobility Trends Reports - Apple](https://www.apple.com/covid19/mobility) |  |
| GOVTRACK        | World   | [Oxford COVID-19 Government Response Tracker](https://covidtracker.bsg.ox.ac.uk/) |   |
| WEATHER         | World   | [MET Informatics Lab](https://www.informaticslab.co.uk/) |  |
| AUS_C1A | Australia | [covid-19-au.com](https://github.com/covid-19-au/covid-19-au.github.io) | Strictly for educational and academic research purposes |
| BEL_LE | Belgium | [Laurent Eschenauer](https://github.com/eschnou/covid19-be/blob/master/covid19-belgium.csv) | CC0 1.0 Universal (CC0 1.0) Public Domain Dedication |
| BEL_SCI | Belgium | [Epistat](https://epistat.wiv-isp.be/Covid/) |  |
| BRA_MSHM | Brazil | [Ministério da Saúde (Brasil)](https://github.com/elhenrico) | CC0 1.0 Universal |
| CAN_GOV | Canada | [Government of Canada](https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection.html) | Attribution required, non-commercial use |
| CHE_OPGOV | Switzerland | [Swiss Cantons and the Principality of Liechtenstein](https://github.com/openZH/covid_19) | CC 4.0 |
| CHN_ICL | Mainland China | [MRC Centre for Global Infectious Disease Analysis](https://github.com/mrc-ide/covid19_mainland_China_report) | CC BY NC ND 4.0 |
| DEU_JPGG | Germany | [Jan-Philip Gehrcke](https://github.com/jgehrcke/covid-19-germany-gae) | MIT |
| ESP_ISCIII | Spain | [Instituto de Salud Carlos III](https://cnecovid.isciii.es/covid19/) |  |
| ESP_MS | Spain | [Ministerio de Sanidad, Consumo y Bienestar Social](https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm) | "Desnaturalización" prohibited, citation required |
| ESP_MSVP | Spain | [Ministerio de Sanidad, Consumo y Bienestar Social](https://raw.githubusercontent.com/victorvicpal/COVID19_es/master/data/final_data/dataCOVID19_es.csv) | Apache License 2.0 |
| EU_ZH | Austria | [Covid19-eu-zh](https://github.com/covid19-eu-zh/covid19-eu-data/blob/master/dataset/covid-19-at.csv) | MIT |
| EU_ZH | Czech republic | [Covid19-eu-zh](https://github.com/covid19-eu-zh/covid19-eu-data/blob/master/dataset/covid-19-at.csv) | MIT |
| EU_ZH | Hungary | [Covid19-eu-zh](https://github.com/covid19-eu-zh/covid19-eu-data/blob/master/dataset/covid-19-at.csv) | MIT |
| EU_ZH | Ireland | [Covid19-eu-zh](https://github.com/covid19-eu-zh/covid19-eu-data/blob/master/dataset/covid-19-at.csv) | MIT |
| EU_ZH | Germany | [Covid19-eu-zh](https://github.com/covid19-eu-zh/covid19-eu-data/blob/master/dataset/covid-19-at.csv) | MIT |
| EU_ZH | Norway | [Covid19-eu-zh](https://github.com/covid19-eu-zh/covid19-eu-data/blob/master/dataset/covid-19-at.csv) | MIT |
| EU_ZH | Poland | [Covid19-eu-zh](https://github.com/covid19-eu-zh/covid19-eu-data/blob/master/dataset/covid-19-at.csv) | MIT |
| EU_ZH | Sweden | [Covid19-eu-zh](https://github.com/covid19-eu-zh/covid19-eu-data/blob/master/dataset/covid-19-at.csv) | MIT |
| EU_ZH | Slovenia | [Covid19-eu-zh](https://github.com/covid19-eu-zh/covid19-eu-data/blob/master/dataset/covid-19-at.csv) | MIT |
| FRA_SPF | France | [Santé publique France](https://www.data.gouv.fr/fr/datasets/donnees-hospitalieres-relatives-a-lepidemie-de-covid-19/) | License Ouverte/Open License 2.0 |
| FRA_SPFCG | France | [Cédric Guadalupe](https://github.com/cedricguadalupe/FRANCE-COVID-19) | GPL 3.0 |
| GBR_NIDH | UK - Northern Ireland | [Department of Health (Northern Ireland)](https://app.powerbi.com/view?r=eyJrIjoiZGYxNjYzNmUtOTlmZS00ODAxLWE1YTEtMjA0NjZhMzlmN2JmIiwidCI6IjljOWEzMGRlLWQ4ZDctNGFhNC05NjAwLTRiZTc2MjVmZjZjNSIsImMiOjh9) |  |
| GBR_PHE | UK - England  | [Public Health England](https://coronavirus.data.gov.uk/downloads/csv/coronavirus-cases_latest.csv) | Open Government Licence v3.0 |
| GBR_PHS | UK - Scotland | [Scottish Government](https://github.com/DataScienceScotland/COVID-19-Management-Information) | GPL 3.0 |
| GBR_PHTW | UK -  | [Tom White](https://github.com/tomwhite/covid-19-uk-data) | The Unlicense |
| GBR_PHW | UK - Wales | [Public Health Wales](https://public.tableau.com/profile/public.health.wales.health.protection#!/vizhome/RapidCOVID-19virology-Public/Headlinesummary) | Open Government Licence v3.0 |
| IDN_GTPPC | Indonesia | [Satuan Tugas Penanganan COVID-19 (Indonesia)](https://covid19.go.id/) | Standard "all rights reserved" notice. No licensing information. |
| IND_COVIND | India | [COVID-19 India Org Data Operations Group](https://api.covid19india.org/) | GPL 3.0 |
| IRL_HSPC | Ireland | [Health Protection Surveillance Centre](https://covid19ireland-geohive.hub.arcgis.com/datasets/4779c505c43c40da9101ce53f34bb923_0?geometry=-16.624%2C52.290%2C0.767%2C54.580) | No license specified' - T&C state not for commercial use. T&C very focussed on OSI information, which is mapping information that we do not use |
| ITA_PC | Italy | [Protezione Civile](https://github.com/pcm-dpc/COVID-19) | CC BY 4.0 |
| ITA_PCDM | Italy | [Protezione Civile](https://github.com/DavideMagno/ItalianCovidData) | CC0 1.0 Universal |
| JPN_C1J | Japan | [Shane Reustle](https://github.com/reustle/covid19japan-data) |  |
| JPN_C1JACD | Japan | [COVID-19 Japan Anti-Coronavirus Dashboard](https://github.com/code4sabae/covid19) | CC BY |
| KOR_DS4C | South Korea | [Jihoo Kim](https://github.com/jihoo-kim/Data-Science-for-COVID-19) | CC BY-NC-SA 4.0 |
| LAT_DSRP | Argentina | [Data Science Research Peru](https://github.com/DataScienceResearchPeru/covid-19_latinoamerica) | CC BY-NC-SA 4.0 |
| LAT_DSRP | Brazil | [Data Science Research Peru](https://github.com/DataScienceResearchPeru/covid-19_latinoamerica) | CC BY-NC-SA 4.0 |
| LAT_DSRP | Chile | [Data Science Research Peru](https://github.com/DataScienceResearchPeru/covid-19_latinoamerica) | CC BY-NC-SA 4.0 |
| LAT_DSRP | Colombia | [Data Science Research Peru](https://github.com/DataScienceResearchPeru/covid-19_latinoamerica) | CC BY-NC-SA 4.0 |
| LAT_DSRP | Dominican Republic | [Data Science Research Peru](https://github.com/DataScienceResearchPeru/covid-19_latinoamerica) | CC BY-NC-SA 4.0 |
| LAT_DSRP | Ecuador | [Data Science Research Peru](https://github.com/DataScienceResearchPeru/covid-19_latinoamerica) | CC BY-NC-SA 4.0 |
| LAT_DSRP | Mexico | [Data Science Research Peru](https://github.com/DataScienceResearchPeru/covid-19_latinoamerica) | CC BY-NC-SA 4.0 |
| LAT_DSRP | Peru | [Data Science Research Peru](https://github.com/DataScienceResearchPeru/covid-19_latinoamerica) | CC BY-NC-SA 4.0 |
| MYS_MHYS | Malaysia | [Young Shung](https://github.com/ynshung/covid-19-malaysia) | Public Domain Dedication and License v1.0 |
| NGA_CDC | Nigeria | [Nigeria Centre for Disease Control](https://covid19.ncdc.gov.ng/) |  |
| NGA_HERA | Nigeria | [Humanitarian Emergency Response Africa](https://data.humdata.org/dataset/nigeria_covid19_subnational) | CC BY |
| NGA_SO | Nigeria | [Nigeria Centre for Disease Control](https://covidnigeria.herokuapp.com/) | No licensing information. |
| NLD_CW | Netherlands | [Jonathan de Bruin](https://github.com/J535D165/CoronaWatchNL/tree/master/data) | CC0 |
| PAK_GOV | Pakistan | [National Information Technology Board, Government of Pakistan](https://datastudio.google.com/u/0/reporting/1PLVi5amcc_R5Gh928gTE8-8r8-fLXJQF/page/kyNJB) |  |
| POL_WIKI | Poland | [Wikipedia](https://en.wikipedia.org/wiki/Coronavirus_disease_2019) | CC BY-SA |
| PRT_MSDS | Portugal | [Data Science for Social Good Portugal](https://github.com/dssg-pt/covid19pt-data) | MIT |
| RUS_GOV | Russia | [Government of Russia](https://xn--80aesfpebagmfblc0a.xn--p1ai/information/) |  |
| SWE_GM | Sweden | [Elin Lütz](https://github.com/elinlutz/gatsby-map/tree/master/src/data/time_series) | MIT |
| SWE_SIR | Sweden | [Svenska Intensivvårdsregistret](https://portal.icuregswe.org/siri/report/corona.inrapp) | Public data may be used, but the source must be reported: Svenska Intensivvårdsregistret https://portal.icuregswe.org/siri/report/corona.inrapp (2020) |
| THA_STAT | Thailand | [Open Government Data of Thailand](https://covid19.th-stat.com/en/api) | DGA Open Government License |
| TUR_MHOE | Turkey | [Ministry of Health (Turkey)](https://github.com/ozanerturk/covid19-turkey-api) | MIT |
| USA_CTP | United States | [COVID Tracking Project](https://covidtracking.com/api) | CC BY-NC-4.0 |
| USA_NYT | USA, county | [The New York Times](https://github.com/nytimes/covid-19-data) | Attribution required, non-commercial use |
| WRD_ECDC | World | [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) | Attribution required |
| WRD_WHO | World | [World Health Organization](https://covid19.who.int/) |  |
| WRD_WHOJHU | World | [Center for Systems Science and Engineering, Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19) | CC BY 4.0 |
| ZAF_DSFSI | South Africa | [Data Science for Social Impact Research Group, University of Pretoria](https://github.com/dsfsi/covid19za) | MIT |
| LBN_GOV | Lebanon | [Ministry of Information (Lebanon)](https://corona.ministryinfo.gov.lb/) |  |
| SAU_GOV | Saudi Arabia | [Ministry of Health (Saudi Arabia)](https://covid19.moh.gov.sa/) |  |
| IRQ_GOV | Iraq | [World Health Organization](https://app.powerbi.com/view?r=eyJrIjoiNjljMDhiYmItZTlhMS00MDlhLTg3MjItMDNmM2FhNzE5NmM4IiwidCI6ImY2MTBjMGI3LWJkMjQtNGIzOS04MTBiLTNkYzI4MGFmYjU5MCIsImMiOjh9) |  |


## Database structure

See [covid19.eng.ox.ac.uk](https://covid19.eng.ox.ac.uk/database.html)

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
