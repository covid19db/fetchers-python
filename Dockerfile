FROM python:3.7.5
RUN apt-get update && apt-get -y install curl

RUN pip install --upgrade pip setuptools
ADD ./src /src
WORKDIR /src

RUN pip install -r /src/requirements.txt

CMD ["python3", "main.py"]