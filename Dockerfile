FROM python:3.7.5
RUN apt-get update && \
    apt-get -qq -y install  libxpm4 libxrender1 libgtk2.0-0 libnss3\
       libgconf-2-4  libpango1.0-0 libxss1 libxtst6 fonts-liberation\
       libappindicator1 xdg-utils

RUN apt-get -y install \
               xvfb gtk2-engines-pixbuf \
               xfonts-cyrillic xfonts-100dpi xfonts-75dpi xfonts-base xfonts-scalable \
               imagemagick x11-apps zip

#download and install chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install

# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

### Get Java via the package manager
RUN apt-get update && \
apt-get install -y openjdk-11-jdk && \
apt-get install -y ant && \
apt-get clean;

# set display port to avoid crash
ENV DISPLAY=:99

RUN pip install --upgrade pip setuptools
ADD ./src /src
WORKDIR /src

RUN pip install -r /src/requirements.txt

CMD ["python3", "main.py"]