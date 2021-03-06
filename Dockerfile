# pull official base image
FROM python:3.9.5

RUN apt-get update -y && apt-get install -y jq

# set work directory
WORKDIR /usr/src/app

# install dependencies
RUN pip install --upgrade pip
COPY ./requirements.txt .
RUN pip install -r requirements.txt

# copy project
COPY project/* ./
