#
# Validations-API image
#

# pull base image
FROM python:3.8.5-slim


# custom packages
RUN apt-get update && apt-get install -y \
    build-essential \
    make \
    gcc \
    python3-dev

# make local dir
RUN mkdir opt/validation-api

# set "validation-api" as the workdir directory from wich CMD, RUN, ADD references
WORKDIR /opt/validation-api

# copy all files in this directory
ADD . .

# pip install all requirements and upgrade pip
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Expose port 5000
EXPOSE 5000
EXPOSE 8000

# start app server 
CMD python main.py runserver