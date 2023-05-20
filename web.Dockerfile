FROM python:3.11-buster
LABEL maintainer="chilledgeek@gmail.com"

USER root
COPY . .
RUN pip install --upgrade pip setuptools wheel poetry
RUN POETRY_VIRTUALENVS_IN_PROJECT=true \
    poetry install

USER 1001
CMD [ "poetry", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000" ]