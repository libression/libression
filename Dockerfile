FROM python:slim-buster
LABEL maintainer="chilledgeek@gmail.com"

USER root
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

USER 1001
CMD [ "python", "./main.py" ]