FROM jupyter/pyspark-notebook:latest

COPY requirements.txt /requirements.txt

USER root

RUN python -m pip install --upgrade pip && pip install --no-cache-dir -r /requirements.txt