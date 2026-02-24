# Dockerfile for Jupyter Notebook
FROM jupyter/datascience-notebook:latest
COPY requirements.txt /tmp/
COPY .env.notebook /home/jovyan/work/.env
RUN pip install --no-cache-dir -r /tmp/requirements.txt