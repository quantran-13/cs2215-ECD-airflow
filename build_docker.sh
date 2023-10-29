#!/bin/bash

docker compose down --volumes --remove-orphans

sudo rm -rf postgres-db-volume

# Ai ma rmi chi, dong service nang + package nang, nen up-down compose thoi
# docker rmi airflow-deamon:latest

mkdir -p postgres-db-volume
sudo chmod 777 -R postgres-db-volume

docker compose up --remove-orphans
# docker-compose run --rm airflow-webserver # airflow variables import ./dags/config/const.json
