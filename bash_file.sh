#!/bin/bash

# Command to unzip the downloaded_data.tgz file
tar -zxvf /home/project/airflow/dags/tolldata.tgz -C /home/project/airflow/dags/

# Extract data from the csv file and save to new file
cut -d "," -f1,2,3,4 /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv

# Extract data from the tsv file and save to new file
cut -f "Number of axles","Tollplaza id","Tollplaza code" /home/project/airflow/dags/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv

# Extract data from fixed-width file
cut -c1-3,7-9 /home/project/airflow/dags/payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv

# Consolidate data from csv_data.csv, tsv_data.csv, and fixed_width_data.csv into extracted_data.csv
paste -d',' /home/project/airflow/dags/csv_data.csv /home/project/airflow/dags/tsv_data.csv /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv

# Transform the vehicle_type field into capital letters and save it into transformed_data.csv
awk 'BEGIN {FS=OFS=","} { $5 = toupper($5) } 1' /home/project/airflow/dags/extracted_data.csv > /home/project/airflow/dags/transformed_data.csv
