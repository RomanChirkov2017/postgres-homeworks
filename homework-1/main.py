"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
import os

current_directory = os.getcwd()


conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='Mmclegnum157',
    options='-c search_path=public',
    client_encoding='UTF8'
)

cur = conn.cursor()
with open(current_directory + '\\north_data\\' + "customers_data.csv", 'r') as file:
    next(file)
    reader = csv.reader(file)
    for row in reader:
        cur.execute('INSERT INTO customers (customer_id, company_name, contact_name)'
                    'VALUES (%s, %s, %s)', (row[0], row[1], row[2]))

with open(current_directory + '\\north_data\\' + "employees_data.csv", 'r') as file:
    next(file)
    reader = csv.reader(file)
    for row in reader:
        cur.execute('INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes)'
                    'VALUES (%s, %s, %s, %s, %s, %s)', (row[0], row[1], row[2], row[3], row[4], row[5]))

with open(current_directory + '\\north_data\\' + "orders_data.csv", 'r') as file:
    next(file)
    reader = csv.reader(file)
    for row in reader:
        cur.execute('INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city)'
                    'VALUES (%s, %s, %s, %s, %s)', (row[0], row[1], row[2], row[3], row[4]))

conn.commit()
cur.close()
conn.close()

