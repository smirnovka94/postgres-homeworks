"""Скрипт для заполнения данными таблиц в БД Postgres."""
import os
from datetime import datetime

import psycopg2
def read_import_data(cur):
    """
    Читаем записанные в таблицу данные
    """
    rows = cur.fetchall()
    for row in rows:
        print(row)

#Путь к папке исполняемого файла
directory = os.path.abspath(os.getcwd())
data_csv = []
password = input("Введите пароль от pgAdmin: ")

#Открываем Databas north в pgAdmin
conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="ghjtrn88"#password
)
def work_table1():
    #Чтение 1 файла в данными (customers)
    file1 = "north_data\customers_data.csv"

    filepath = os.path.join(directory, file1)
    with open(filepath) as file:
        lines = [line.rstrip() for line in file]
        i = 0
        for line in lines:
            if i == 0:
                i = 1
                continue
            datas = line.split(',')
            data1 = datas[0][1:-1]
            data2 = datas[1][1:-1]
            data3 = datas[2][1:-1]
            db = (data1,data2,data3)
            data_csv.append(db)

    #Запись данных из 1 файла в таблицу(customers)
    with psycopg2.connect(host="localhost", database="north", user="postgres",password=password) as conn:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", data_csv)
            cur.execute("SELECT * FROM customers")
            conn.commit()
            read_import_data(cur)
    conn.close()

def work_table2():
    # Чтение 2 файла в данными (employees)
    file1 = "north_data\employees_data.csv"

    filepath = os.path.join(directory, file1)
    with open(filepath) as file:
        lines = [line.rstrip() for line in file]
        i = 0
        for line in lines:
            if i == 0:
                i = 1
                continue
            datas = line.split(',"')
            data1 = int(datas[0])
            data2 = datas[1][0:-1]
            data3 = datas[2][0:-1]
            data4 = datas[3][0:-1]
            str_data5 = datas[4][0:-1]
            data5 = datetime.strptime(str_data5, "%Y-%m-%d").date()
            data6 = datas[5][0:-1]
            db = (data1, data2, data3, data4, data5, data6)
            data_csv.append(db)

    # Запись данных из 2 файла в таблицу(employees)
    with psycopg2.connect(host="localhost", database="north", user="postgres", password=password) as conn:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", data_csv)
            cur.execute("SELECT * FROM employees")
            conn.commit()
            read_import_data(cur)
    conn.close()

def work_table3():
    # Чтение 3 файла в данными (orders)
    file1 = "north_data\orders_data.csv"

    filepath = os.path.join(directory, file1)
    with open(filepath) as file:
        lines = [line.rstrip() for line in file]
        i = 0
        for line in lines:
            if i == 0:
                i = 1
                continue
            datas = line.split(',')
            data1 = int(datas[0])
            data2 = datas[1][1:-1]
            data3 = int(datas[2])
            str_data4 = datas[3][1:-1]
            data4 = datetime.strptime(str_data4, "%Y-%m-%d").date()
            data5 = datas[4][1:-1]
            db = (data1, data2, data3, data4, data5)
            data_csv.append(db)

    # Запись данных из 3 файла в таблицу(orders)
    with psycopg2.connect(host="localhost", database="north", user="postgres", password=password) as conn:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", data_csv)
            cur.execute("SELECT * FROM orders")
            conn.commit()
            read_import_data(cur)
    conn.close()

#Заполнение таблиц
work_table1()
work_table2()
work_table3()
