import mysql.connector


def connect():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='Singh@54321',
        database='data'
    )
