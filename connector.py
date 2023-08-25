import json
import os
import mysql.connector
import requests
import datetime
import redshift_connector

db=os.getenv(DB)

class Querier:
    def __init__(self, db):
        if db=='REDSHIFT':

    @staticmethod
    def fetch_service_data_mysql(query) -> list:
        mydb = mysql.connector.connect(
            host=os.getenv('COMMON_DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME_SERVICE')
        )
        cursor = mydb.cursor()
        cursor.execute(query)
        query_result: tuple = cursor.fetchall()
        fields = [field[0] for field in cursor.description]
        t_result = [dict(zip(fields,
                             map(lambda x: x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x.strftime(
                                 '%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x, record))) for record
                    in query_result]
        return t_result

    @staticmethod
    def fetch_groot_data_mysql(query) -> list:
        mydb = mysql.connector.connect(
            host=os.getenv('COMMON_DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME_GROOT')
        )
        cursor = mydb.cursor()
        cursor.execute(query)
        query_result: tuple = cursor.fetchall()
        fields = [field[0] for field in cursor.description]
        t_result = [dict(zip(fields,
                             map(lambda x: x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x.strftime(
                                 '%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x, record))) for record
                    in query_result]
        return t_result

    @staticmethod
    def fetch_sec_groot_data_mysql(query) -> list:
        mydb = mysql.connector.connect(
            host=os.getenv('COMMON_DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME_SEC_GROOT')
        )
        cursor = mydb.cursor()
        cursor.execute(query)
        query_result: tuple = cursor.fetchall()
        fields = [field[0] for field in cursor.description]
        t_result = [dict(zip(fields,
                             map(lambda x: x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x.strftime(
                                 '%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x, record))) for record
                    in query_result]
        return t_result

    @staticmethod
    def fetch_mav_data_mysql(query) -> list:
        mydb = mysql.connector.connect(
            host=os.getenv('COMMON_DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME_MAV')
        )
        cursor = mydb.cursor()
        cursor.execute(query)
        query_result: tuple = cursor.fetchall()
        fields = [field[0] for field in cursor.description]
        t_result = [dict(zip(fields,
                             map(lambda x: x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x.strftime(
                                 '%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x, record))) for record
                    in query_result]
        return t_result
