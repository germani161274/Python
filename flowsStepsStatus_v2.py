import logging
import psycopg2
from pymongo import MongoClient
import json
from datetime import datetime, timedelta
from bson import json_util

class Flow:

    conn = None

    def __init__(self):
        None

    def getStatusOfFlowV2(self):

        conn = None

        try:
            conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@10.0.1.198:27017/flow_manager?authSource=admin')
            #conn = MongoClient('mongodb://juno_controls:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            #conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['flow_manager']
            flow = db['statusOfFlow']          
        
            result = flow.find({"currentStep": {"$exists": True}})

            # Convertir el resultado a una lista de diccionarios
            result_list = list(result)

            # Guardar el resultado en un archivo de texto
            #with open('output.json', 'w') as f:
                #json.dump(result_list, f, default=str)

            return result_list
           
        except Exception as exception: 
            logging.error("Error getflowsToInitilize - %s." % str(exception))

    def getLenders(self):
        conn = None

        try:
            conn = psycopg2.connect(
                dbname='juno',
                user='juno_controls',
                password='601Jwk66OIYH18jll711Nd2wSklOPI6o',
                host='juno-dev-database2.c5ebb2jx7zvm.us-east-1.rds.amazonaws.com',
                port='5432'
            )

            cursor = conn.cursor()
            cursor.execute("select id, description from lender.lender_v2")
 
            lender_data = cursor.fetchall()

            cursor.close()

            return lender_data  # Devolver el nombre del archivo donde se guardaron los resultados

        except psycopg2.Error as e:
            logging.error("Error getQtyCountry_v2 - %s." % str(e))
            
        finally:
            if conn is not None:
                conn.close()