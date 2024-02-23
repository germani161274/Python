import logging
from click import UUID
from pymongo import MongoClient
import json
from datetime import datetime, timedelta
from bson import Binary, json_util
import uuid
import re
from uuid import uuid4
from bson.binary import UuidRepresentation


class Lender:

    conn = None

    def __init__(self):
        None

    def getLender(self, lenderId):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Lenders?authSource=admin&retryWrites=false')

            db = conn['Lenders']
            lenders = db['lenders']          

            explicit_binary = Binary.from_uuid(uuid4(), UuidRepresentation.STANDARD)

            lenderId = "d45e3dd8-46a5-42e4-b699-bde3d871b1dc"
            lenderId = uuid4()
            result = lenders.count_documents({'_id':lenderId} )
   
            return result

        except Exception as exception: 
            print(str(exception))
            logging.error("Error getflowsToInitilize - %s." % str(exception))

