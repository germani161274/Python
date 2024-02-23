import logging
import json
import os
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import json_util


class MongoConn:

    conn = None

    def __init__(self):
        None

    def deleteCustomer(self, orgId, documentNumber):
        conn = None
        try:
            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Customers']
            customers = db['customers']

            # Eliminar múltiples documentos basados en la fecha
            organizationCustomerId = orgId+"-"+documentNumber
            result = customers.delete_many({ "$and": [ {"customerInformation.organizationCustomerId":organizationCustomerId}]})

            return result.deleted_count  # Devolver el número de documentos eliminados
        
        except Exception as exception:
            logging.error("Error deleteBalancesByDate - %s." % str(exception))

    def deleteExternalData(self, orgId, documentNumber):
        conn = None
        try:
            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Customers']
            externalData = db['externalData']
            organizationCustomerId = orgId+"-"+documentNumber
            result = externalData.delete_many({ "$and": [ {"organizationCustomerId":organizationCustomerId}]})
            return result.deleted_count  # Devolver el número de documentos eliminados


        except Exception as exception:
            logging.error("Error deleteBalancesByDate - %s." % str(exception))

    def deleteAdditionalInformation(self, orgId, documentNumber):
        conn = None
        try:
            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
           
            db = conn['Customers']
            additionalInformation = db['additionalInformation']
            organizationCustomerId = orgId+"-"+documentNumber
            result = additionalInformation.delete_many({ "$and": [ {"organizationCustomerId":organizationCustomerId}]})
            return result.deleted_count  # Devolver el número de documentos eliminados        

        except Exception as exception:
            logging.error("Error deleteBalancesByDate - %s." % str(exception))            


    def bkCustomer(self, orgId, documentNumber):
        conn = None
        try:

            directorio_actual = os.getcwd()
            # Nombre de la carpeta a crear si no existe
            nombre_carpeta = "Backups"
            # Ruta completa para la carpeta
            ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
            # Verificar si la carpeta no existe, y si no, crearla
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)   

            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Customers']
            customers = db['customers']

            # Eliminar múltiples documentos basados en la fecha
            organizationCustomerId = orgId+"-"+documentNumber
            result = customers.find({ "$and": [ {"customerInformation.organizationCustomerId":organizationCustomerId}]})

            # Nombre del archivo para guardar los resultados
            nombre_archivo = str(orgId)+"_"+str(documentNumber)+"_customers_"+ str(datetime.now())+".json"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            # Guardar los resultados en un archivo JSON dentro de la carpeta
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            # Guardar los resultados en un archivo JSON
            with open(ruta_archivo, 'w') as archivo:
                for documento in result:
                    # Escribir cada documento en el archivo
                    document_str = json.dumps(documento, default=json_util.default)
                    archivo.write(json.dumps(document_str))
                    archivo.write("\n")

            return nombre_archivo  # Devolver el nombre del archivo donde se guardaron los resultados
        
        except Exception as exception:
            logging.error("Error deleteBalancesByDate - %s." % str(exception))

    def bkExternalData(self, orgId, documentNumber):
        conn = None
        try:

            directorio_actual = os.getcwd()
            # Nombre de la carpeta a crear si no existe
            nombre_carpeta = "Backups"
            # Ruta completa para la carpeta
            ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
            # Verificar si la carpeta no existe, y si no, crearla
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)   

            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Customers']
            externalData = db['externalData']
            organizationCustomerId = orgId+"-"+documentNumber
            result = externalData.find({ "$and": [ {"organizationCustomerId":organizationCustomerId}]})

            # Nombre del archivo para guardar los resultados
            nombre_archivo = str(orgId)+"_"+str(documentNumber)+"_externalData_"+ str(datetime.now())+".json"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            # Guardar los resultados en un archivo JSON dentro de la carpeta
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            # Guardar los resultados en un archivo JSON
            with open(ruta_archivo, 'w') as archivo:
                for documento in result:
                    # Escribir cada documento en el archivo
                    document_str = json.dumps(documento, default=json_util.default)
                    archivo.write(json.dumps(document_str))
                    archivo.write("\n")

            return nombre_archivo  # Devolver el nombre del archivo donde se guardaron los resultados


        except Exception as exception:
            logging.error("Error deleteBalancesByDate - %s." % str(exception))

    def bkAdditionalInformation(self, orgId, documentNumber):
        conn = None
        try:

            directorio_actual = os.getcwd()
            # Nombre de la carpeta a crear si no existe
            nombre_carpeta = "Backups"
            # Ruta completa para la carpeta
            ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
            # Verificar si la carpeta no existe, y si no, crearla
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)   

            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_controls:5Y1Y9843cKIzn2dls8j2bkaIxJ6x5V16@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
           
            db = conn['Customers']
            additionalInformation = db['additionalInformation']

            organizationCustomerId = orgId+"-"+documentNumber
            result = additionalInformation.find({ "$and": [ {"organizationCustomerId":organizationCustomerId}]})

            # Nombre del archivo para guardar los resultados
            nombre_archivo = str(orgId)+"_"+str(documentNumber)+"_additionalInformation_"+ str(datetime.now())+".json"            
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')

            # Guardar los resultados en un archivo JSON dentro de la carpeta
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            # Guardar los resultados en un archivo JSON
            with open(ruta_archivo, 'w') as archivo:
                for documento in result:
                    # Escribir cada documento en el archivo
                    document_str = json.dumps(documento, default=json_util.default)
                    archivo.write(json.dumps(document_str))
                    archivo.write("\n")

            return nombre_archivo  # Devolver el nombre del archivo donde se guardaron los resultados      

        except Exception as exception:
            logging.error("Error deleteBalancesByDate - %s." % str(exception))      


