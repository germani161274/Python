import logging
import os
import csv
import uuid
import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo import MongoClient
from uuid import uuid4

class MongoConn:

    conn = None

    def __init__(self):
        None

    def bkOlimpiaIntegration(self, month, year, source):

        conn = None
        try:
            directorio_actual = os.getcwd()
            nombre_carpeta = "Backups"
            ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
            
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)   

            if source == 1:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@10.0.1.198:27017/Customers?authSource=admin')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
        
            elif source == 2:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            else:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            db = conn['olimpiaIntegration']
            signature_process = db['signature_process']

            if int(month) != 12:
                next_month =  int(month) + 1
                next_year = int(year)
            else:   
                next_month =  1
                next_year = int(year) + 1

            result = signature_process.find({"$and": [{"creationDate": {"$gte": datetime(int(year), int(month), 1)}}, {"creationDate": {"$lt": datetime(int(next_year), int(next_month), 1)}}]})

            nombre_archivo = str(month) + "_" + str(year) + "_olimpiaIntegration_" + str(datetime.now()) + ".csv"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            # Lista de nombres de columnas que deseas incluir en el archivo CSV
            columnas_deseadas = ['creationDate', 'transactionID', 'lenderId','identificacionComercio','timeOutInDays','processStatus','rejectionReason','requiereATDP','externalRequest']

            

            with open(ruta_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
                csv_writer = csv.writer(archivo_csv)
                
                # Escribir encabezados (nombres de las columnas)
                csv_writer.writerow(columnas_deseadas)
                
                for documento in result:
                    # Obtener los valores solo para las columnas deseadas

                    transaction_id_hex = documento.get('transactionID', '')
                    # Convertir el valor binario a un objeto UUID
                    uuid_from_binary_tx = uuid.UUID(bytes=transaction_id_hex)
                    transactionId_str = uuid_from_binary_tx 

                    lenderId_hex = documento.get('lenderId', '')
                    # Convertir el valor binario a un objeto UUID
                    uuid_from_binary_lenderId = uuid.UUID(bytes=lenderId_hex)
                    lenderId_str = uuid_from_binary_lenderId 

                    fila = [
                        documento.get('creationDate', ''),
                        transactionId_str,
                        lenderId_str,
                        documento.get('identificacionComercio', ''),
                        documento.get('timeOutInDays', ''),
                        documento.get('processStatus', ''),
                        documento.get('rejectionReason', ''),
                        documento.get('requiereATDP', ''),
                        documento.get('externalRequest', '')
                        #str(transactionId_str) if transactionId_str else ''
                    ]

                    #fila = [documento.get(columna, '') for columna in columnas_deseadas]
                    csv_writer.writerow(fila)

            return nombre_archivo

        except Exception as exception:
            logging.error("Error bkExternalData - %s." % str(exception))

        finally:
            if conn:
                conn.close()

    def bkTruoraIntegration(self, month, year, source):

        conn = None
        try:
            directorio_actual = os.getcwd()
            nombre_carpeta = "Backups"
            ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
            
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)   

            if source == 1:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@10.0.1.198:27017/Customers?authSource=admin')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
        
            elif source == 2:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            else:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            db = conn['truoraIntegration']
            signature_process = db['backgroundChecks']

            if int(month) != 12:
                next_month =  int(month) + 1
                next_year = int(year)
            else:   
                next_month =  1
                next_year = int(year) + 1

            result = signature_process.find({"$and": [{"creationDate": {"$gte": datetime(int(year), int(month), 1)}}, {"creationDate": {"$lt": datetime(int(next_year), int(next_month), 1)}}]})

            nombre_archivo = str(month) + "_" + str(year) + "_truoraIntegration_" + str(datetime.now()) + ".csv"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            # Lista de nombres de columnas que deseas incluir en el archivo CSV
            columnas_deseadas = ['creationDate', 'externalRequest.reference.flowId', 'externalRequest.reference.stepId', 'externalRequest.parameters.retries', 
                                 'externalRequest.parameters.priority', 'externalRequest.payload.backgroundCheckBody.country', 
                                 'externalRequest.payload.backgroundCheckBody.type', 'externalRequest.payload.backgroundCheckBody.userAuthorized', 
                                 'externalRequest.payload.backgroundCheckBody.forceCreation', 'externalRequest.payload.backgroundCheckBody.nationalId', 
                                 'transactionID', 'personId', 'lenderId', 'processStatus', 'statusCode', 'maxRetries', 'retryCount']

            with open(ruta_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
                csv_writer = csv.writer(archivo_csv)
                
                # Escribir encabezados (nombres de las columnas)
                csv_writer.writerow(columnas_deseadas)
                
                for documento in result:
                    # Obtener los valores solo para las columnas deseadas

                    transaction_id_hex = documento.get('transactionID', '')
                    # Convertir el valor binario a un objeto UUID
                    if documento.get('transactionID', ''):
                        uuid_from_binary_tx = uuid.UUID(bytes=transaction_id_hex)
                        transactionId_str = uuid_from_binary_tx 
                    else:
                        transactionId_str = ''

                    lenderId_hex = documento.get('lenderId', '')
                    # Convertir el valor binario a un objeto UUID
                    if documento.get('lenderId', ''):
                        uuid_from_binary_lenderId = uuid.UUID(bytes=lenderId_hex)
                        lenderId_str = uuid_from_binary_lenderId 
                    else:
                        lenderId_str = ''    

                    personId_hex = documento.get('personId', '')
                    # Convertir el valor binario a un objeto UUID
                    if documento.get('personId', ''):
                        uuid_from_binary_personId= uuid.UUID(bytes=personId_hex)
                        personId_str = uuid_from_binary_personId 
                    else:
                        personId_str = ''    

                    if  documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', ''):
                        country = documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', '').get('country', '')
                    else:   
                        country = '' 
                    if  documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', ''):
                        type = documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', '').get('type', '')
                    else:  
                        type = ''
                    if  documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', ''):
                        userAuthorized = documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', '').get('userAuthorized', '')
                    else:  
                        userAuthorized = ''
                    if  documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', ''):
                        forceCreation = documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', '').get('forceCreation', '')
                    else:  
                        forceCreation = ''
                    if  documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', ''):
                        nationalId = documento.get('externalRequest', {}).get('payload', {}).get('backgroundCheckBody', '').get('nationalId', '')
                    else:  
                        nationalId = ''
                    if  documento.get('processStatus', {}):
                        processStatus  = documento.get('processStatus', {})
                    else:  
                        processStatus  = ''
                    if  documento.get('statusCode', {}):
                        statusCode = documento.get('statusCode', {})
                    else:  
                        statusCode = ''                    
                    if  documento.get('maxRetries', {}):
                        maxRetries = documento.get('maxRetries', {})
                    else:  
                        maxRetries = ''                    
                    if  documento.get('retryCount', {}) :
                        retryCount = documento.get('retryCount', {}) 
                    else:  
                        retryCount = ''
                    if  documento.get('creationDate', ''):
                        creationDate = documento.get('creationDate', '')
                    else:
                        creationDate = ''
                    if  documento.get('externalRequest', {}).get('reference', {}).get('flowId', ''):
                        flowId = documento.get('externalRequest', {}).get('reference', {}).get('flowId', '')
                    else: 
                        flowId = ''
                    if  documento.get('externalRequest', {}).get('reference', {}).get('stepId', ''):
                        stepId = documento.get('externalRequest', {}).get('reference', {}).get('stepId', '')
                    else:
                        stepId = ''
                    if  documento.get('externalRequest', {}).get('parameters', {}).get('retries', ''):
                        retries = documento.get('externalRequest', {}).get('parameters', {}).get('retries', '')
                    else:
                        retries = ''
                    if  documento.get('externalRequest', {}).get('parameters', {}).get('priority', ''):
                        priority = documento.get('externalRequest', {}).get('parameters', {}).get('priority', '')
                    else:
                        priority = ''

                    fila = [
                        creationDate,
                        flowId,
                        stepId,
                        retries,
                        priority,
                        country,
                        type,
                        userAuthorized,
                        forceCreation,
                        nationalId,
                        transactionId_str,
                        personId_str,
                        lenderId_str,
                        processStatus,
                        statusCode,
                        maxRetries,
                        retryCount                      
                    ]

                    csv_writer.writerow(fila)

            return nombre_archivo

        except Exception as exception:
            logging.error("Error bkExternalData - %s." % str(exception))

        finally:
            if conn:
                conn.close()


    def bkAdoIntegration(self, month, year, source):

        conn = None
        try:
            directorio_actual = os.getcwd()
            nombre_carpeta = "Backups"
            ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
            
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)   

            if source == 1:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@10.0.1.198:27017/Customers?authSource=admin')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
        
            elif source == 2:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            else:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            db = conn['adointegration']
            ado_integration = db['ado_integration']

            if int(month) != 12:
                next_month =  int(month) + 1
                next_year = int(year)
            else:   
                next_month =  1
                next_year = int(year) + 1

            result = ado_integration.find()

            nombre_archivo = str(month) + "_" + str(year) + "_adointegration_" + str(datetime.now()) + ".csv"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            # Lista de nombres de columnas que deseas incluir en el archivo CSV
            columnas_deseadas = ['StartingDate','transactionId','adoCallback', 'adoCallbackVerifyResponse','adoTransactionId', 'parameters','status','retry','reason']
            

            with open(ruta_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
                csv_writer = csv.writer(archivo_csv)
                
                # Escribir encabezados (nombres de las columnas)
                csv_writer.writerow(columnas_deseadas)
                
                for documento in result:
                    # Obtener los valores solo para las columnas deseadas

                    transaction_id_hex = documento.get('transactionId', '')
                    # Convertir el valor binario a un objeto UUID
                    uuid_from_binary_tx = uuid.UUID(bytes=transaction_id_hex)
                    transactionId_str = uuid_from_binary_tx 

                    adoCallback = documento.get('adoCallback', '')

                    ado_callback_dict = json.loads(adoCallback)

                    # Obtener la fecha de inicio
                    starting_date = ado_callback_dict.get('StartingDate', '')

                    starting_date_obj = datetime.fromisoformat(starting_date.split(".")[0])

                    if starting_date_obj.month == int(month) and starting_date_obj.year == int(year):

                        fila = [
                            starting_date,    
                            transactionId_str,
                            documento.get('adoCallback', ''),
                            documento.get('adoCallbackVerifyResponse', ''),
                            documento.get('adoTransactionId', ''),
                            documento.get('parameters', ''),
                            documento.get('status', ''),
                            documento.get('retry', ''),
                            documento.get('reason', '')
                        ]

                        csv_writer.writerow(fila)

            return nombre_archivo

        except Exception as exception:
            logging.error("Error bkExternalData - %s." % str(exception))

        finally:
            if conn:
                conn.close()               


    def bkCustomersExternalDataSummaryBuro(self, month, year, source):

        conn = None
        try:
            directorio_actual = os.getcwd()
            nombre_carpeta = "Backups"
            ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
            
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)   

            if source == 1:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@10.0.1.198:27017/Customers?authSource=admin')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
        
            elif source == 2:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            else:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")

            db = conn['Customers']
            externalData = db['externalData']

            if int(month) != 12:
                next_month =  int(month) + 1
                next_year = int(year)
            else:   
                next_month =  1
                next_year = int(year) + 1

            result = externalData.find({"summaryBuro": {"$exists": True}})

            nombre_archivo = str(month) + "_" + str(year) + "_customersExternalDataSummaryBuro_" + str(datetime.now()) + ".csv"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            # Lista de nombres de columnas que deseas incluir en el archivo CSV
            columnas_deseadas = ['date']
            
            with open(ruta_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
                csv_writer = csv.writer(archivo_csv)
                
                # Escribir encabezados (nombres de las columnas)
                csv_writer.writerow(columnas_deseadas)
                
                for documento in result:
                    # Obtener los valores solo para las columnas deseadas

                    if documento.get('summaryBuro',  []) and isinstance(documento.get('summaryBuro',  []), list):                  
                       if documento.get('summaryBuro', [])[0].get("score", []) :
                            date_value = documento.get('summaryBuro', [])[0].get('score', [])[0].get('date', '')

                            if es_fecha(date_value):     

                                if date_value.month and date_value.year:
                                    if date_value.month == int(month) and date_value.year == int(year):

                                        fila = [
                                            date_value
                                        ]

                                        csv_writer.writerow(fila)

            return nombre_archivo

        except Exception as exception:
            logging.error("Error bkExternalData - %s." % str(exception))
            logging.exception("Detalles del error:")  # Esta línea imprimirá la traza de la excepción

        finally:
            if conn:
                conn.close()               


    def bkCustomersExternalDataSummaryBackgroundChecks(self, month, year, source):

        conn = None
        try:
            directorio_actual = os.getcwd()
            nombre_carpeta = "Backups"
            ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
            
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)   

            if source == 1:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@10.0.1.198:27017/Customers?authSource=admin')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
        
            elif source == 2:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            else:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            db = conn['Customers']
            externalData = db['externalData']

            if int(month) != 12:
                next_month =  int(month) + 1
                next_year = int(year)
            else:   
                next_month =  1
                next_year = int(year) + 1

            result = externalData.find({"summaryBackgroundChecks": {"$exists": True, "$ne": []}})

            nombre_archivo = str(month) + "_" + str(year) + "_customersExternalDataSummaryBackgroundChecks_" + str(datetime.now()) + ".csv"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            # Lista de nombres de columnas que deseas incluir en el archivo CSV
            columnas_deseadas = ['date']
            
            with open(ruta_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
                csv_writer = csv.writer(archivo_csv)
                
                # Escribir encabezados (nombres de las columnas)
                csv_writer.writerow(columnas_deseadas)
                
                for documento in result:
                    # Obtener los valores solo para las columnas deseadas

                    if documento.get('summaryBackgroundChecks') :                  
                       if documento.get('summaryBackgroundChecks').get("dateResponse") :
                            
                            date = documento.get('summaryBackgroundChecks', '').get('dateResponse', '')
                            timestamp_segundos = int(date) / 1000.0
                            # Crear un objeto datetime a partir del timestamp en segundos
                            date_value = datetime.utcfromtimestamp(timestamp_segundos)
                            date_value = date_value - timedelta(hours=5)

                            if es_fecha(date_value):
                                if date_value.month and date_value.year:
                                    if date_value.month == int(month) and date_value.year == int(year):

                                        fila = [
                                            date_value
                                        ]

                                        csv_writer.writerow(fila)

            return nombre_archivo

        except Exception as exception:
            logging.error("Error bkExternalData - %s." % str(exception))

        finally:
            if conn:
                conn.close()               

    def bkCrossCoreIntegration(self, month, year, source):

        conn = None
        try:
            directorio_actual = os.getcwd()
            nombre_carpeta = "Backups"
            ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
            
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)   

            if source == 1:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@10.0.1.198:27017/Customers?authSource=admin')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
        
            elif source == 2:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            else:
                try:
                    conn = MongoClient('mongodb://juno_controls:601Jwk66OIYH18jll711Nd2wSklOPI6o@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
            db = conn['crosscoreIntegration']
            bureauOtpValidationCO = db['bureauOtpValidationCO']

            if int(month) != 12:
                next_month =  int(month) + 1
                next_year = int(year)
            else:   
                next_month =  1
                next_year = int(year) + 1

            result = bureauOtpValidationCO.find({"$and": [{"creationDate": {"$gte": datetime(int(year), int(month), 1)}}, {"creationDate": {"$lt": datetime(int(next_year), int(next_month), 1)}}]})

            nombre_archivo = str(month) + "_" + str(year) + "_crosscoreIntegration_" + str(datetime.now()) + ".csv"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            # Lista de nombres de columnas que deseas incluir en el archivo CSV
            columnas_deseadas = ['creationDate','flowId','stepId','transactionId','personId','lenderId','origin','status']

            with open(ruta_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
                csv_writer = csv.writer(archivo_csv)
                
                # Escribir encabezados (nombres de las columnas)
                csv_writer.writerow(columnas_deseadas)
                
                for documento in result:
                    # Obtener los valores solo para las columnas deseadas
                    
                    transaction_id_hex = documento.get('transactionId', '')
                    # Convertir el valor binario a un objeto UUID
                    uuid_from_binary_tx = uuid.UUID(bytes=transaction_id_hex)
                    transactionId_str = uuid_from_binary_tx 
                    
                    lenderId_hex = documento.get('lenderId', '')
                    # Convertir el valor binario a un objeto UUID
                    uuid_from_binary_lenderId = uuid.UUID(bytes=lenderId_hex)
                    lenderId_str = uuid_from_binary_lenderId 

                    flowId_hex = documento.get('flowId', '')
                    # Convertir el valor binario a un objeto UUID
                    uuid_from_binary_flowId = uuid.UUID(bytes=flowId_hex)
                    flowId_str = uuid_from_binary_flowId

                    stepId_hex = documento.get('stepId', '')
                    # Convertir el valor binario a un objeto UUID
                    uuid_from_binary_stepId = uuid.UUID(bytes=stepId_hex)
                    stepId_str = uuid_from_binary_stepId

                    personId_hex = documento.get('personId', '')
                    # Convertir el valor binario a un objeto UUID
                    uuid_from_binary_personId = uuid.UUID(bytes=personId_hex)
                    personId_str = uuid_from_binary_personId 

                    fila = [
                        documento.get('creationDate', ''),
                        flowId_str,
                        stepId_str,
                        transactionId_str,
                        personId_str,
                        lenderId_str,
                        documento.get('origin', ''),
                        documento.get('status', '')                   
                    ]

                    #fila = [documento.get(columna, '') for columna in columnas_deseadas]
                    csv_writer.writerow(fila)

            return nombre_archivo

        except Exception as exception:
            logging.error("Error bkExternalData - %s." % str(exception))

        finally:
            if conn:
                conn.close()


def es_fecha(dato):
    return isinstance(dato, datetime)                
