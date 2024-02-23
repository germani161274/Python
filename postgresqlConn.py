import psycopg2
import logging
import os
from datetime import datetime, timedelta
import csv


class PostgresConn:
    conn = None

    logging.basicConfig(
                    filename='Control.log',
                    level=logging.INFO,
                    format='%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

    def __init__(self):
        None

    def deleteOtp_validation(self, orgId, documentNumber):  
        conn = None

        try:
            conn = psycopg2.connect(
                dbname='juno',
                user='juno_controls',
                password='601Jwk66OIYH18jll711Nd2wSklOPI6o',
                host='prod-juno-database.cluster-ctza19neneum.us-east-1.rds.amazonaws.com',
                port='6543'
            )

            cursor = conn.cursor()
            cursor.execute("delete from authorization_process.otp_validation where organization_customer_id in ('"+orgId+"-"+documentNumber+"')")
 
            conn.commit()

            cursor.close()

        except psycopg2.Error as e:
            logging.error("Error getQtyCountry_v2 - %s." % str(e))

        finally:
            if conn is not None:
                conn.close()

    def deleteCustomer_onboarding(self, orgId, documentNumber):
        conn = None

        try:
            conn = psycopg2.connect(
                dbname='juno',
                user='juno_controls',
                password='601Jwk66OIYH18jll711Nd2wSklOPI6o',
                host='prod-juno-database.cluster-ctza19neneum.us-east-1.rds.amazonaws.com',
                port='6543'
            )

            cursor = conn.cursor()
            cursor.execute("delete FROM organization.customer_onboarding  where document_number in ('"+documentNumber+"') and organization_id in ('"+orgId+"')")
 
            conn.commit()

            cursor.close()

        except psycopg2.Error as e:
            logging.error("Error getQtyCountry_v2 - %s." % str(e))
            
        finally:
            if conn is not None:
                conn.close()

    def deleteTransaction(self, orgId, documentNumber):
        conn = None

        try:
            conn = psycopg2.connect(
                dbname='juno',
                user='juno_controls',
                password='601Jwk66OIYH18jll711Nd2wSklOPI6o',
                host='prod-juno-database.cluster-ctza19neneum.us-east-1.rds.amazonaws.com',
                port='6543'
            )

            cursor = conn.cursor()
            cursor.execute("delete from transaction_manager.transaction where organization_customer_id in ('"+orgId+"-"+documentNumber+"')")
 
            conn.commit()

            cursor.close()

        except psycopg2.Error as e:
            logging.error("Error getQtyCountry_v2 - %s." % str(e))
            
        finally:
            if conn is not None:
                conn.close()


    def bkOtp_validation(self, orgId, documentNumber):  
        conn = None

        directorio_actual = os.getcwd()
        # Nombre de la carpeta a crear si no existe
        nombre_carpeta = "Backups"
        # Ruta completa para la carpeta
        ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
        # Verificar si la carpeta no existe, y si no, crearla
        if not os.path.exists(ruta_carpeta):
            os.makedirs(ruta_carpeta)   

        try:
            conn = psycopg2.connect(
                dbname='juno',
                user='juno_controls',
                password='601Jwk66OIYH18jll711Nd2wSklOPI6o',
                host='prod-juno-database.cluster-ctza19neneum.us-east-1.rds.amazonaws.com',
                port='6543'
            )

            cursor = conn.cursor()
            cursor.execute("select * from authorization_process.otp_validation where organization_customer_id in ('"+orgId+"-"+documentNumber+"')")
 
            country_data = cursor.fetchall()

            cursor.close()

            # Nombre del archivo para guardar los resultados
            nombre_archivo = str(orgId)+"_"+str(documentNumber)+"_otp_validation_"+ str(datetime.now())+".csv"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            # Guardar los resultados en un archivo JSON dentro de la carpeta
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            with open(ruta_archivo, 'w', newline='') as archivo:
                csv_writer = csv.writer(archivo)
                # Escribir encabezados
                csv_writer.writerow([i[0] for i in cursor.description])

                for row in country_data:
                    # Formatear la fecha y eliminar la palabra 'Decimal' si existe en algún campo
                    formatted_row = [value if not isinstance(value, datetime) else value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime) else str(value).replace("Decimal('", "").replace("')", "") for value in row]
                    csv_writer.writerow(formatted_row)

            return nombre_archivo  # Devolver el nombre del archivo donde se guardaron los resultados

        except psycopg2.Error as e:
            logging.error("Error getQtyCountry_v2 - %s." % str(e))

        finally:
            if conn is not None:
                conn.close()

    def bkCustomer_onboarding(self, orgId, documentNumber):
        conn = None

        directorio_actual = os.getcwd()
        # Nombre de la carpeta a crear si no existe
        nombre_carpeta = "Backups"
        # Ruta completa para la carpeta
        ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
        # Verificar si la carpeta no existe, y si no, crearla
        if not os.path.exists(ruta_carpeta):
            os.makedirs(ruta_carpeta)   

        try:
            conn = psycopg2.connect(
                dbname='juno',
                user='juno_controls',
                password='601Jwk66OIYH18jll711Nd2wSklOPI6o',
                host='prod-juno-database.cluster-ctza19neneum.us-east-1.rds.amazonaws.com',
                port='6543'
            )

            cursor = conn.cursor()
            cursor.execute("select *  FROM organization.customer_onboarding  where document_number in ('"+documentNumber+"') and organization_id in ('"+orgId+"')")
 
            country_data = cursor.fetchall()

            cursor.close()

            # Nombre del archivo para guardar los resultados
            nombre_archivo = str(orgId)+"_"+str(documentNumber)+"_customer_onboarding_"+ str(datetime.now())+".csv"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            # Guardar los resultados en un archivo JSON dentro de la carpeta
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            with open(ruta_archivo, 'w', newline='') as archivo:
                csv_writer = csv.writer(archivo)
                # Escribir encabezados
                csv_writer.writerow([i[0] for i in cursor.description])

                for row in country_data:
                    # Formatear la fecha y eliminar la palabra 'Decimal' si existe en algún campo
                    formatted_row = [value if not isinstance(value, datetime) else value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime) else str(value).replace("Decimal('", "").replace("')", "") for value in row]
                    csv_writer.writerow(formatted_row)

            return nombre_archivo  # Devolver el nombre del archivo donde se guardaron los resultados

        except psycopg2.Error as e:
            logging.error("Error getQtyCountry_v2 - %s." % str(e))
            
        finally:
            if conn is not None:
                conn.close()

    def bkTransaction(self, orgId, documentNumber):
        conn = None

        directorio_actual = os.getcwd()
        # Nombre de la carpeta a crear si no existe
        nombre_carpeta = "Backups"
        # Ruta completa para la carpeta
        ruta_carpeta = os.path.join(directorio_actual, nombre_carpeta)
        # Verificar si la carpeta no existe, y si no, crearla
        if not os.path.exists(ruta_carpeta):
            os.makedirs(ruta_carpeta)   

        try:
            conn = psycopg2.connect(
                dbname='juno',
                user='juno_controls',
                password='601Jwk66OIYH18jll711Nd2wSklOPI6o',
                host='prod-juno-database.cluster-ctza19neneum.us-east-1.rds.amazonaws.com',
                port='6543'
            )

            cursor = conn.cursor()
            cursor.execute("select * from transaction_manager.transaction where organization_customer_id in ('"+orgId+"-"+documentNumber+"')")
 
            country_data = cursor.fetchall()

            cursor.close()

            # Nombre del archivo para guardar los resultados
            nombre_archivo = str(orgId)+"-"+str(documentNumber)+"_transaction_"+ str(datetime.now())+".csv"
            nombre_archivo = nombre_archivo.replace(' ', '_').replace(':', '_')
            # Guardar los resultados en un archivo JSON dentro de la carpeta
            ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

            with open(ruta_archivo, 'w', newline='') as archivo:
                csv_writer = csv.writer(archivo)
                # Escribir encabezados
                csv_writer.writerow([i[0] for i in cursor.description])

                for row in country_data:
                    # Formatear la fecha y eliminar la palabra 'Decimal' si existe en algún campo
                    formatted_row = [value if not isinstance(value, datetime) else value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime) else str(value).replace("Decimal('", "").replace("')", "") for value in row]
                    csv_writer.writerow(formatted_row)

            return nombre_archivo  # Devolver el nombre del archivo donde se guardaron los resultados


        except psycopg2.Error as e:
            logging.error("Error getQtyCountry_v2 - %s." % str(e))
            
        finally:
            if conn is not None:
                conn.close()

    """ 
    def getQtyCountry_v2(self):
        conn = None

        try:
            conn = psycopg2.connect(
                dbname='juno',
                user='juno_controls',
                password='601Jwk66OIYH18jll711Nd2wSklOPI6o',
                host='juno-dev-database2.c5ebb2jx7zvm.us-east-1.rds.amazonaws.com',
                port='6543'
            )

            cursor = conn.cursor()
            #cursor.execute("SELECT COUNT(*) FROM balances WHERE loan_id = %s AND date = %s AND organization_id = %s", (loanId, date, orgId))
            cursor.execute("SELECT count(*) FROM geography.country_v2")
            
            country_count = cursor.fetchone()[0]

            cursor.close()
            return country_count

        except psycopg2.Error as e:
            logging.error("Error getQtyCountry_v2 - %s." % str(e))
        finally:
            if conn is not None:
                conn.close()
    """


      



