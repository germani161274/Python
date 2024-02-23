from datetime import datetime, timedelta
from postgresqlConn import PostgresConn
from mongodbConn import MongoConn
from pymongo import MongoClient
import argparse
import sys

def main(orgId, documentNumber):
    # Aquí va tu lógica principal basada en los parámetros recibidos
    print("                                                                   ")
    print(f"OrganizationID: {orgId}")
    print(f"DocumentNumber: {documentNumber}")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Script con parámetros')
    parser.add_argument('--orgId', help='OrganizationID')
    parser.add_argument('--documentNumber', help='DocumentNumber')

    args = parser.parse_args()

    orgId = args.orgId
    documentNumber = args.documentNumber

    #orgId = 'org-074'
    #documentNumber = 'prueba'  

    if len(orgId) != 7:
        print("El parametro orgId " + str(orgId)+ " deber ser de 7 caracteres.")
        sys.exit()

    main(orgId, documentNumber)

    print("                                                 ")
    print("                 ATENCIÓN !!!                    ")
    print("                                                 ")
    print("#########    Borrado tablas PostgreSQL  #########")
    print("Se borrará de la tabla authorization_process.otp_validation el organization_customer_id = '"+orgId+"-"+documentNumber+"'")
    print("Se borrará de la tabla organization.customer_onboarding el document_number = '"+documentNumber+"' y organization_id = '"+orgId+"'")
    print("Se borrará de la tabla transaction_manager.transaction el organization_customer_id = '"+orgId+"-"+documentNumber+"'")
    print("                                                                   ")
    print("#########    Borrado tablas MongoDB     #########")
    print("Se borrará de la tabla customer el organizationCustomerId = '"+orgId+"-"+documentNumber+"'")
    print("Se borrará de la tabla externalData el organizationCustomerId = '"+orgId+"-"+documentNumber+"'")
    print("Se borrará de la tabla additionalInformation el organizationCustomerId = '"+orgId+"-"+documentNumber+"'")
    print("                                                                   ")
    
    while True:
        respuesta = input("¿Desea continuar? (yes/no): ").lower()
        if respuesta == "yes":
            print("Continuando...")
            # Aquí puedes colocar el código que se ejecutará si la respuesta es "yes"
            break  # Rompe el bucle para salir
        elif respuesta == "no":
            print("Deteniendo el proceso.")
            sys.exit()
        else:
            print("Por favor, responda con 'yes' o 'no'.")

    ################################################################## 
    #               Borrado tablas Postgres                          #
    ##################################################################
    
    postgresqlConn = PostgresConn()

    print("Realizando bk PostgreSQL...")

    #transaction_manager.transaction
    borraTransaction = postgresqlConn.bkTransaction(orgId, documentNumber)   
    if postgresqlConn.conn:
        postgresqlConn.conn.close()

    #authorization_process.otp_validation
    borraOtp_validation = postgresqlConn.bkOtp_validation(orgId, documentNumber)   
    if postgresqlConn.conn:
        postgresqlConn.conn.close()

    #organization.customer_onboarding
    borraCustomer_onboarding = postgresqlConn.bkCustomer_onboarding(orgId, documentNumber)    
    if postgresqlConn.conn:
        postgresqlConn.conn.close()
     
    print("Borrando registros PostgreSQL...")
    
    #authorization_process.otp_validation
    borraOtp_validation = postgresqlConn.deleteOtp_validation(orgId, documentNumber)   
    if postgresqlConn.conn:
        postgresqlConn.conn.close()

    #organization.customer_onboarding
    borraCustomer_onboarding = postgresqlConn.deleteCustomer_onboarding(orgId, documentNumber)    
    if postgresqlConn.conn:
        postgresqlConn.conn.close()
     
    #transaction_manager.transaction
    borraTransaction = postgresqlConn.deleteTransaction(orgId, documentNumber)   
    if postgresqlConn.conn:
        postgresqlConn.conn.close()
    
    print("Finalizado") 

    ################################################################## 
    #               Bk y borrado tablas MongoDb                      #
    ##################################################################

    mongodbConn = MongoConn()

    print("Realizando bk MongoDb...")

    #Customer
    borraOtp_validation = mongodbConn.bkCustomer(orgId, documentNumber)     
    if mongodbConn.conn:
        mongodbConn.conn.close()

    #ExternalData
    borraTransaction = mongodbConn.bkExternalData(orgId, documentNumber)    
    if mongodbConn.conn:
        mongodbConn.conn.close()

    #AdditionalInformation
    borraCustomer_onboarding = mongodbConn.bkAdditionalInformation(orgId, documentNumber)   
    if mongodbConn.conn:
        mongodbConn.conn.close()   

    print("Borrando registros MongoDb...")
    
    #Customer
    borraOtp_validation = mongodbConn.deleteCustomer(orgId, documentNumber)     
    if mongodbConn.conn:
        mongodbConn.conn.close()

    #ExternalData
    borraTransaction = mongodbConn.deleteExternalData(orgId, documentNumber)    
    if mongodbConn.conn:
        mongodbConn.conn.close()

    #AdditionalInformation
    borraCustomer_onboarding = mongodbConn.deleteAdditionalInformation(orgId, documentNumber)   
    if mongodbConn.conn:
        mongodbConn.conn.close()   
    
    print("Finalizado")    
