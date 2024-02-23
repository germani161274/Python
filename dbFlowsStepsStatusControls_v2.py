import logging
from datetime import datetime, timedelta
from flowsStepsStatus_v2 import Flow
from sendEmailFlowsStepsStatus_html_v2 import Mails
import json
from datetime import datetime, timedelta
from bson import json_util
from lenders import Lender
import uuid
import base64
from pymongo import MongoClient
from bson.binary import UuidRepresentation
from uuid import uuid4

# use the 'standard' representation for cross-language compatibility.
client = MongoClient(uuidRepresentation='standard')
collection = client.get_database('uuid_db').get_collection('uuid_coll')


if __name__ == '__main__':

    ##############################################################################################################################################
    ###    Control status of flow
    ##############################################################################################################################################
    
    mail = Mails()
    
    email_date = datetime.now()+ timedelta(hours=5)
 
    dia = email_date.day
    mes = email_date.month
    ano = email_date.year

    date = datetime(ano, mes, dia)

    date = date - timedelta(days=1)
    date_ant = date - timedelta(days=1)

    orgId = ''
    contador_flow = 0
    dic_status = {}
    dic_lender = {}
    dic_error = {}
    dic_failed = {}
    dic_pending = {}
    dic_in_progress = {}
    dic_finished = {}
    dic_confirmed = {}
    dic_routing_rule_not_match = {}

    dic_lender_tx_date = {}

    flow = Flow() 
    flowStatusList = flow.getStatusOfFlowV2()      
    if flow.conn:
        flow.conn.close()

    lenderList = flow.getLenders()

    mensaje = ''


    try:
        # Nombre del archivo para guardar los resultados
        nombre_archivo = "Status_of_flow_v2"+ str(datetime.now())+".json"

        document_str = json.dumps(flowStatusList, default=json_util.default)

        datos = json.loads(document_str)

        for etapa in datos:
            ################################################################################################
            # Extraer el valor binario del campo lenderId
            lenderId = etapa.get('lenderId')
            
            # Extraer el valor base 64 (no funcionaba la funcion asi que lo hice a mano)
            lenderId_str = str(lenderId)
            lenderId_str = lenderId_str[24:] 
            lenderId_str = lenderId_str[:-20] 
            base64_value = lenderId_str

            lender_desc =""

            # Decodificar la cadena base64
            binary_data = base64.b64decode(base64_value)

            # Convertir el valor binario a un objeto UUID
            uuid_from_binary = uuid.UUID(bytes=binary_data)
            lenderId_str  = uuid_from_binary 
            # Pisa lo de arriba 
            lenderId = "UUID('{}')".format(uuid_from_binary)
            lenderId_str = lenderId[6:] 
            lenderId_str = lenderId_str[:-2] 

            for lender in lenderList:
                loender_0 = lender[0]
                if loender_0 == lenderId_str:
                    lender_desc = lender[1] 

            #################################################################################################################

            # Extraer el valor binario del campo transactionId
            transactionId = etapa.get('transactionId')

            # Extraer el valor base 64 (no funcionaba la funcion asi que lo hice a mano)
            transactionId_str = str(transactionId)
            transactionId_str = transactionId_str[24:] 
            transactionId_str = transactionId_str[:-20] 
            base64_value = transactionId_str

            # Decodificar la cadena base64
            binary_data = base64.b64decode(base64_value)

            # Convertir el valor binario a un objeto UUID
            uuid_from_binary_tx = uuid.UUID(bytes=binary_data)
            transactionId_str = uuid_from_binary_tx            

            #################################################################################################################
                
            createdAt_str1 =  etapa.get("createdAt")
            createdAt_str1 = createdAt_str1['$date']
            createdAt = datetime.fromisoformat(createdAt_str1[:-1])  # Quitar 'Z' al final  

            updatedAt_str2 =  etapa.get("createdAt")
            updatedAt_str2 = updatedAt_str2['$date']
            updatedAt = datetime.fromisoformat(updatedAt_str2[:-1])  # Quitar 'Z' al final  

            currentStep = etapa.get("currentStep")


            # Se insertan en diferentes diccionarios las transaciones de acuerdo al estado de los steps  
            if etapa.get("status") == "ERROR": 
                dic_error[transactionId_str] = (lenderId_str, currentStep, createdAt, updatedAt)   
            if etapa.get("status") == "FAILED":  
                dic_failed[transactionId_str] = (lenderId_str, currentStep, createdAt, updatedAt)  
            if etapa.get("status") == "PENDING":  
                dic_pending[transactionId_str] = (lenderId_str, currentStep, createdAt, updatedAt)   
            if etapa.get("status") == "IN_PROGRESS": 
                dic_in_progress[transactionId_str] = (lenderId_str, currentStep, createdAt, updatedAt)   
            if etapa.get("status") == "FINISHED": 
                dic_finished[transactionId_str] = (lenderId_str, currentStep, createdAt, updatedAt)     
            if etapa.get("status") == "ROUTING_RULE_NOT_MATCH":
                dic_routing_rule_not_match[transactionId_str] = (lenderId_str, currentStep, createdAt, updatedAt)          
            if etapa.get("status") == "CONFIRMED":
                dic_confirmed[transactionId_str] = (lenderId_str, currentStep, createdAt, updatedAt)  

        # Se ordena la coleccion
        dic_error = dict(sorted(dic_error.items(), key=lambda x: x[1][0]))        
        dic_failed = dict(sorted(dic_failed.items(), key=lambda x: x[1][0]))
        dic_pending = dict(sorted(dic_pending.items(), key=lambda x: x[1][0]))
        dic_in_progress = dict(sorted(dic_in_progress.items(), key=lambda x: x[1][0]))
        dic_finished = dict(sorted(dic_finished.items(), key=lambda x: x[1][0]))
        dic_routing_rule_not_match = dict(sorted(dic_routing_rule_not_match.items(), key=lambda x: x[1][0]))
        dic_confirmed = dict(sorted(dic_confirmed.items(), key=lambda x: x[1][0]))
        
        #Presentacion de los datos
        mensaje = mensaje + "<br>" + "<h3 style='color: #14044F;'>Estado de las transacciones</h3>"

        mensaje = mensaje + "<table width='100%' style='border: 1px solid #133337;'>"
        mensaje = mensaje + "<table width='100%' style='background-color: #BAD2FF; '>"  
        mensaje = mensaje + "ERROR"
        mensaje = mensaje + "</table>"

        cabecera = "<tr><td style='width: 20%; text-align: left;'>Lender</td><td style='width: 30%; text-align: left;'>Transacción</td><td style='width: 20%; text-align: left;'>Step Actual</td><td style='td style='width: 15%; text-align: left;'>Tiempo ciclo</td><td style='width: 15%; text-align: left;'>Tiempo espera</td></tr>"

        mensaje = mensaje + "<table width='100%' style='background-color: #FEFED9;'>"  
        mensaje = mensaje + cabecera
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%'>" 
        if dic_error:
            for clave, valor in dic_error.items():  
                lender = valor[0]
                transaction = clave
                currrent_step = valor[1]
                cycle_time = updatedAt - createdAt
                wait_time = email_date - updatedAt
                

                mensaje = mensaje +  "<tr><td style='width: 20%; text-align: left;'>" + str(lender_desc) + "</td><td style='width: 30%; text-align: left;'>" + str(transaction)  + "</td><td style='width: 20%; text-align: left;'>" +  str(currrent_step)+ "</td><td style='width: 15%;'>" + str(cycle_time)[:-4] + "</td><td style='width: 15%;'>" + str(wait_time)[:-4]+ "</td></tr>"
        else: 
                mensaje = mensaje +  "<tr><td style='width: 100%; text-align: left;'>" + "No existen transacciones" + "</td> </tr>"
        mensaje = mensaje + "</table>"

        
        mensaje = mensaje + "<table width='100%' style='background-color: #BAD2FF; '>"  
        mensaje = mensaje + "FAILED"     
        mensaje = mensaje + "</table>"  

        mensaje = mensaje + "<table width='100%' style='background-color: #FEFED9;'>"  
        mensaje = mensaje + cabecera
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%'>"  
        if dic_failed:
            for clave, valor in dic_failed.items():  
                lender = valor[0]
                transaction = clave
                currrent_step = valor[1]
                cycle_time = updatedAt - createdAt
                wait_time = 0

                mensaje = mensaje +  "<tr><td style='width: 20%; text-align: left;'>" + str(lender_desc) + "</td><td style='width: 30%; text-align: left;'>" + str(transaction)  + "</td><td style='width: 20%; text-align: left;'>" +  str(currrent_step)+ "</td><td style='width: 15%;'>" + str(cycle_time) + "</td><td style='width: 15%;'>No se mide</td></tr>"
        else: 
                mensaje = mensaje +  "<tr><td style='width: 100%; text-align: left;'>" + "No existen transacciones" + "</td> </tr>"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #BAD2FF; '>"   
        mensaje = mensaje + "PENDING"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #FEFED9;'>"  
        mensaje = mensaje + cabecera
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%'>" 
        if dic_pending:
            for clave, valor in dic_pending.items(): 
                lender = valor[0]
                transaction = clave
                currrent_step = valor[1]
                cycle_time = email_date - createdAt
                wait_time = 0            

                mensaje = mensaje +  "<tr><td style='width: 20%; text-align: left;'>" + str(lender_desc) + "</td><td style='width: 30%; text-align: left;'>" + str(transaction)  + "</td><td style='width: 20%; text-align: left;'>" +  str(currrent_step)+ "</td><td style='width: 15%;'>" + str(cycle_time)[:-4] + "</td><td style='width: 15%;'>No se mide</td></tr>"
        else: 
                mensaje = mensaje +  "<tr><td style='width: 100%; text-align: left;'>" + "No existen transacciones" + "</td> </tr>"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #BAD2FF; '>"   
        mensaje = mensaje + "IN_PROGRESS"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #FEFED9;'>"  
        mensaje = mensaje + cabecera
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%'>"  
        if dic_in_progress:       
            for clave, valor in dic_in_progress.items():  
                lender = valor[0]
                transaction = clave
                currrent_step = valor[1]
                cycle_time = email_date - createdAt 
                wait_time = email_date - updatedAt
      
                mensaje = mensaje +  "<tr><td style='width: 20%; text-align: left;'>" + str(lender_desc) + "</td><td style='width: 30%; text-align: left;'>" + str(transaction)  + "</td><td style='width: 20%; text-align: left;'>" +  str(currrent_step)+ "</td><td style='width: 15%;'>" + str(cycle_time)[:-4] + "</td><td style='width: 15%;'>" + str(wait_time)[:-4]+ "</td></tr>"
        else: 
                mensaje = mensaje +  "<tr><td style='width: 100%; text-align: left;'>" + "No existen transacciones" + "</td> </tr>"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #BAD2FF; '>"   
        mensaje = mensaje + "FINISHED"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #FEFED9;'>"  
        mensaje = mensaje + cabecera
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%'>"   
        if dic_finished:                 
            for clave, valor in dic_finished.items(): 
                lender = valor[0]
                transaction = clave
                currrent_step = valor[1]
                cycle_time = updatedAt - createdAt
                wait_time = 0
                
                mensaje = mensaje +  "<tr><td style='width: 20%; text-align: left;'>" + str(lender_desc) + "</td><td style='width: 30%; text-align: left;'>" + str(transaction)  + "</td><td style='width: 20%; text-align: left;'>" +  str(currrent_step)+ "</td><td style='width: 15%;'>" + str(cycle_time)[:-4] + "</td><td style='width: 15%;'>No se mide</td></tr>"
        else: 
                mensaje = mensaje +  "<tr><td style='width: 100%; text-align: left;'>" + "No existen transacciones" + "</td> </tr>"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #BAD2FF; '>"   
        mensaje = mensaje + "ROUTING_RULE_NOT_MATCH"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #FEFED9;'>"  
        mensaje = mensaje + cabecera
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%'>" 
        if dic_routing_rule_not_match:            
            for clave, valor in dic_routing_rule_not_match.items():  
                lender = valor[0]
                transaction = clave
                currrent_step = valor[1]
                wait_time = updatedAt - createdAt
                cycle_time = email_date - updatedAt     

                mensaje = mensaje +  "<tr><td style='width: 20%; text-align: left;'>" + str(lender_desc) + "</td><td style='width: 30%; text-align: left;'>" + str(transaction)  + "</td><td style='width: 20%; text-align: left;'>" +  str(currrent_step)+ "</td><td style='width: 15%;'>" + str(cycle_time)[:-4] + "</td><td style='width: 15%;'>" + str(wait_time)[:-4]+ "</td></tr>"
        else: 
                mensaje = mensaje +  "<tr><td style='width: 100%; text-align: left;'>" + "No existen transacciones" + "</td> </tr>"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #BAD2FF; '>"   
        mensaje = mensaje + "CONFIRMED"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "<table width='100%' style='background-color: #FEFED9;'>"  
        mensaje = mensaje + cabecera
        mensaje = mensaje + "</table>"
                
        mensaje = mensaje + "<table width='100%'>" 
        if dic_confirmed:           
            for clave, valor in dic_confirmed.items():  
                lender = valor[0]
                transaction = clave
                currrent_step = valor[1]
                cycle_time = email_date - createdAt 
                wait_time = email_date - updatedAt 

                mensaje = mensaje +  "<tr><td style='width: 20%; text-align: left;'>" + str(lender_desc) + "</td><td style='width: 30%; text-align: left;'>" + str(transaction)  + "</td><td style='width: 20%; text-align: left;'>" +  str(currrent_step)+ "</td><td style='width: 15%;'>" + str(cycle_time)[:-4] + "</td><td style='width: 15%;'>" + str(wait_time)[:-4]+ "</td></tr>"
        else: 
                mensaje = mensaje +  "<tr><td style='width: 100%; text-align: left;'>" + "No existen transacciones" + "</td> </tr>"
        mensaje = mensaje + "</table>"

        mensaje = mensaje + "</table>"

    except Exception as exception: 
         logging.error("Error - %s." % str(exception))
     
    if flow.conn:
        flow.conn.close()
         
    mail.sendEmailFlowsStepsStatus_html_v2(mensaje, email_date)
 