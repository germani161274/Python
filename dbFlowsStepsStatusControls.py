import logging
from datetime import datetime, timedelta
from flowsStepsStatus import Flow
from sendEmailFlowsStepsStatus_html import Mails
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
    
    email_date = datetime.now() + timedelta(hours=3)

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
    dic_finished_final = {}
    dic_error_final = {}
    dic_failed_final = {}
    dic_pending_final = {}
    dic_in_progress_final = {}

    dic_lender_tx_date = {}

    flow = Flow() 
    flowStatusList = flow.getStatusOfFlow()      
    if flow.conn:
        flow.conn.close()
    mensaje = ''

    try:
        # Nombre del archivo para guardar los resultados
        nombre_archivo = "Status_of_flow_"+ str(datetime.now())+".json"

        # Guardar los resultados en un archivo JSON dentro de la carpeta
        #ruta_archivo = os.path.join(nombre_archivo)

        # Guardar los resultados en un archivo JSON
        #with open(ruta_archivo, 'w') as archivo:
            # Escribir todos los documentos en el archivo
        document_str = json.dumps(flowStatusList, default=json_util.default)
            #archivo.write(json.dumps(document_str))
            #archivo.write("\n")

        datos = json.loads(document_str)

            # Recorrer la estructura de datos para encontrar los steps con status "inProgress"
        for etapa in datos:
                ################################################################################################
                # Extraer el valor binario del campo lenderId
                lenderId = etapa.get('lenderId')
              
                # Extraer el valor base 64 (no funcionaba la funcion asi que lo hice a mano)
                lenderId_str = str(lenderId)
                lenderId_str = lenderId_str[24:] 
                lenderId_str = lenderId_str[:-20] 
                base64_value = lenderId_str

                # Decodificar la cadena base64
                binary_data = base64.b64decode(base64_value)

                # Convertir el valor binario a un objeto UUID
                uuid_from_binary = uuid.UUID(bytes=binary_data)
                    
                # Pisa lo de arriba 
                lenderId = "UUID('{}')".format(uuid_from_binary)
                lenderId_str = lenderId[6:] 
                lenderId_str = lenderId_str[:-2] 

                #lender = Lender()
                #desc_lender = lender.getLender(lenderId_str)    
                #if lender.conn:
                #  lender.conn.close()
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
               

                #transactionId = "UUID('{}')".format(uuid_from_binary_tx)
                
                #transactionId_str = transactionId[6:] 
                #transactionId_str = transactionId_str[:-3] 

                stages = etapa.get("stages", [])
                for stage in stages:
                    steps = stage.get("steps", [])
                    for step in steps:
                    
                        fecha_str1 =  step.get("updatedAt")
                        fecha_str1 = fecha_str1['$date']
                        fecha1 = datetime.fromisoformat(fecha_str1[:-1])  # Quitar 'Z' al final  
                        fecha_str2 = email_date
                        fecha2 = fecha_str2
                                        
                        # Se insertan en diferentes diccionarios las transaciones de acuerdo al estado de los steps  
                        if step.get("status") == "ERROR": 
                            if not dic_error.get(transactionId_str): 
                                dic_error[transactionId_str] = (lenderId_str , fecha1)   
                        if step.get("status") == "FAILED":  
                            if not dic_error.get(transactionId_str): 
                                if not dic_failed.get(transactionId_str): 
                                    dic_failed[transactionId_str] = (lenderId_str , fecha1)  
                        if step.get("status") == "PENDING":  
                            if not dic_error.get(transactionId_str): 
                                if not dic_failed.get(transactionId_str):      

                                    val = dic_pending.get(transactionId_str)    
                                    if val:
                                       fecha = val[1]

                                    if not dic_pending.get(transactionId_str) or fecha1 < fecha: 
                                        dic_pending[transactionId_str] = (lenderId_str , fecha1)   
                        if step.get("status") == "IN_PROGRESS": 
                            if not dic_error.get(transactionId_str): 
                                if not dic_failed.get(transactionId_str):                               
                                    if not dic_pending.get(transactionId_str):       

                                        val = dic_in_progress.get(transactionId_str)    
                                        if val:
                                           fecha = val[1]

                                        if not dic_in_progress.get(transactionId_str) or fecha1 < fecha: 
                                            dic_in_progress[transactionId_str] = (lenderId_str , fecha1)   
                        if step.get("status") == "FINISHED": 
                            if not dic_error.get(transactionId_str): 
                                if not dic_failed.get(transactionId_str):                               
                                    if not dic_pending.get(transactionId_str):                                
                                        if not dic_in_progress.get(transactionId_str):    

                                            val = dic_in_progress.get(transactionId_str)    
                                            if val:
                                                fecha = val[1]                                            

                                            if not dic_finished.get(transactionId_str) or fecha1 > fecha: 
                                                dic_finished[transactionId_str] = (lenderId_str , fecha1)     
  
        # Se cargan las solicitudes error    
        for clave, valor in dic_error.items():
            dic_error_final[clave] = valor
        # Se cargan las solicitudes failed    
        for clave, valor in dic_failed.items():
            dic_failed_final[clave] = valor   
        for clave in dic_failed:   
            if dic_error_final.get(clave):
               del dic_failed_final[clave]   
        # Se cargan las solicitudes in progress
        for clave, valor in dic_in_progress.items():
            dic_in_progress_final[clave] = valor
        for clave in dic_in_progress:   
            if dic_error_final.get(clave) or dic_failed_final.get(clave) :
               del dic_in_progress_final[clave]  
        # Se cargan las solicitudes pending
        for clave, valor in dic_pending.items():
            dic_pending_final[clave] = valor
        for clave in dic_pending:   
            if dic_error_final.get(clave) or dic_failed_final.get(clave) or dic_in_progress_final.get(clave):
               del dic_pending_final[clave]  
        # Se cargan las solicitudes finalizadas
        for clave, valor in dic_finished.items():
            dic_finished_final[clave] = valor
        for clave in dic_finished:   
            if dic_error_final.get(clave) or dic_failed_final.get(clave) or dic_pending_final.get(clave) or dic_in_progress_final.get(clave) :
               del dic_finished_final[clave]    

        #------------------------------------------------------------------------------------------------------------------------------------

        # Esto es para sumar por lender por status
        for clave, valor in dic_error_final.items():           
            status = "ERROR      "
            if dic_lender.get(valor[0] + ' - ' + status): 
                dic_lender[valor[0] + ' - '  + status] = dic_lender[valor[0] + ' - ' + status] + 1
            else:
                dic_lender[valor[0] + ' - '  + status] = 1  

        for clave, valor in dic_failed_final.items():           
            status = "FAILED     "
            if dic_lender.get(valor[0] + ' - ' + status): 
                dic_lender[valor[0] + ' - '  + status] = dic_lender[valor[0] + ' - ' + status] + 1
            else:
                dic_lender[valor[0] + ' - '  + status] = 1  

        for clave, valor in dic_pending_final.items():           
            status = "PENDING    "
            if dic_lender.get(valor[0] + ' - ' + status): 
                dic_lender[valor[0] + ' - '  + status] = dic_lender[valor[0] + ' - ' + status] + 1
            else:
                dic_lender[valor[0] + ' - '  + status] = 1  

        for clave, valor in dic_in_progress_final.items():           
            status = "IN_PROGRESS"
            if dic_lender.get(valor[0] + ' - ' + status): 
                dic_lender[valor[0] + ' - '  + status] = dic_lender[valor[0] + ' - ' + status] + 1
            else:
                dic_lender[valor[0] + ' - '  + status] = 1  

        for clave, valor in dic_finished_final.items():           
            status = "FINISHED   "
            if dic_lender.get(valor[0] + ' - ' + status): 
                dic_lender[valor[0] + ' - '  + status] = dic_lender[valor[0] + ' - ' + status] + 1
            else:
                dic_lender[valor[0] + ' - '  + status] = 1  
 
        # Se ordena la coleccion
        dic_lender = dict(sorted(dic_lender.items(), key=lambda x: x[0]))
 
        mensaje = mensaje + "<br>" + "<h3 style='color: #14044F;'>Como se definen los estados de las transacciones y el tiempo en el mismo ?</h3>" 
        mensaje = mensaje + "ERROR (al menos un step en ERROR)"     
        mensaje = mensaje + "<br>" + "FAILED (al menos un step en FAILED y ninguno en ERROR)"     
        mensaje = mensaje + "<br>" + "IN_PROGRESS (al menos un step IN_PROGRESS y ninguno en FAILED ni en ERROR y para el calculo se toma la fecha del primer step)"                             
        mensaje = mensaje + "<br>" + "PENDING (al menos un step PENDING y ninguno en FAILED, en ERROR ni en IN_PROGRESS y para el calculo se toma la fecha del primer step)"        
        mensaje = mensaje + "<br>" + "FINISHED (todos los steps FINISHED y para el calculo se toma la fecha del último step)"
        mensaje = mensaje + "<br>"+ " "

        mensaje = mensaje + "<br>" + "<h3 style='color: #14044F;'>Total de transacciones por estado en cada Lender</h3>" 
        ant = ""
        mensaje = mensaje + "<table width='50%' border='1'>"
        for clave in dic_lender:         
            primquince = str(clave)[:15] 
            if primquince != ant :   
                if "8901c37b-9ef1-42e5-9bff-4df41e508ca3" in str(clave): 
                   
                    mensaje = mensaje + "<tr style='background-color: #F5FDD8;'><th style='color: #428A33;' >" + "<h2>IMAGINA MX</h2></th></tr>"  

                    mensaje = mensaje + "<tr style='background-color: lightblue;'><td>Estado</td><td>Cantidad</td></tr>"               
                    mensaje = mensaje + "<tr><td>" + (str(clave)[-11:]   + "</td><td>" + str(dic_lender[clave])) + "</b></td></tr>"
                                 
                if "97799c15-69bd-4d08-af11-cec65c06ef2b" in str(clave):  
                   
                    mensaje = mensaje + "<tr style='background-color: #F5FDD8;'><th style='color: #428A33;' >" + "<h2>FODUM</h2></th></tr>" 
                                     
                    mensaje = mensaje + "<tr style='background-color: lightblue;'><td style='background-color: lightblue;'>Estado</td><td>Cantidad</td></tr>"               
                    mensaje = mensaje + "<tr><td>" + (str(clave)[-11:]   + "</td><td>" + str(dic_lender[clave])) + "</b></td></tr>"              
                if "db100629-b567-4aef-93e4-7f5377779ae4" in str(clave):  
                     
                    mensaje = mensaje + "<tr style='background-color: #F5FDD8;'><th style='color: #428A33;' >" + "<h2>ISENTIES CHAUVET</h2></th></tr>" 
                    
                    mensaje = mensaje + "<tr style='background-color: lightblue;'><td style='background-color: lightblue;'>Estado</td><td>Cantidad</td></tr>"               
                    mensaje = mensaje + "<tr><td>" + (str(clave)[-11:]   + "</td><td>" + str(dic_lender[clave])) + "</b></td></tr>"              
                ant = primquince
            else :   
                if "8901c37b-9ef1-42e5-9bff-4df41e508ca3" in str(clave):                             
                    mensaje = mensaje + "<tr><td>" + (str(clave)[-11:]   + "</td><td>" + str(dic_lender[clave])) + "</b></td></tr>"              
                if "97799c15-69bd-4d08-af11-cec65c06ef2b" in str(clave):                             
                    mensaje = mensaje + "<tr><td>" + (str(clave)[-11:]   + "</td><td>" + str(dic_lender[clave])) + "</b></td></tr>"              
                if "db100629-b567-4aef-93e4-7f5377779ae4" in str(clave):                                            
                    mensaje = mensaje + "<tr><td>" + (str(clave)[-11:]   + "</td><td>" + str(dic_lender[clave])) + "</b></td></tr>"              
        mensaje = mensaje + "</table>"


        mensaje = mensaje + "<br>" + "<h3 style='color: #14044F;'>Detalle de transacciones por estado en cada Lender</h3>" 
        mensaje = mensaje + "<br>"  
        mensaje = mensaje + "<table width='80%' border='1'>"
        ant = ""
        for clave in dic_lender:         
            primquince = str(clave)[:15] 
            if primquince != ant :   

                if "8901c37b-9ef1-42e5-9bff-4df41e508ca3" in str(clave):                
                    mensaje = mensaje + "<table width='100%'>"    
                    mensaje = mensaje + "<tr style='background-color: #FEFDDD;'><th style='color: #555B90;' >" + "<h2>IMAGINA MX</h2></th></tr>" 
                    mensaje = mensaje + "</table>" 
                    mensaje = mensaje + "<table width='100%' border='1'>" 
                    #mensaje = mensaje + "<tr><td style='background-color: lightblue;'><b>Estado: " + (str(clave)[-11:]   + " - Cantidad: " + str(dic_lender[clave])) + "</b></td></tr>" 
                    estado= str(clave)[-11:]
                    cantidad = dic_lender[clave]
                    mensaje = mensaje + "<tr style='background-color: lightblue;'><td style='width: 20%;'><b>Estado</b></td><td style='width: 45%;'><b>Transacción</b></td><td style='width: 35%;'><b>Días</b></td></tr>" 
                    if str(clave)[-11:] == "ERROR      ":
                       for clave, valor in dic_error_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #F8240F;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"   
                    if str(clave)[-11:] == "FAILED     ":
                       for clave, valor in dic_failed_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FFE8F3;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"   
                    if str(clave)[-11:] == "IN_PROGRESS":
                       for clave, valor in dic_in_progress_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #E5FEFE;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"                                
                    if str(clave)[-11:] == "PENDING    ":
                       for clave, valor in dic_pending_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FCE5B5;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "FINISHED   ":
                       for clave, valor in dic_finished_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #DAFDC7;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"       
                    mensaje = mensaje + "</table>"
                if "97799c15-69bd-4d08-af11-cec65c06ef2b" in str(clave):  

                    mensaje = mensaje + "<table width='100%'>"    
                    mensaje = mensaje + "<tr style='background-color: #FEFDDD;'><th style='color: #555B90;' >" + "<h2>FODUN</h2></th></tr>" 
                    mensaje = mensaje + "</table>" 
                    mensaje = mensaje + "<table width='100%' border='1'>" 
                    #mensaje = mensaje + "<tr><td style='background-color: lightblue;'><b>Estado: " + (str(clave)[-11:]   + " - Cantidad: " + str(dic_lender[clave])) + "</b></td></tr>" 
                    estado= str(clave)[-11:]
                    mensaje = mensaje + "<tr style='background-color: lightblue;'><td style='width: 20%;'><b>Estado</b></td><td style='width: 45%;'><b>Transacción</b></td><td style='width: 35%;'><b>Días</b></td></tr>" 
                    if str(clave)[-11:] == "ERROR      ":
                       for clave, valor in dic_error_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #F8240F;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "FAILED     ":
                       for clave, valor in dic_failed_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FFE8F3;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"  
                    if str(clave)[-11:] == "IN_PROGRESS":
                       for clave, valor in dic_in_progress_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #E5FEFE;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"                              
                    if str(clave)[-11:] == "PENDING    ":
                       for clave, valor in dic_pending_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FCE5B5;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"  
                    if str(clave)[-11:] == "FINISHED   ":
                       for clave, valor in dic_finished_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #DAFDC7;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"   
                    mensaje = mensaje + "</table>"
                if "db100629-b567-4aef-93e4-7f5377779ae4" in str(clave):  
                    mensaje = mensaje + "<table width='100%'>"    
                    mensaje = mensaje + "<tr style='background-color: #FEFDDD;'><th style='color: #555B90;'>" + "<h2>SENTIES CHAUVET</h2></th></tr>" 
                    mensaje = mensaje + "</table>" 
                    mensaje = mensaje + "<table width='100%' border='1'>" 
                    #mensaje = mensaje + "<tr><td style='background-color: lightblue;'><b>Estado: " + (str(clave)[-11:]   + " - Cantidad: " + str(dic_lender[clave])) + "</b></td></tr>" 
                    estado= str(clave)[-11:]
                    mensaje = mensaje + "<tr style='background-color: lightblue;'><td style='width: 20%;'><b>Estado</b></td><td style='width: 45%;'><b>Transacción</b></td><td style='width: 35%;'><b>Días</b></td></tr>" 
                    if str(clave)[-11:] == "ERROR      ":
                       for clave, valor in dic_error_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #F8240F;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"      
                    if str(clave)[-11:] == "FAILED     ":
                       for clave, valor in dic_failed_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FFE8F3;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "IN_PROGRESS":
                       for clave, valor in dic_in_progress_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #E5FEFE;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"                               
                    if str(clave)[-11:] == "PENDING    ":
                       for clave, valor in dic_pending_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FCE5B5;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "FINISHED   ":
                       for clave, valor in dic_finished_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #DAFDC7;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"                                                                           
                    mensaje = mensaje + "</table>"
                ant = primquince
            else :   

                if "8901c37b-9ef1-42e5-9bff-4df41e508ca3" in str(clave):                

                    mensaje = mensaje + "<table width='100%' border='1'>" 
                    estado= str(clave)[-11:]
                    #mensaje = mensaje + "<tr><td style='background-color: lightblue;'><b>Estado: " + (str(clave)[-11:]   + " - Cantidad: " + str(dic_lender[clave])) + "</b></td></tr>"  

                    if str(clave)[-11:] == "ERROR      ":
                       for clave, valor in dic_error_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #F8240F;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"      
                    if str(clave)[-11:] == "FAILED     ":
                       for clave, valor in dic_failed_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FFE8F3;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "IN_PROGRESS":
                       for clave, valor in dic_in_progress_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #E5FEFE;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"                                
                    if str(clave)[-11:] == "PENDING    ":
                       for clave, valor in dic_pending_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FCE5B5;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "FINISHED   ":
                       for clave, valor in dic_finished_final.items():      
                            if str(valor[0]) == "8901c37b-9ef1-42e5-9bff-4df41e508ca3":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #DAFDC7;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"     
                    mensaje = mensaje + "</table>"
                if "97799c15-69bd-4d08-af11-cec65c06ef2b" in str(clave):                 

                    mensaje = mensaje + "<table width='100%' border='1'>" 
                    estado= str(clave)[-11:]
                    #mensaje = mensaje + "<tr><td style='background-color: lightblue;'><b>Estado: " + (str(clave)[-11:]   + " - Cantidad: " + str(dic_lender[clave])) + "</b></td></tr>"  

                    if str(clave)[-11:] == "ERROR      ":
                       for clave, valor in dic_error_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #F8240F;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"      
                    if str(clave)[-11:] == "FAILED     ":
                       for clave, valor in dic_failed_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FFE8F3;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "IN_PROGRESS":
                       for clave, valor in dic_in_progress_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #E5FEFE;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"                                
                    if str(clave)[-11:] == "PENDING    ":
                       for clave, valor in dic_pending_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FCE5B5;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "FINISHED   ":
                       for clave, valor in dic_finished_final.items():      
                            if str(valor[0]) == "97799c15-69bd-4d08-af11-cec65c06ef2b":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #DAFDC7;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"      
                    mensaje = mensaje + "</table>"
                if "db100629-b567-4aef-93e4-7f5377779ae4" in str(clave):               

                    mensaje = mensaje + "<table width='100%' border='1'>" 
                    estado= str(clave)[-11:]
                    #mensaje = mensaje + "<tr><td style='background-color: lightblue;'><b>Estado: " + (str(clave)[-11:]   + " - Cantidad: " + str(dic_lender[clave])) + "</b></td></tr>"  

                    if str(clave)[-11:] == "ERROR      ":
                       for clave, valor in dic_error_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #F8240F;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"      
                    if str(clave)[-11:] == "FAILED     ":
                       for clave, valor in dic_failed_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FFE8F3;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "IN_PROGRESS":
                       for clave, valor in dic_in_progress_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #E5FEFE;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"                                
                    if str(clave)[-11:] == "PENDING    ":
                       for clave, valor in dic_pending_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #FCE5B5;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"    
                    if str(clave)[-11:] == "FINISHED   ":
                       for clave, valor in dic_finished_final.items():      
                            if str(valor[0]) == "db100629-b567-4aef-93e4-7f5377779ae4":
                                diferencia = fecha2 - valor[1]
                                mensaje = mensaje + "<tr style='background-color: #DAFDC7;'><td style='width: 20%;'>" + estado  + "</td><td style='width: 45%;'>" + str(clave) + "</td><td style='width: 35%;'>" + str(diferencia).replace("days","días").replace("day","día") + "</td><tr>"                          
                    mensaje = mensaje + "</table>"
        mensaje = mensaje + "</table>"   

    except Exception as exception: 
         logging.error("Error - %s." % str(exception))
     
    if flow.conn:
        flow.conn.close()
      
    mail.sendEmailFlowsStepsStatus_html(mensaje, email_date)
 