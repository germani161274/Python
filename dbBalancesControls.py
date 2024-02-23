import logging
from datetime import datetime, timedelta

from balance import Balance
from sendEmailBalance import Mails


if __name__ == '__main__':
    
    ##############################################################################################################################################
    ###    Control loans en balances
    ##############################################################################################################################################
 
    mail = Mails()
    
    email_date = datetime.now()

    dia = email_date.day
    mes = email_date.month
    ano = email_date.year

    date = datetime(ano, mes, dia)

    date_ant = date - timedelta(days=1)
    #date_ant = datetime(ano, mes, dia-1)

    #logging.basicConfig(
    #                filename='Control_balances_'+ str(date) +'.log',
    #                level=logging.INFO,
    #                format='%(message)s',
    #                datefmt='%Y-%m-%d %H:%M:%S')
    
    orgId = ''

    correctos=0
    duplicados=0
    inexistentes=0

    dic = {}
    dic_org = {}     

    contador_ayer = 0

    balance = Balance() 
    contador_ayer = balance.getQtyBalancesByDate(date_ant)      
    if balance.conn:
       balance.conn.close()

    balanceList = balance.getBalancesByDate(date) 
    contador_loan = 0

    for item in balanceList:

        contador_loan = contador_loan + 1
                   
        try:        
                orgId = item['organizationId']
                loanId = item['loanId']                  
               
                if dic.get(loanId + ' - ' + orgId): 
                    dic[orgId + ' - '  + loanId] = dic[orgId + ' - ' + loanId] + 1
                else:
                    dic[orgId + ' - '  + loanId] = 1    

                if dic_org.get(orgId): 
                    dic_org[orgId] = dic_org[orgId] + 1
                else:
                    dic_org[orgId] = 1    

        except Exception as exception: 
            logging.error("Error - %s." % str(exception))
   
    flag_duplicados = 0
    mensaje = ""

    for key in dic:
              
        if dic[key] > 1:         
           mensaje = mensaje + "\n" + (str(key) + "--> " + str(dic[key]))
           flag_duplicados = 1

    for key in dic_org:

        cant_org = balance.getQtyBalancesByDateandOrgId(date_ant, str(key)) 
        desc =  key

        if key == "org-003":
            desc =  "FRUBANA BR"            
        if key  == "org-005":
            desc =  "FRUBANA MX"        
        if key  == 'org-002':
            desc =  "FRUBANA COL"        
        if key  == 'org-039':
            desc =  "FINANCIA-SEGURO"      
        if key  == 'org-001':
            desc =  "KALA" 
        if key  == 'org-044':
            desc =  "Demo Portfolio"       

        mensaje = mensaje + "\n" + "Organization: " + str(key) + " - " + desc
        mensaje = mensaje + "\n" + "Total de loans en balance ayer " + str(date_ant.strftime("%d/%m/%Y")) + ": " + str(cant_org-dic_org[key])     
        mensaje = mensaje + "\n" + "Total de loans en balance hoy " + str(email_date.strftime("%d/%m/%Y")) + ": " + str(dic_org[key]) 

        cant_ayer = cant_org-dic_org[key]
        cant_hoy = dic_org[key]
        porc = round((((cant_hoy - cant_ayer) * 100) / cant_ayer),2)

        mensaje = mensaje + "\n" + "Porcentaje de diferencia: " + str(porc) + "%" + "\n"

    if balance.conn:
        balance.conn.close()
      
    mail.sendEmailBalance(mensaje, email_date, contador_loan, contador_ayer - contador_loan, flag_duplicados)
    
 
  