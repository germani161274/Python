from io import BytesIO
from datetime import datetime, timedelta

from loanTotal import LoanTotal
from sendEmailLoansTotal import Mails
import matplotlib.pyplot as plt


if __name__ == '__main__':

    ##############################################################################################################################################
    ###    Control loanTotals en loan
    ##############################################################################################################################################
 
    mail = Mails()
    
    email_date = datetime.now()
    #email_date = email_date - timedelta(days=1)

    dia = email_date.day
    mes = email_date.month
    ano = email_date.year

    date = datetime(ano, mes, dia)
    #date = date - timedelta(days=1)

    orgId = ''

    loanTotal = LoanTotal() 
    total = 0
    mensaje = ""

    dic = {}

    distinct_organization_ids = loanTotal.getDistinctOrganizationId()

    message_body = f'<p>Cantidades:</p><ul>'

    for item in distinct_organization_ids :

        if str(item)  == "org-003":
            desc =  "FRUBANA BR"   
            contador_loanTotal = 0
            contador_loanTotal = loanTotal.getQtyTotalLoansByDateandOrgId(date, str(item))
            total = total + contador_loanTotal
            mensaje = mensaje + "\n" + "Organization: " + str(item) + " - " + desc + ": " + str(contador_loanTotal)
            mensaje = mensaje + "\n"
   
            dic[desc] = contador_loanTotal    
            message_body += f'<li>{desc}: {contador_loanTotal}</li>'

        if str(item)  == "org-005":
            desc =  "FRUBANA MX"   
            contador_loanTotal = 0
            contador_loanTotal = loanTotal.getQtyTotalLoansByDateandOrgId(date,str(item))
            total = total + contador_loanTotal
            mensaje = mensaje + "\n" + "Organization: " + str(item) + " - " + desc + ": " + str(contador_loanTotal)
            mensaje = mensaje + "\n"
   
            dic[desc] = contador_loanTotal    
            message_body += f'<li>{desc}: {contador_loanTotal}</li>'

        if str(item)  == "org-002":
            desc =  "FRUBANA COL"  
            contador_loanTotal = 0
            contador_loanTotal = loanTotal.getQtyTotalLoansByDateandOrgId(date,str(item))
            total = total + contador_loanTotal
            mensaje = mensaje + "\n" + "Organization: " + str(item) + " - " + desc + ": " + str(contador_loanTotal) 
            mensaje = mensaje + "\n"
   
            dic[desc] = contador_loanTotal    
            message_body += f'<li>{desc}: {contador_loanTotal}</li>'

        if str(item)  == "org-039":
            desc =  "FINANCIA-SEGURO"  
            contador_loanTotal = 0
            contador_loanTotal = loanTotal.getQtyTotalLoansByDateandOrgId(date,str(item))
            total = total + contador_loanTotal
            mensaje = mensaje + "\n" + "Organization: " + str(item) + " - " + desc + ": " + str(contador_loanTotal)
            mensaje = mensaje + "\n"
   
            dic[desc] = contador_loanTotal    
            message_body += f'<li>{desc}: {contador_loanTotal}</li>'

    message_body += '<br>'
    message_body += f'<li>Total: {total}</li>'
    message_body += '</ul>'
    """
    # Crear gráfico a partir del diccionario
    labels, values = zip(*dic.items())
    plt.bar(labels, values)
    #plt.xlabel('Keys')
    #plt.ylabel('Values')
    plt.title('Loans generados por Kala')

    # Guardar el gráfico en un BytesIO buffer
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    plt.close()
    """
    # Crear gráfico de torta con matplotlib
    labels, values = zip(*dic.items())
    plt.pie(values, labels=labels, autopct='%1.1f%%')
    #plt.title('Porcentaje por Lender')

    # Guardar el gráfico en un BytesIO buffer
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    plt.close()
    buffer.seek(0)

    mail.sendEmailLoansTotal(message_body, email_date, buffer)
 
    