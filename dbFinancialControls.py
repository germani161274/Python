from datetime import datetime, timedelta
from financialControlConn import MongoConn
import sys

def main(month, year):
    print(f"Mes a solicitar: {month}")
    print(f"Año a solicitar: {year}")

if __name__ == '__main__':

    print("")
    print("1 - DEV")
    print("2 - QA")
    print("3 - PROD")
    source = input("Seleccione el origen del que desea obtener dos datos:")

    if not source.isdigit() :
        print("Debe seleccionar 1 ,2 o 3.")
        sys.exit()  

    if int(source) != 1 and int(source) != 2  and int(source) != 3 :
        print("El parametro de origen deber estar entre 1 y 3.")
        sys.exit()

    month = input("Ingrese el mes a filtrar: ")
    year = input("Ingrese el año a filtrar: ")

    if not year.isdigit() :
        print("Los parametros deben ser numericos.")
        sys.exit()         

    if not month.isdigit() :
        print("Los parametros deben ser numericos.")
        sys.exit()  

    if int(month) < 1 or int(month) > 12:
        print("El parametro mes " + str(month)+ " deber estar entre 1 y 12.")
        sys.exit()

    if int(year) > datetime.now().year:
        print("El parametro año no debe ser mayor al año actual.")
        sys.exit()

    financialControlConn = MongoConn()

    source = int(source)
    
    print("Trayendo datos solicitados OlimpiaIntegration...")
    olimpiaIntegration = financialControlConn.bkOlimpiaIntegration(month, year,source)    
    print("Trayendo datos solicitados TruoraIntegration...")
    bkTruoraIntegration = financialControlConn.bkTruoraIntegration(month, year,source)    
    print("Trayendo datos solicitados AdoIntegration...")
    bkAdoIntegration = financialControlConn.bkAdoIntegration(month, year,source)  
    print("Trayendo datos solicitados CustomersExternalDataSummaryBuro...")
    bkCustomersExternalDataSummaryBuro= financialControlConn.bkCustomersExternalDataSummaryBuro(month, year,source)
    print("Trayendo datos solicitados CustomersExternalDataSummaryBackgroundChecks...")
    bkCustomersExternalDataSummaryBackgroundChecks = financialControlConn.bkCustomersExternalDataSummaryBackgroundChecks(month, year,source)
    print("Trayendo datos solicitados CrossCoreIntegration...")
    bkCrossCoreIntegration = financialControlConn.bkCrossCoreIntegration(month, year,source)
    
    print("Finalizado")    
