import logging
import psycopg2
import requests
from datetime import datetime
from postgresqlConn import PostgresConn

if __name__ == '__main__':

    postgresqlConn = PostgresConn()

    # Where USD is the base currency you want to use
    url = 'https://v6.exchangerate-api.com/v6/3e041bc42b89cbd40007525d/latest/USD'

    # Making our request
    response = requests.get(url)
    data = response.json()

    cop_exchange_rate = data['conversion_rates']['COP']
    print(f'Tipo de cambio para AED: {cop_exchange_rate}')

    # Acceder a la fecha en formato UTC
    fecha_utc_str = data['time_last_update_utc']

    fecha_utc_datetime = datetime.strptime(fecha_utc_str, '%a, %d %b %Y %H:%M:%S +0000')

    # Obtener el timestamp sin zona horaria
    timestamp_sin_zona = fecha_utc_datetime.timestamp()
    print(f'Timestamp sin zona horaria: {timestamp_sin_zona}')
    print(f'Fecha en formato UTC: {fecha_utc_str}')

    # Your JSON object
    #print (data)

    #transaction_manager.transaction
    borraTransaction = postgresqlConn.insertExchangeRate(id_currency, date, conversion_rate)   

    def insertExchangeRate(self, id_currency, date, conversion_rate):
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
            cursor.execute("insert into management.exchange_rate_v2 (id_currency, date, conversion_rate) values ("+id_currency+", "+date+", "+conversion_rate+")")
 
            conn.commit()

            cursor.close()

        except psycopg2.Error as e:
            logging.error("Error getQtyCountry_v2 - %s." % str(e))
            
        finally:
            if conn is not None:
                conn.close()   
		