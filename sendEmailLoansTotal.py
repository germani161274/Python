import logging

from pymongo import MongoClient
from email.mime.text import MIMEText

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

class Mails:

    def __init__(self):
        None

    def sendEmailLoansTotal(self, mensaje,email_date, buffer):

        try:
            
            import smtplib, ssl

            port = 465  # For SSL

            smtp_server = "smtp.mail.us-east-1.awsapps.com"
            sender_email = "monitoring@kalaplatform.tech"
            password = "62KCyZ441Xb13E21Hp7ixZAw254omTeH"
            
            receiver_email = ["german@kala.tech", "benja@kala.tech","pablo@kala.tech","camilo@kala.tech","rodolfo@kala.tech","manuel@kala.tech"]  #"german@kala.tech"  # Enter receiver address
            #receiver_email = ["german@kala.tech"]

            subject = "Total de Loans generados con Kala hasta el dia: " + str(email_date.strftime("%d/%m/%Y"))

            # Crear el mensaje de correo electrónico
            #message = MIMEText(mensaje)
            message = MIMEMultipart()
            message["Subject"] = subject
            message["From"] = sender_email
            message["To"] = ", ".join(receiver_email)

            # Adjuntar el gráfico en el cuerpo del correo como una imagen HTML
            buffer.seek(0)
            image_data = buffer.getvalue()
            image_cid = 'grafico.png'  # Identificador del contenido (Content ID)
            message.attach(MIMEText(f'<html><body><p>{mensaje}</p>'
                                    f'<img src="cid:{image_cid}" alt="Gráfico"></body></html>', 'html'))

            # Adjuntar la imagen al correo como un contenido embebido
            image = MIMEImage(image_data, name='grafico.png')
            image.add_header('Content-ID', f'<{image_cid}>')
            message.attach(image)


            # Establecer la conexión al servidor SMTP y enviar el correo
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, message.as_string())

        except Exception as exception: 
            logging.error("Error sendEmailLoans - %s." % str(exception))