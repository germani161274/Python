import logging

from pymongo import MongoClient
from email.mime.text import MIMEText

class Mails:

    def __init__(self):
        None

    def sendEmailLoans(self, mensaje,email_date, contador_loan, contador_ayer, flag_duplicados):

        try:
            
            import smtplib, ssl

            port = 465  # For SSL

            smtp_server = "smtp.mail.us-east-1.awsapps.com"
            sender_email = "monitoring@kalaplatform.tech"
            password = "62KCyZ441Xb13E21Hp7ixZAw254omTeH"

            receiver_email = ["german@kala.tech", "benja@kala.tech","pablo@kala.tech","camilo@kala.tech","rodolfo@kala.tech"]  #"german@kala.tech"  # Enter receiver address
            #receiver_email = ["german@kala.tech"]

            subject = "Control de loans del dia: " + str(email_date.strftime("%d/%m/%Y"))

            # Crear el mensaje de correo electrónico
            message = MIMEText(mensaje)
            message["Subject"] = subject
            message["From"] = sender_email
            message["To"] = ", ".join(receiver_email)

            # Establecer la conexión al servidor SMTP y enviar el correo
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, message.as_string())


        except Exception as exception: 
            logging.error("Error sendEmailLoans - %s." % str(exception))