import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time

class Mails:
    def __init__(self):
        None

    def sendEmailFlowsStepsStatus_html_v2(self, mensaje, email_date):
        try:
            port = 465  # Para SSL
            smtp_server = "smtp.mail.us-east-1.awsapps.com"
            sender_email = "monitoring@kalaplatform.tech"
            password = "62KCyZ441Xb13E21Hp7ixZAw254omTeH"
            #receiver_email = ["german@kala.tech", "benja@kala.tech","pablo@kala.tech","camilo@kala.tech","rodolfo@kala.tech","manuel@kala.tech"] 
            receiver_email = ["german@kala.tech"]

            subject = "Control de status of flows 2.0 del dia: " + str(email_date.strftime("%d/%m/%Y")) + " a las " + str(
                time.localtime().tm_hour) + ":" + str(time.localtime().tm_min) + " hs"

            # Crear el mensaje de correo electrónico como multipart
            message = MIMEMultipart()
            message["Subject"] = subject
            message["From"] = sender_email
            message["To"] = ", ".join(receiver_email)
           

            # Agregar cuerpo del mensaje en formato HTML
            body_html = """
            <html>
            <body>
                <p>{}</p>
            </body>
            </html>
            """.format(mensaje)

            message.attach(MIMEText(body_html, "html"))

            # Establecer la conexión al servidor SMTP y enviar el correo
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, message.as_string())

        except Exception as exception: 
            logging.error("Error sendEmailFlows - %s." % str(exception))
