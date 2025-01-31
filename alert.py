import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os

# Need to connect to UAlberta VPN or be on UAlberta network to send email.
# https://universityofalberta.freshservice.com/support/solutions/articles/19000109142

with open('email_cred.txt', 'r') as f:
    email = f.readline().strip()
    password = f.readline().strip()

def send_email(subject:str, body:str, attachment, to:list[str], from_:str = email, password:str = password) -> bool:
    
    try:
        ccid = from_.split('@')[0]
        # Create message container
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = from_
        msg['To'] = ', '.join(to)

        # Add body
        msg.attach(MIMEText(body, 'plain'))

        # Add attachment if provided
        if attachment:
            if isinstance(attachment, str) and os.path.exists(attachment):
                # If attachment is a file path
                with open(attachment, 'rb') as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(attachment))
            else:
                # If attachment is file-like object or bytes
                part = MIMEApplication(attachment, Name='attachment')
            
            part['Content-Disposition'] = f'attachment; filename="{part.get_param("Name")}"'
            msg.attach(part)

        # Set up SMTP server
        server = smtplib.SMTP_SSL('smtp.ualberta.ca', 465)
        server.ehlo()
        server.login(ccid, password)

        # Send email
        server.send_message(msg)
        server.quit()
        print(f"Email sent to {', '.join(to)}")
        return True

    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        return False
    
if __name__ == "__main__":
    send_email('Test', 'This is a test email', 'log.txt', ['hhpeng@ualberta.ca'])