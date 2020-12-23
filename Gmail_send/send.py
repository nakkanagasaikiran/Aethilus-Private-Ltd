import smtplib
from cred import my_mail,my_pwd

s = smtplib.SMTP('smtp.gmail.com',587) #host, port

s.starttls()
s.login(my_mail,my_pwd)

message = 'hello this a check for sending emails from a file using python'

with open('emails.txt','r') as f:
    receiver_mail = f.read()
    receiver_mail=receiver_mail.replace("\n",'')
    f.close()
print(receiver_mail)

s.sendmail(my_mail,receiver_mail,message)

s.quit()

print('successfully sent..!')
