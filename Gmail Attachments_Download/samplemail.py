import imaplib, email, os
import getpass

user = input('Enter your Gmail username:')
password = getpass.getpass('Enter your password:')   #https://myaccount.google.com/lesssecureapps
imap_url = 'imap.gmail.com'

#Where you want your attachments to be saved
#attachment_dir= r'C:/pyprojects/Gmail/Mail_Attachments'
attachment_dir = os.path.join(os.getcwd(),r"Mail_Attachments")
if not os.path.exists(attachment_dir):
    os.makedirs(attachment_dir)

# sets up the auth
def auth(user,password,imap_url):
    con = imaplib.IMAP4_SSL(imap_url)
    con.login(user,password)
    return con

# allows you to download attachments
def get_attachments(msg):
    for part in msg.walk():
        if part.get_content_maintype()=='multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        fileName = part.get_filename()

        if bool(fileName):
            filePath = os.path.join(attachment_dir, fileName)
            with open(filePath,'wb') as f:
                f.write(part.get_payload(decode=True))


con = auth(user,password,imap_url)
con.select('INBOX')

result, data = con.uid('search',None,"ALL")
inbox_list=data[0].split()[-1:]

for no in inbox_list:
    result, data = con.uid('fetch',no,'(RFC822)')
    raw = email.message_from_bytes(data[0][1])
    get_attachments(raw)
