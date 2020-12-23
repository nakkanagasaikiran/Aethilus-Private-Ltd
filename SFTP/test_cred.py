One_drive_file_path = r'C:/pyprojects/Local'

site_sftp_port=22

site_sftp_host_name='192.168.2.4'

User_name='tester'

password='password'

from cryptography.fernet import Fernet
sharing_key=Fernet.generate_key()

ciper_encrept=Fernet(sharing_key)

encrpt_site_sftp_username =ciper_encrept.encrypt(User_name.encode('UTF-8'))

encrpt_site_sftp_password =ciper_encrept.encrypt(password.encode('UTF-8'))

ciper_decrept=Fernet(sharing_key)
