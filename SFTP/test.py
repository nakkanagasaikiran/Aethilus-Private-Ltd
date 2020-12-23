import test_cred
import logging
import logging.handlers
import sys
import os

log = logging.getLogger(name='example.log')
log.setLevel(logging.INFO)
fmtstr = "%(asctime)s: (%(filename)s): %(levelname)s: %(funcName)s Line: %(lineno)d - %(message)s"
log_fmt = logging.Formatter(fmtstr)
filehandler = logging.handlers.RotatingFileHandler(r'C:/pyprojects/Local/example.log', mode='a')
filehandler.setLevel(logging.INFO)
filehandler.setFormatter(log_fmt)
log.addHandler(filehandler)

def download_upload_2_sftp():
    # decrepting encrepted SFTP credentails
    log.info("Begin Decryting SFTP credentials")
    host_name=test_cred.site_sftp_host_name
    pswd=test_cred.ciper_decrept.decrypt(test_cred.encrpt_site_sftp_password)
    usr_nm=test_cred.ciper_decrept.decrypt(test_cred.encrpt_site_sftp_username)
    log.info("After Decryting SFTP credentials")

    try:
        log.info("Before Paramiko Invoke")
        import paramiko
        transport = paramiko.Transport((host_name,22))
        transport.connect(username=usr_nm, password=pswd)
        sftp = paramiko.SFTPClient.from_transport(transport)
        log.info("After Paramiko Invoke")

    except paramiko.SSHException as error:
        log.critical(f"Error: {error} - please check user name, password, host credentials")
        log.info("Exiting Interface")
        sys.exit()

    try:
        log.info("Before SFTP Call")
        print("SFTP Server Root files: ",os.listdir("C:/SFTP/data"))
        filename=input("Enter the file name with extention : ")
        sftp.get(filename,test_cred.One_drive_file_path+'/'+filename) #....server file download into a local system....
        log.info("file downloaded from the server...")

        log.info("Doing some modifications in that file")
        import pyexcel as pe
        sheet = pe.get_sheet(file_name=filename)
        sheet.column+=["col6",13, 14, 15,18]
        sheet.row+=["row6",64,86,34,42]
        sheet.save_as("modified_"+filename)
        log.info("After modified save that file is modified_filename")

        sftp.put("modified_"+filename,'/./modified_'+filename) #...local system file upload into a server
        log.info("modified file upload into the server...")
        log.info("After SFTP Call")

    except  OSError as error:
        log.warning(error)

    except Exception as error:
        log.error(error)

    finally:
        try:
            log.info("Before SFTP Close")
            sftp.close()
            log.info("After SFTP Close")
        except:
            pass

if __name__=='__main__':
    log.info("***************************Start of SFTP Process*********************************************")
    log.info("Calling download_upload_2_sftp")
    download_upload_2_sftp()
    log.info("***************************End of download_upload_2_sftp call**************************************")
