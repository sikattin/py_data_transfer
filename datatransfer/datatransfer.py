#-------------------------------------------------------------------------------
# Name:        datatransfer.py
# Purpose:     for data transfering to use scp.
#
# Author:      shikano.takeki
#
# Created:     14/02/2018
# Copyright:   (c) shikano.takeki 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
from osfile import fileope
from mylogger.logger import Logger
from connection.sshconn import SSHConn
from paramiko import BadHostKeyException, AuthenticationException, SSHException
from socket import error

class DataTransfer(object):
    """Transfer the data using scp."""

    def __init__(self,
                 hostname: str,
                 username: str,
                 keyfile_path=None,
                 password=None,
                 logpath=".",
                 errlog_path=".",
                 loglevel=None):
        """constructor

        keyfile_path or password arg is necessary.
        log file name is 'yymmdd_backup.log'
        error log file name is 'yymmdd_error.log'
        Args:
            param1 hostname: host to connect ssh.
                type: str
            param2 username: user name to connect ssh.
                type: str
            parma3 keyfile_path: private key file path.
                type: str
            param4 password: user password
                type: str
                recommmend to receive from standard input.
            param5 logpath: log file path. default is current.
                type: str
            param6 errlog_path: error log file path. default is current.
                type: str

        """
        from datetime import datetime
        if keyfile_path is None and password is None:
            raise TypeError("keyfile_path or password arg is necessary.")
        # ロガーのセットアップ.
        if loglevel is None:
            loglevel = 30
        Logger.loglevel = loglevel
        self._logger = Logger(str(self))

        # hostname to connect ssh
        self.ssh_host = hostname
        # username
        self.ssh_user = username
        # private key file
        self.private_key = keyfile_path
        # password
        self.password = password
        # log file root path.
        self.log_root = logpath
        # error log file root path.
        self.errlog_root = errlog_path
        # YYYYmmdd
        self.ymd = datetime.now().strftime("%Y") + \
                   datetime.now().strftime("%m") + \
                   datetime.now().strftime("%d")

    def get_transfer_files(self):
        """obtain transfer files path."""
        dirs = get_transfer_dirs()
        for dir in dirs:
            filenames = fileope.get_file_names(dir_path=dir)
            for filename in filenames:
                target_file = "{0}/{1}".format(dir, filename)
                target_files.append(target_file)
        return target_files

    def get_transfer_dirs(self, path: str):
        """obtain transfer directories path."""
        target_dirs = list()
        # obtain Database directory path.
        db_dirnames = fileope.get_dir_names(dir_path=path)
        # generates full path of directory.
        for db_dirname in db_dirnames:
            target_dir = "{0}/{1}".format(path, db_dirname)
            target_dirs.append(target_dir)
        return target_dirs

    def transfer_files(self, targets: list, remote_path: str):
        """transfer local data to remoto host.

        Args:
            param1 targets: target files/dir
                type: list
            param2 remote_path: remote host path.
                type: str
        """
        print("Start transfering process.")
        try:
            with SSHConn(hostname=self.ssh_host,
                         username=self.ssh_user,
                         authkey=self.private_key) as dtrans:
                for target in targets:
                    try:
                        # if the target data is directory, its comporess to zip archive.
                        if fileope.dir_exists(target):
                            fileope.zip_data(file_path=target)
                            target = "{}.zip".format(target)

                        # execute scp.
                        self._logger.debug("try to scp.")
                        dtrans.scp_put(local_path=target, remote_path=remote_path)
                    except:
                        print("Error: failed to transfer files/dir to remote host.")
                        print("failed to transfer the target file. writing error log in {}".format(self.errlog_root))
                        with open(r"{0}/{1}_error.log".format(self.errlog_root, self.ymd), 'a') as f:
                            f.write("{} was not transfer to remote host.".format(target))
                        continue
                    else:
                        line = "succeeded to transfer data from local: {0} to remote: {1}".format(target, remote_path)
                        print(line)
                        with open(r"{0}/{1}_backup.log".format(self.log_root, self.ymd), 'a') as f:
                            f.write(line)
        except BadHostKeyException as badhoste:
            line = "SSH connection failed." \
                   "The host key given by the SSH server did not match what we were expecting"
            with open(r"{0}/{1}_error.log".format(self.errlog_root, self.ymd), 'a') as f:
                f.write(line)
            raise badhoste
        except AuthenticationException as authe:
            line = "SSH authentication failed. retry with diffrent credentials."
            with open(r"{0}/{1}_error.log".format(self.errlog_root, self.ymd), 'a') as f:
                f.write(line)
            raise authe
        except SSHException as sshe:
            line = "SSH connection failed. reason occured exception is unknwon."
            with open(r"{0}/{1}_error.log".format(self.errlog_root, self.ymd), 'a') as f:
                f.write(line)
            raise sshe
        except error as sockete:
            line = "SSH connection was timeout."
            with open(r"{0}/{1}_error.log".format(self.errlog_root, self.ymd), 'a') as f:
                f.write(line)
            raise sockete
