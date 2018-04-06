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
# -*- coding: utf-8 -*-
from osfile import fileope
from mylogger.logger import Logger
from mylogger import factory
from connection.sshconn import SSHConn
from paramiko import BadHostKeyException, AuthenticationException, SSHException
from socket import error
from os.path import split, join

class DataTransfer(object):
    """Transfer the data using scp."""

    def __init__(self,
                 hostname: str,
                 username: str,
                 keyfile_path=None,
                 password=None,
                 logger=None,
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
            param7 logger: logger instance.
            param8 loglevel: log level.

        """
        from datetime import datetime
        if keyfile_path is None and password is None:
            raise TypeError("keyfile_path or password arg is necessary.")
        # ロガーのセットアップ.
        # ロガーが渡されなかった場合の処理を書いていないので
        # 書く必要がある　改修予定
        self._logger = logger
        self.loglevel= loglevel
        if logger is None:
            slog_fac = factory.StdoutLoggerFactory()
            self._logger = slog_fac.create()
        if self._logger is not None and loglevel in {10, 20, 30, 40, 50}:
            self._logger.set_loglevel(loglevel)

        # hostname to connect ssh
        self.ssh_host = hostname
        # username
        self.ssh_user = username
        # private key file
        self.private_key = keyfile_path
        # password
        self.password = password
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

    def transfer_files(self,
                       targets: list,
                       remote_path: str,
                       archive_name=None,
                       delete=False):
        """transfer local data to remoto host.

        Args:
            param1 targets: target files/dir
                type: list
            param2 remote_path: remote host path.
                type: str
            param3 delete: delete an archive after transfering. defult is False.
        """
        self._logger.info("Start transfering process.")
        try:
            with SSHConn(hostname=self.ssh_host,
                         username=self.ssh_user,
                         authkey=self.private_key,
                         logger=self._logger,
                         loglevel=self.loglevel) as dtrans:
                for target in targets:
                    head, tail = split(target)
                    if archive_name is None:
                        archive_name = tail
                    try:
                        # if the target data is directory, its comporess to zip archive.
                        if fileope.dir_exists(target):
                            fileope.zip_data(file_path=target, archive_name=archive_name)
                            self._logger.info("Archiving... {}".format(target))
                            target = join(head, archive_name)
                            target = "{}.zip".format(target)

                        # execute scp.
                        dtrans.scp_put(local_path=target, remote_path=remote_path)
                    except:
                        line = "{} was not transfer to remote host.".format(target)
                        self._logger.error(line)
                        continue
                    else:
                        line = "the data transferd from local: {0} to remote: {1}".format(target, remote_path)
                        self._logger.info(line)
                        # if delete param is True, delete the archived file of localhost.
                        if delete:
                            self._delete_archive(target)
                            self._logger.info("delete {}".format(target))

        except BadHostKeyException as badhoste:
            line = "SSH connection failed." \
                   "The host key given by the SSH server did not match what we were expecting"
            self._logger.error(line)
            raise badhoste
        except AuthenticationException as authe:
            line = "SSH authentication failed. retry with diffrent credentials."
            self._logger.error(line)
            raise authe
        except SSHException as sshe:
            line = "SSH connection failed. reason occured exception is unknwon."
            self._logger.error(line)
            raise sshe
        except error as sockete:
            line = "SSH connection was timeout."
            self._logger.error(line)
            raise sockete

    def _delete_archive(self, path: str):
        fileope.rm_filedir(path)

