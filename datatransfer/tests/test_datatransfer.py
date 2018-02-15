# -*- coding: utf-8 -*-
import unittest
from datatransfer.datatransfer import DataTransfer

HOST = '172.16.30.7'
USER = "Nxj_ToS_TS1"
PRIVATE_KEY = '/home/Nxj_ToS_TS1/.ssh/id_rsa'
TEST_FILE = '/tmp/test.test'
REMOTE_PATH = '/tmp'

class TestDataTransfer(unittest.TestCase):
    """DataTransfer Class test module."""

    def setUp(self):
        self.dt = DataTransfer(hostname=HOST,
                          username=USER,
                          keyfile_path=PRIVATE_KEY,
                          loglevel=10)
    def tearDown(self):
        del self.dt
    def test_transfer_files(self):
        with open(TEST_FILE, mode='r') as file:
            pass
        targets = [TEST_FILE]
        remote_path = REMOTE_PATH
        self.dt.transfer_files(targets=targets, remote_path=REMOTE_PATH)
        test_success = True
        print("finished transfer_files method test.")
        self.assertTrue(test_success)


if __name__ == '__main__':
    unittest.main()