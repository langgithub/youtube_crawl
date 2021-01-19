#!/usr/local/env python
# -*- coding:utf-8 -*-

import unittest
import hashlib
import base64
from obs.constants import *


class TestUtil(unittest.TestCase):

    def test_100(self):
        with open("tests\\temp\\test_crypto.txt", "rb") as file_object:
            data = file_object.read()
            md5 = hashlib.md5(data).digest()
            b64_md5 = base64.b64encode(md5)
            b64_md5 = b64_md5.decode(DEFAULT_ENCODING)
            self.assertEqual('fJgLk23Yno+s+1GxlaHCFA==', b64_md5)

    def test_110_big_file(self):
        m = hashlib.md5()
        m.update(b"Nobody inspects")
        m.update(b" the spammish repetition")
        data = m.hexdigest()
        print('data:', data)

if __name__ == "__main__":
    unittest.main()
