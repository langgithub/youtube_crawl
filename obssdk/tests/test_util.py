#!/usr/local/env python
# -*- coding:utf-8 -*-

import unittest

from obs.util import *
from obs.sign import RGWSigner


class TestUtil(unittest.TestCase):

    def test_102_trim(self):
        e_tag = '"abc"'
        e_tag = trim_tag(e_tag)
        self.assertEqual('abc', e_tag)

    def test_103_base64_md5(self):
        data = "crypto data:11223344"
        ret = base64_md5(bytes(data, encoding=DEFAULT_ENCODING))
        self.assertEqual("fJgLk23Yno+s+1GxlaHCFA==", ret)

    def test_104_sign(self):
        headers = {
            HTTP_DATE: "Thu, 16 Aug 2018 09:54:12 GMT",
        }
        sign = RGWSigner.sign("wh4UEdlUdfBM-etLB9bU8ic7bF_zCFtXVuV9V7ntBZ1ADgYxbuY4A7PPAicyLVnpGQMiQoPPGOUFlT6vxJH46Q",
                              "/2121212/shen4.jpg", "DELETE", headers)
        self.assertEqual("ArNEmk3E0QnG0E0bOAD6TJV8YT8=", sign)

    # s3的一个示范校验
    def test_114_sign(self):
        string_to_sign = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/"
        sign = RGWSigner.compute_sign("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", string_to_sign)
        self.assertEqual("htDYFYduRNen8P9ZfE/s9SuKy0U=", sign)

if __name__ == "__main__":
    unittest.main()
