#!/usr/local/env python
# -*- coding:utf-8 -*-

import unittest
import os

# 用例路径
case_path = os.path.join(os.getcwd(), "tests")


def all_case():
    discover = unittest.defaultTestLoader.discover(case_path, pattern="test*.py", top_level_dir=None)
    print(discover)
    return discover


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(all_case())

