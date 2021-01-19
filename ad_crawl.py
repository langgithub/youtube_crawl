#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
# author：yuanlang 
# creat_time: 2021/1/18 上午10:39
# file: ad_crawl.py

# -*- coding: utf-8 -*-
import io
import base64
import pickle
import requests
import urllib3
import functools
import random
import time
from openpyxl import load_workbook, Workbook
from selenium import webdriver
from scrapy import Selector
from abc import ABCMeta, abstractmethod
from PIL import Image, ImageChops
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from you_get.extractors import (
    imgur,
    magisto,
    youtube,
    missevan,
    acfun,
    bilibili,
    soundcloud,
    tiktok
)


from obssdk.obs import ObsOperator, MultipartUploadFileRequest
from obssdk.obs.util import *
from io import BytesIO, BufferedReader

urllib3.disable_warnings()
# 深圳地址
obs_host = "obs-cn-shenzhen.yun.pingan.com"
obs_access_key = 'QTU2QjkyRjFFRDhGNDJDMzlBRkFDQUUyMTU5Q0Y5REM'
obs_secret_key = 'QzFBODBEQTFDNEVENEU1NDlDQkEwNjMwQzk2MzU2MkM'


class ErrorLog(object):

    def __init__(self, bg, full, diff, offset):
        self.bg = bg
        self.full = full
        self.diff = diff
        self.offset = offset


class BaseSpiderCrack(metaclass=ABCMeta):
    """验证码破解基础类"""

    def __init__(self, driver):
        self.driver = driver
        self.driver.maximize_window()
        self.error: ErrorLog = None

    @abstractmethod
    def crack(self):
        pass


class AdCrawl(BaseSpiderCrack):
    """youtube 广告爬虫"""

    def __init__(self, driver):
        super().__init__(driver)

    def check_response(self):
        """检查是否成功"""
        pickle.dump(self.driver.get_cookies(), open("cookies.pkl","wb"))

    def crack(self):
        """执行破解程序"""
        # self.write_to_excel("youtube.xlsx", [[1,2],[3,2]])

        self.driver.get("https://www.youtube.com/results?search_query=%E5%88%9B%E6%84%8F%E5%B9%BF%E5%91%8A")

        while 1:
            time.sleep(5)
            js = 'document.documentElement.scrollTop=1000000000000;return document.documentElement.scrollTop;'
            scrollTop = self.driver.execute_script(js)
            print(scrollTop)
            if "无更多结果" in self.driver.page_source:
                break

        print("over")
        html = Selector(text=self.driver.page_source)
        videos = html.css("div#container a#video-title")
        result_videos = []
        for video in videos:
            title = video.css("a::attr(title)").extract()[0]
            href = "https://www.youtube.com"+video.css("a::attr(href)").extract()[0]
            label = video.css("a::attr(aria-label)").extract()[0]
            result_videos.append([title, href, label])

        self.write_to_excel("youtube.xlsx", result_videos)

    def write_to_excel(self, path, items):
        wb = load_workbook(path)
        # wb = Workbook()
        sheets = wb.worksheets  # 获取当前所有的sheet

        sheet1 = sheets[0]
        print(sheet1)

        # 通过Cell对象读取
        # rows = sheet1.max_row
        # column = sheet1.max_column
        # print(rows, column)

        i = 0
        for rows in items:
            i = i + 1
            j = 0
            for value in rows:
                j = j + 1
                sheet1.cell(row=i, column=j, value=value)
        wb.save(path)

    def parse_3(self,response):
        data = response.meta.get("data")
        id = response.request.meta.get("id")
        url =f"http://obs-cn-shenzhen.yun.pingan.com/spider/{id}.pdf"
        data["url"]=url
        data["id"]=id
        # print(data)
        yield data
        self.put_stream(id,response.body)

    def put_stream(self, id, content):
        file_like = BufferedReader(BytesIO(content))
        self.logger.debug(type(file_like))
        obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
        ret = obs.put_object(bucket_name, id+".pdf", file_like)
        self.logger.debug(ret)


def video_list():
    """视频列表下载"""
    from selenium.webdriver.chrome.options import Options
    options = Options()
    # 我本地的 Google Chrome
    options.binary_location = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
    # 我本地的 chromedriver
    driver = webdriver.Chrome(chrome_options=options, executable_path='/Users/yuanlang/work/javascript/chromedriver')
    # 这个js没用到。过滑块可以使用
    with open('stealth.min.js') as f:
        js = f.read()

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": js
    })

    cracker = AdCrawl(driver)
    cracker.crack()


def get_href(path):
    wb = load_workbook(path)
    sheets = wb.worksheets  # 获取当前所有的sheet
    sheet1 = sheets[0]
    # 通过Cell对象读取
    rows = sheet1.max_row
    column = sheet1.max_column
    print(rows, column)
    # 启动线程池下载
    return [sheet1.cell(row=i, column=2).value for i in range(1, rows + 1)]


def video_download():

    def download(url):
        print("download url>>>>{0}".format(url))
        youtube.download(url, info_only=True)

    href1 = get_href("youtube_创意广告.xlsx")
    print(href1)
    href2 = get_href("youtube_泰国广告.xlsx")
    print(href2)
    hrefs = href1 + href2

    with ThreadPoolExecutor(max_workers=5) as t:
        for href in hrefs:
            t.submit(download, href)


def video_upload():
    pass


if __name__ == "__main__":
    youtube.download(
        'https://www.youtube.com/watch?v=o2kLaT5sRzM', info_only=True
    )
    # video_download()