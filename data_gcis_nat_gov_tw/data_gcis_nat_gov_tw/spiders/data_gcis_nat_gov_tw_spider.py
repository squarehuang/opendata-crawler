# -*- coding: utf-8 -*-
import os
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

import traceback
import csv
import urlparse
import shutil
import zipfile
import re
import logging
import scrapy

from scrapy.conf import settings
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from bs4 import BeautifulSoup
from data_gcis_nat_gov_tw.items import MappingItem


logger = logging.getLogger('DataGcisNatGovTwSpiderLogger')


class DataGovTwSpider(scrapy.Spider):
    name = "data_gcis_nat_gov_tw"
    url_prefix = 'http://data.gcis.nat.gov.tw/'
    allow_domains = ["data.gcis.nat.gov.tw"]
    mapping_columns = ['filename', 'id', 'name', 'description', 'changed_date',
                       'columns', 'char_encoding', 'file_format']
    target_col_text = {
        u"商業登記(依營業項目分)": "business-registration", u"公司登記(依營業項目分)": "company-registration"
    }
    mapping_dict = {}

    def get_start_urls(self):
        start_urls = [
            "http://data.gcis.nat.gov.tw/od/datacategory"
        ]
        return start_urls

    def get_download_store_path(self):
        sub_path = settings.get('DOWNLOAD_STORE_PATH')
        project_root = os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))))
        print project_root
        download_store_path = os.path.join(os.path.sep, project_root, sub_path)
        try:
            if os.path.isdir(download_store_path) == False:
                os.makedirs(download_store_path)
                logger.info('mkdir {} folder '.format(download_store_path))
        except Exception as e:
            logger.warning(
                'mkdir {} folder failed '.format(download_store_path))
            logger.warning(str(e))
            raise e

        return download_store_path

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.start_urls = self.get_start_urls()
        self.download_store_path = self.get_download_store_path()
        self.proxies = settings.get('PROXIES_FOR_REQUESTS')
        self.mapping_filename = settings.get('MAPPING_FILENAME') + '.csv'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # write mapping file
    def spider_closed(self, spider):
        mapping_file_path = os.path.join(
            os.path.sep, self.download_store_path, self.mapping_filename)
        with open(mapping_file_path, 'w') as mapping_file:
            wr = csv.writer(mapping_file, quoting=csv.QUOTE_ALL)
            wr.writerow(self.mapping_columns)
            for key, line in self.mapping_dict.iteritems():
                wr.writerow([col.encode('utf-8') for col in line])

    def parse(self, response):
        soup = BeautifulSoup(response.body)
        target_block = soup.find_all('div', class_='panel panel-default')
        for block in target_block:
            block_title = block.find(
                'div', class_='panel-heading').find('div').find(text=True, recursive=False).strip()
            # print block_title
            if block_title in self.target_col_text.keys():
                items = block.find_all('li')
                for item in items:
                    if item.a != None:
                        item_name = item.a.text
                        suffix_url = item.a.get('href')
                        item_url = self.url_prefix + suffix_url
                        item_id = suffix_url.split('?oid=')[-1]
                        item_date = item.find('span', class_='date').text
                        yield scrapy.Request(item_url, meta={'query_condition': self.target_col_text.get(block_title, ''), 'item_name': item_name, 'item_id': item_id, 'item_date': item_date},
                                             callback=self.parse_item)
                    else:
                        logger.info(item.text)

            else:
                continue

    def parse_item(self, response):
        logger.info('parsing item: {}'.format(
            response.meta.get('item_name').encode('utf-8')))
        try:
            item = MappingItem()
            response.meta.get('channel')
            filename = '{}-{}'.format(response.meta.get('query_condition'),
                                      response.meta.get('item_id'))
            soup = BeautifulSoup(response.body)
            try:
                description = soup.find('th', text=re.compile(
                    u'資料集描述')).find_next_sibling('td').text.strip()
                item['columns'] = description
            except Exception as e:
                logger.warning('get description error')

            try:
                columns = soup.find('th', text=re.compile(
                    u'主要欄位說明')).find_next_sibling('td').text.strip()
                item['columns'] = columns
            except Exception as e:
                logger.warning('get columns error')

            try:
                changed_date = response.meta.get('item_date')
                print changed_date
                item['changed_date'] = changed_date
            except Exception as e:
                logger.warning('get changed_date error')

            try:
                char_encoding = soup.find('th', text=re.compile(
                    u'編碼格式')).find_next_sibling('td').text.strip()
                item['char_encoding'] = char_encoding
            except Exception as e:
                logger.warning('get char_encoding error')

            item['filename'] = filename
            item['id'] = response.meta.get('item_id')
            item['name'] = response.meta.get('item_name')

            try:
                download_btn = soup.find('th', text=re.compile(
                    u'原始檔案下載')).find_next_sibling('td')
                # print download_btn
                # get download url
                download_suffix_url = download_btn.a.get(
                    'onclick').split('\'')[1]
                download_url = self.url_prefix + download_suffix_url
                print download_url
            except Exception as e:
                logger.warning('get download_btn error {}'.format(str(e)))

            # get file format
            extension_filename = download_btn.a.text.lower()
            if '.zip' in download_url.lower() or u'壓縮檔' in extension_filename:
                extension_filename = 'zip'
            item['file_format'] = extension_filename
            # append to mapping list
            if filename not in self.mapping_dict:
                submapping_list = []
                for column in self.mapping_columns:
                    if column in item:
                        submapping_list.append(
                            item.get(column, ''))
                self.mapping_dict[filename] = submapping_list

            is_zip = False
            # 確認下載回來的副檔名 filename ，有些 url 沒有包含檔案格式的文字 ，即代表下載完是 zip 檔，需再進行解壓縮
            if extension_filename in download_url.lower():
                temp_extension_filename = extension_filename
            else:
                temp_extension_filename = 'zip'
                is_zip = True

            logger.info('name :{}-> download_url: {}'.format(
                filename.encode('utf-8'), download_url.encode('utf-8')))
            local_temp_filename = os.path.join(
                os.path.sep, self.download_store_path, filename) + '.' + temp_extension_filename

            local_filename = os.path.join(
                os.path.sep, self.download_store_path, filename) + '.' + extension_filename

            # download file
            yield scrapy.Request(download_url, callback=self.save_file, meta={
                'local_temp_filename': local_temp_filename, 'local_filename': local_filename, 'is_zip': is_zip})
        except Exception as e:
            logger.warning(e)
            logger.warning('parse_item failed : {}'.format(str(e)))
            traceback.print_exc()

    def save_file(self, response):
        logger.info('response status code : {}'.format(str(response.status)))
        local_temp_filename = response.meta.get('local_temp_filename')
        local_filename = response.meta.get('local_filename')
        is_zip = response.meta.get('is_zip')

        logger.info('save_file : {}'.format(local_filename.encode('utf-8')))
        with open(local_temp_filename.encode('utf-8'), 'wb') as f:
            f.write(response.body)
        if is_zip:
            try:
                # unzip file
                fh = open(local_temp_filename.encode('utf-8'), 'rb')
                z = zipfile.ZipFile(fh)
                for name in z.namelist():
                    z.extract(name, self.download_store_path)
                    shutil.move(os.path.join(
                        self.download_store_path, name), local_filename)
                fh.close()
                # delete zip file
                os.remove(local_temp_filename.encode('utf-8'))
            except Exception as e:
                logger.warning('upzip file error : {}'.format(str(e)))
                shutil.move(os.path.join(
                    local_temp_filename), local_filename)
