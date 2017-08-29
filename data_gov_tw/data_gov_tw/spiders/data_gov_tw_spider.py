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
from data_gov_tw.items import MappingItem


logger = logging.getLogger('DataGovTwSpiderLogger')


class DataGovTwSpider(scrapy.Spider):
    name = "data_gov_tw"
    url_prefix = 'https://data.gov.tw'
    allow_domains = ["data.gov.tw"]
    mapping_columns = ['filename', 'id', 'name', 'description', 'changed_date',
                       'columns', 'char_encoding', 'file_format']
    mapping_dict = {}

    def get_start_urls(self):
        start_urls = {
            "https://data.gov.tw/datasets/search?qs=%E5%85%AC%E5%8F%B8%E7%99%BB%E8%A8%98%20%E4%BE%9D%E7%87%9F%E6%A5%AD%E9%A0%85%E7%9B%AE": "company-registration",
            "https://data.gov.tw/datasets/search?qs=%E5%95%86%E6%A5%AD%E7%99%BB%E8%A8%98%20%E4%BE%9D%E7%87%9F%E6%A5%AD%E9%A0%85%E7%9B%AE": "business-registration"

        }
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
        for url in self.start_urls.keys():
            yield scrapy.Request(url, meta={'name': self.start_urls.get(url)}, callback=self.parse)

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
        name = response.meta.get('name')
        next_url = self.get_next_link(response)
        soup = BeautifulSoup(response.body)
        items = soup.find('div', id='data').find_all(
            'div', re.compile("node-"))
        for item in items:
            item_name = item.a.text
            suffix_url = item.a.get('href')
            # item_url = self.url_prefix + suffix_url
            item_url = urlparse.urljoin(self.url_prefix, suffix_url)
            item_id = suffix_url.split('/')[-1]

            yield scrapy.Request(item_url, meta={'query_condition': name, 'item_name': item_name, 'item_id': item_id},
                                 callback=self.parse_item)

        if next_url != None:
            yield scrapy.Request(url=next_url,
                                 callback=self.parse, meta={'name': name})

    def parse_item(self, response):
        logger.info('parsing item: {}'.format(
            response.meta.get('item_name').encode('utf-8')))
        try:
            item = MappingItem()
            response.meta.get('channel')
            filename = '{}-{}'.format(response.meta.get('query_condition'),
                                      response.meta.get('item_id'))
            soup = BeautifulSoup(response.body)

            # get description
            description = soup.find(
                'div', class_="field field-name-field-content field-type-text-long field-label-inline clearfix").find('div', class_='field-item even').text
            item['description'] = description

            # get columns
            columns = soup.find_all(
                'div', class_="field field-custom-field clearfix field-label-inline")[0].find('div', class_='field-item').text
            # print(columns)
            item['columns'] = columns

            # get changed_date
            changed_date = soup.find_all(
                'div', class_="field field-custom-field clearfix field-label-inline")[10].find('div', class_='field-item').text
            # print(changed_date)
            item['changed_date'] = changed_date

            # get char_encoding
            char_encoding = soup.find_all(
                'div', class_="field field-custom-field clearfix field-label-inline")[3].find('div', class_='field-item').text
            # print(char_encoding)
            item['char_encoding'] = char_encoding

            item['filename'] = filename
            item['id'] = response.meta.get('item_id')
            item['name'] = response.meta.get('item_name')

            download_btn = soup.find(
                'div', class_="field field-name-field-dataset-resource field-type-dgresource-resouce field-label-inline clearfix").find('div', class_='field-item even')
            # get download url
            download_url = download_btn.a.get('href')
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

    def get_next_link(self, response):
        try:
            soup = BeautifulSoup(response.body)
            next_btn = soup.find("ul", class_="pagination").find_all("li")[-2]
            print(next_btn)
            next_btn_cls = next_btn.get('class')[0]
            print(next_btn_cls)
            if next_btn_cls == 'disabled':
                final_url = None
            else:
                final_url = urlparse.urljoin(
                    self.url_prefix, next_btn.a.get('href'))
        except Exception as e:
            final_url = None
        print('final_url : {}'.format(final_url))
        return final_url
