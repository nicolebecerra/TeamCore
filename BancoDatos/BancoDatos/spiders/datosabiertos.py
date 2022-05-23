# -*- coding: utf-8 -*-
"""
Created on Fri May 20 22:07:56 2022

@author: NICOLE BECERRA
"""

import scrapy
import requests
from zipfile import ZipFile
import pandas as pd
import os 


class DatosAbiertosSpider(scrapy.Spider):
    name = "datosabiertos"

    def __init__(self, *args, **kwargs):
        super(DatosAbiertosSpider, self).__init__(*args, **kwargs)
        self.tipo = kwargs.get('tipo')
        self.categoria = kwargs.get('categoria')
        self.formato = kwargs.get('formato')
        self.nombre = kwargs.get('nombre')

    def start_requests(self):

        urls = [
            'https://www.datosabiertos.gob.pe/'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Filtrar por tipo
        path = '//div[@class="item-list"]/ul/li/\
            a[contains(text(),'+self.tipo+')]/@href'
        url_tipo = response.urljoin(response.xpath(path).extract_first())
        yield scrapy.Request(url=url_tipo, callback=self.parse_tipo)

    def parse_tipo(self, response):
        # Filtrar por categoria
        path = '//div[@class="panel-panel-inner"]/div[2]//ul/li/\
            a[contains(text(),'+self.categoria+')]/@href'
        url_categoria = response.urljoin(response.xpath(path).extract_first())
        yield scrapy.Request(url=url_categoria, callback=self.parse_categoria)

    def parse_categoria(self, response):
        # Filtrar por formato
        path = '//div[@class="panel-panel-inner"]/div[4]//ul/li/\
            a[contains(text(),'+self.formato+')]/@href'
        url_formato = response.urljoin(response.xpath(path).extract_first())
        yield scrapy.Request(url=url_formato, callback=self.parse_formato)

    def parse_formato(self, response):
        # Filtrar por nombre
        url_nombre = response.urljoin("?query="+self.nombre+"&sort_by=changed&sort_order=DESC")
        yield scrapy.Request(url=url_nombre, callback=self.parse_final)

    def parse_final(self, response):
        # Selecciona archivo
        path = '//div[@class="view-content"]/div/article/div[2]/h2/a/@href'
        url_archivo = response.urljoin(response.xpath(path).extract_first())
        yield scrapy.Request(url=url_archivo, callback=self.parse_inf)

    def parse_inf(self, response):
        # Descarga data .zip
        path = "//p[contains(text(),'Data')]/../span/a/@href"
        search_data = response.xpath(path).extract_first()
        response = requests.get(search_data, stream=True)
        with open('donaciones.zip', "wb") as f:
            for chunk in response.iter_content(chunk_size=512):
                if chunk:
                    f.write(chunk)

    def close(self):
        zip = ZipFile('donaciones.zip')
        zip.extractall()
        df = pd.read_csv('pcm_donaciones.csv', sep=',', encoding='LATIN-1')
        df[df['REGION'] == 'LIMA'].to_csv('lima.csv', index=False)
        os.remove('donaciones.zip')
        os.remove('donaciones.zip')
