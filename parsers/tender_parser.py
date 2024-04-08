from zipfile import ZipFile
from xml.etree import ElementTree
from datetime import date
from util.date_range_parser import DataRangeParser
from util.work_timer import WorkTimer
from util.dict_util import DictUtil
from queue import Queue
from threading import Thread
import xmltodict

import numpy as np
import os
import csv
import time
import json
import sys

class TenderParser():
    def __init__(self) -> None:
        self.work_timer = WorkTimer()
        self.data_range_parser = DataRangeParser()
        self.ignore_tag = ["signature", "printForm", "printFormInfo", "cryptoSigns"]
        self.objects = []
        self.okpd2_class_target = ['21']#'02', '03', '63', '14',]
        self.types = ['epNotificationEF2020', 'fcsNotificationEP', 'fcsNotificationEF', 
                      'fcsNotificationZK', 'fcsNotificationOK', 'fcsNotificationISM', 
                      'fcsNotificationOKOU', 'fcsNotificationZP', 'fcsNotificationZakK', 
                      'fcsNotificationZakA', 'fcsNotificationPO', 'fcsNotificationOKD',
                      'epNotificationEOKPF', 'epNotificationEOK', 'epNotificationEOK2020', 
                      'epNotificationEZK', 'epNotificationEZK2020', 'epNotificationEZP', 
                      'epNotificationEOKOU', 'epNotificationEOKD']
        self.ns = {
            "ns9": '{http://zakupki.gov.ru/oos/EPtypes/1}',
            "ns4": '{http://zakupki.gov.ru/oos/common/1}',
            "ns2": '{http://zakupki.gov.ru/oos/base/1}',
        }
    
    # получить номер тендера
    def _get_data(self, rootElement, ns, element):
        for purchaseNumber in rootElement.iter(ns + element):
            return purchaseNumber.text.strip()
        return ""
    
    def parse_json(self, directory: str) -> None:

        path = os.fsencode(directory)
        file_count = len(os.listdir(path))
        file_index = 0
        name = "data_1.json"

        with open(name, "w", encoding="utf-8") as file:
            file.write('[')
            file.close()

        for file in os.listdir(path):
            filename = os.fsdecode(file)

            if filename.endswith(".zip"):
                file_index += 1

                if file_index >= 1 and file_index <= 1587:

                    self.work_timer.start()
                    
                    try:
                        with ZipFile(directory + filename, "r") as myzip:
                            file_zip_count = len(myzip.namelist())
                            file_zip_index = 0

                            for nameFileXml in myzip.namelist():
                                file_zip_index += 1

                                if nameFileXml.endswith(".xml"):
                                    str_data_xml = myzip.read(nameFileXml).decode('utf-8')
                                    root = ElementTree.fromstring(str_data_xml)

                                    if len(root) == 0: continue
                                    
                                    try:
                                        marked_dict_array = np.array([])
                                        type_purchase = root[0].tag.split('}')[1]

                                        # Номер тендера
                                        number = self._get_data(root, self.ns["ns9"], 'purchaseNumber')
                                        
                                        if type_purchase in self.types:
                                            
                                            # Дата публикации
                                            docPublishDate = self._get_data(root, self.ns["ns9"], 'plannedPublishDate')

                                            # Объект закупки
                                            purchaseObjectInfo = self._get_data(root, self.ns["ns9"], 'purchaseObjectInfo')

                                            # Валюта
                                            currency = ""
                                            for cur in root.iter(self.ns["ns9"] + 'currency'):
                                                currency = cur.find(self.ns["ns2"] + 'code').text.strip()
                                                break

                                            # Дата старта процедуры
                                            procedureStartDate = ""
                                            # Дата окончания процедуры
                                            procedureEndDate = ""
                                            for stages in root.iter(self.ns["ns9"] + 'stagesInfo'):
                                                # Сначала берем дату старта самой первой стадии и прерываем поиск
                                                for firstStage in stages.iter(self.ns["ns9"] + 'stageInfo'):
                                                    procedureStartDate = self._get_data(firstStage, self.ns["ns4"], 'startDate')
                                                    break
                                                # Далее берем последнюю стадию и берем ее дату окончания
                                                for endDate in stages.iter(self.ns["ns4"] + 'endDate'):
                                                    procedureEndDate = endDate.text.strip()
                                                break;

                                            # Сбор массива данных по объектам закупок
                                            okpd2s = np.array([])
                                            isGot = False
                                            for purchaseObject in root.iter(self.ns["ns4"] + 'drugPurchaseObjectInfo'):
                                                isGot = True
                                                okpd2 = self._get_data(purchaseObject, self.ns["ns2"], 'OKPDCode')
                                                ktru = self._get_data(purchaseObject, self.ns["ns4"], 'KTRU')
                                                cost = self._get_data(purchaseObject, self.ns["ns4"], 'positionPrice')
                                                if cost != "":
                                                    cost = float(cost)
                                                okpd2s = np.append(okpd2s, {'OKPD2': okpd2, 'KTRU': ktru, 'cost': cost})
                                            if isGot == False:
                                                for purchaseObject in root.iter(self.ns["ns4"] + 'purchaseObject'):
                                                    okpd2 = self._get_data(purchaseObject, self.ns["ns2"], 'OKPDCode')
                                                    ktru = self._get_data(purchaseObject, self.ns["ns4"], 'KTRU')
                                                    cost = self._get_data(purchaseObject, self.ns["ns4"], 'sum')
                                                    if cost != "":
                                                        cost = float(cost)
                                                    okpd2s = np.append(okpd2s, {'OKPD2': okpd2, 'KTRU': ktru, 'cost': cost})

                                            
                                            isTarget = False

                                            for x in okpd2s:
                                                if x['OKPD2'].split('.')[0] in self.okpd2_class_target:
                                                    isTarget = True
                                                    marked_dict_array = np.append( marked_dict_array, {
                                                        'number': number,
                                                        'typePurchase': type_purchase,
                                                        'status': "Определение поставщика завершено",
                                                        'docPublishDate': docPublishDate,
                                                        'purchaseObjectInfo': purchaseObjectInfo,
                                                        'maxPrice': x['cost'],
                                                        'currency': currency,
                                                        'okpd2CodeClass': x['OKPD2'],
                                                        'ktruCode': x['KTRU'],
                                                        'procedureStartDate': procedureStartDate,
                                                        'procedureEndDate': procedureEndDate,
                                                    })
                                                    
                                            if isTarget:
                                                with open(name, "a", encoding="utf-8") as file:
                                                    for obj in marked_dict_array:
                                                        json.dump(obj, file)
                                                        file.write(',')
                                                    file.close()

                                        elif type_purchase in ['epNotificationCancel', 'epNotificationCancelFailure', 'fcsNotificationCancel', 'fcsNotificationCancelFailure', 'fcsNotificationLotCancel']:

                                            # Сбор массива данных по объектам закупок
                                            isTarget = False
                                            # okpd2s = np.array([])
                                            # for purchaseObject in root.iter(self.ns["ns4"] + 'drugPurchaseObjectInfo'):
                                            #     okpd2 = self._get_data(purchaseObject, self.ns["ns2"], 'OKPDCode')
                                            #     okpd2s = np.append(okpd2s, {'OKPD2': okpd2})

                                            # for x in okpd2s:
                                            #     if x['OKPD2'].split('.')[0] in self.okpd2_class_target:
                                            #         isTarget = True

                                            # Дата публикации
                                            docPublishDate = self._get_data(root, self.ns["ns9"], 'docPublishDTInEIS')
                                            #root[0].find('{http://zakupki.gov.ru/oos/types/1}docPublishDate').text.strip()

                                            # Причина закрытия
                                            # cancelReason = root[0].find('{http://zakupki.gov.ru/oos/types/1}cancelReason')
                                            # responsibleDecision = cancelReason.find('{http://zakupki.gov.ru/oos/types/1}responsibleDecision')
                                            decisionDate = self._get_data(root, self.ns["ns9"], 'decisionDate')
                                            # responsibleDecision.find('{http://zakupki.gov.ru/oos/types/1}decisionDate').text.strip()

                                            if isTarget:
                                                marked_dict = {
                                                        'number': number,
                                                        'typePurchase': type_purchase,
                                                        'status': "Определение поставщика отменено",
                                                        'docPublishDate': docPublishDate,
                                                        'decisionDate': decisionDate
                                                }

                                                with open(name, "a", encoding="utf-8") as file:
                                                    json.dump(marked_dict, file)
                                                    file.write(',')
                                                    file.close()
                                    except Exception as err:
                                        print(f"Unexpected {err=}, {type(err)=}")
                                        continue

                                print('\033[F\033[K', end='')
                                print(f'Просмотренно zip: {file_index}/{file_count}\t{round(file_index/file_count*100, 2)}%\t|\t{file_zip_index}/{file_zip_count}\t{round(file_zip_index/file_zip_count*100, 2)}%\t|\tВремя скачивания {self.work_timer.days} дней {self.work_timer.hours} часов {self.work_timer.minutes} минут {self.work_timer.seconds} секунд')
                    except Exception as err:
                        print(f"Unexpected {err=}, {type(err)=}")
                        continue
                    self.work_timer.calculate_time(file_index, file_count)

        with open(name, "rb+") as file:
            file.seek(-1, 2)
            file.truncate()
            file.close()

        with open(name, "r+", encoding="utf-8") as file:
            file.seek(0, 2)
            file.write(']')
            file.close()


    def demo_parse_json(self, directory: str) -> None:
        path = os.fsencode(directory)
        file_count = len(os.listdir(path))
        file_index = 0
        name = "data_1.json"

        with open(name, "w", encoding="utf-8") as file:
            file.write('[')
            file.close()

        for file in os.listdir(path):
            filename = os.fsdecode(file)

            if filename.endswith(".zip"):
                file_index += 1

                if file_index >= 1 and file_index <= 999999999:

                    self.work_timer.start()

                    with ZipFile(directory + filename, "r") as myzip:
                        file_zip_count = len(myzip.namelist())
                        file_zip_index = 0

                        for nameFileXml in myzip.namelist():
                            file_zip_index += 1

                            if nameFileXml.endswith(".xml"):
                                
                                doc = xmltodict.parse(myzip.read(nameFileXml).decode('utf-8'))

                                with open(name, "a", encoding="utf-8") as file:
                                    json.dump(doc, file)
                                    file.write(',')
                                    file.close()