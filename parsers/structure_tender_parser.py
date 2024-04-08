from zipfile import ZipFile
from xml.etree import ElementTree
from util.date_range_parser import DataRangeParser
from util.date_range import DateRange
from datetime import date
from util.dict_util import DictUtil

import os
import re
import json

class PluralValidator():
    """Класс занимается проверкой слов на множественное число"""

    def is_validate(self, word: str) -> bool:
        """Функция проверяет английское слово на множественное число

        Parameters
        ----------
        word:
            Слово, которое будет проверяться
        ----------
        Return
        ----------
        Булевое значение
        """

        # Правила для определения множественного числа
        plural_rules = [
            (r's$', ''),
            (r'^(ax|test)es$', r'\1'),
            (r'(octop|vir)us$', r'\1i'),
            (r'(alias|status)$', r'\1es'),
            (r'(bu)s$', r'\1ses'),
            (r'(buffal|tomat)o$', r'\1oes'),
            (r'([ti])um$', r'\1a'),
            (r'([ti])a$', r'\1a'),
            (r'sis$', 'ses'),
            (r'([^aeiouy]|qu)ies$', r'\1y'),
            (r'(matr|vert|ind)ix|ex$', r'\1ices'),
            (r'([m|l])ouse$', r'\1ice'),
            (r'^(ox)$', r'\1en'),
            (r'(quiz)$', r'\1zes')
        ]

        for rule in plural_rules:
            pattern, replacement = rule
            if re.search(pattern, word):
                return re.sub(pattern, replacement, word) != word

        return False

class StructureTenderParser():
    """Класс отвечает за парсинг структуры тендра по промежуткам дат и типам закупок"""

    def __init__(self) -> None:
        self.plural_validator = PluralValidator()
        self.ignore_tag = ["signature", "cryptoSigns", "printForm"]
        self.structures = {}

    def parsing_from_xml(self, element) -> list:
        """Функция дастает из zip файлы xml и парсит эти файлы в структуру по промежуткам дат и типам закупок

        Parameters
        ----------
        element:
            Корневой элемент xml дерева (ElementTree)
        ----------
        Return
        ----------
        Словарь с структурой тендера
        """

        # Инициализация пустого словаря для текущего элемента
        isArray = False
        if element.tag.split('}')[1][-1].lower() == 's':
            current_dict = []
            isArray = True
        else:
            current_dict = {}

        # # Перебор атрибутов текущего элемента
        # for key, value in element.attrib.items():
        #     print(value)
        #     current_dict[key] = value

        # Рекурсивный перебор дочерних элементов
        for child in element:
            if child.tag.split('}')[1] not in self.ignore_tag:
                # Рекурсивный вызов функции для каждого дочернего элемента
                child_dict = self.parsing_from_xml(child)

                if isArray:
                    current_dict.append({child.tag.split('}')[1]: child_dict})
                else:
                    # Объединение словарей текущего элемента и его дочерних элементов
                    current_dict.setdefault(child.tag.split('}')[1], child_dict)

        # Если у текущего элемента нет дочерних элементов, сохраняем его текст
        if not current_dict:
            current_dict = str(element.text)

        return current_dict

    def parse(self, directory: str) -> None:
        """Функция дастает из zip файлы xml и парсит эти файлы в структуру по промежуткам дат и типам закупок

        Parameters
        ----------
        directory:
            Директория где лежат zip файлы, которые нужно спарсить
        ----------
        Return
        ----------
        Нет
        """
        data_range_parser = DataRangeParser()
        
        path = os.fsencode(directory)
        file_index = 0

        for file in os.listdir(path):
            filename = os.fsdecode(file)
            file_count = len(os.listdir(path))

            if filename.endswith(".zip"):
                file_index += 1

                date_range = data_range_parser.parse(filename)

                # Смотрим промежуток с начала 2014 года по конец 2016 года
                self.get_structure(date_range, directory, filename, date(2014, 1, 1), date(2017, 1, 1))
                # Смотрим промежуток с начала 2016 года по конец 2020 года
                self.get_structure(date_range, directory, filename, date(2017, 1, 1), date(2021, 1, 1))
                # Смотрим промежуток с начала 2020 года по конец Н.В года
                self.get_structure(date_range, directory, filename, date(2021, 1, 1), date(2023, 11, 15))

                print(f'{file_index / file_count * 100} %')
                continue
            else:
                continue

        with open("structures.txt", "w") as file:
            json.dump(self.structures, file, indent=4)
            file.close()

    def get_structure(self, date_range: DateRange, directory_str: str, filename: str, start: date, end: date) -> None:
        """Функция для получения тендеров по промежуткам дат"""

        if date_range.start >= start and date_range.end <= end:
            with ZipFile(directory_str + filename, "r") as myzip:
                fileZipIndex = 0

                for nameFileXml in myzip.namelist():
                    fileZipIndex += 1

                    if nameFileXml.endswith(".xml"):
                        str_data_xml = myzip.read(nameFileXml).decode('utf-8')
                        root = ElementTree.fromstring(str_data_xml)

                        index = 0
                        for element in root.iter():
                            if (index == 1):
                                type_tender = element.tag.split('}')[1]

                                type_tender_date_range = f"{type_tender}_{start.strftime('%d.%m.%Y')}_{end.strftime('%d.%m.%Y')}"

                                if (type_tender_date_range not in self.structures):
                                    json_structure = {
                                        "name": type_tender,
                                        "toDate": start.strftime("%d.%m.%Y"),
                                        "fromDate": end.strftime("%d.%m.%Y"),
                                        "structure": self.parsing_from_xml(element)
                                    }
                                    self.structures[type_tender_date_range] = json_structure
                                else:
                                    old_structure = self.structures[type_tender_date_range]["structure"]
                                    new_structure = self.parsing_from_xml(element)
                                    self.structures[type_tender_date_range]["structure"] = DictUtil.merging_dictionaries(old_structure, new_structure)

                            index += 1