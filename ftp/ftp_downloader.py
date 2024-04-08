from datetime import date
from ftplib import FTP
from util.date_range_parser import DataRangeParser
from util.work_timer import WorkTimer

import os
import re

class FtpDownloader():
    """Класс отвечает за скачивание файлов с ftp сервера"""

    def __init__(self, ftp: FTP) -> None:
        self.data_range_parser = DataRangeParser()
        self.work_timer = WorkTimer()
        self.ftp = ftp
    
    def download_zip(self, source: str, path: str, date_start: date, date_end: date) -> None:
        """Функция скачивает zip файлы с ftp сервера и сохраняет их на диске

        Parameters
        ----------
        source:
            Путь на ftp сервер
        path:
            Путь куда будут скачиваться данные
        date_start:
            Дата с которой начать скачивать Zip
        date_end:
            Дата до какой будут скачиваться Zip
        ----------
        Return
        ----------
        Нет
        """

        directory = self.ftp.nlst(source)

        file_count = 0
        file_skip_count = 0

        if len(directory) > 0:
            self._try_create_directory(path)

            for file in directory:
                file_count += 1
                self.work_timer.start()

                if re.fullmatch(r'^.+(\.xml)+(\.zip)$', file):
                    file_name = file.split('/')[-1]
                    date_range = self.data_range_parser.parse(file_name)

                    if date_range.start >= date_start and date_range.end <= date_end:
                        self.ftp.retrbinary('RETR ' + file, open(path + file_name, 'wb').write)
                    else:
                        file_skip_count += 1
                else:
                    file_skip_count += 1
                
                self.work_timer.calculate_time((file_count - file_skip_count), len(directory) - file_skip_count)

                print('\033[F\033[K\n', end='')
                print(f'Скачивание файлов {int(file_count / len(directory) * 100)}%\t|\tВремя скачивания {self.work_timer.days} дней {self.work_timer.hours} часов {self.work_timer.minutes} минут {self.work_timer.seconds} секунд')


    def _try_create_directory(self, path: str) -> None:
        """Функция пытается создать деректорию

        Parameters
        ----------
        path:
            Путь для создания директории
        ----------
        Return
        ----------
        Нет
        """

        try:
            os.makedirs(path)
        except OSError as exception:
            print("Error: The directory already exists or could not be created")