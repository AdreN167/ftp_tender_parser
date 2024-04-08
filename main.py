from ftp.ftp_downloader import FtpDownloader
from parsers.tender_parser import TenderParser
from datetime import date
from parsers.structure_tender_parser import StructureTenderParser

import ftplib
import schedule
import socket

source="/fcs_regions/Novosibirskaja_obl/notifications/"
directory_str="./regions/Novosibirskaja_obl/"

def task():
    years = {
        #0: [date(2016, 1, 1), date(2017, 12, 31)],
        #1: [date(2018, 1, 1), date(2019, 12, 31)],
        2: [date(2020, 1, 1), date(2021, 12, 31)],
        3: [date(2022, 1, 1), date(2023, 12, 31)],
        4: [date(2024, 1, 1), date(2025, 12, 31)],
    }
    
    # скачивание данных
    for year in years:
        try:
            ftp = ftplib.FTP(host='95.167.245.94', timeout=10)
            ftp.login(user='free', passwd='free')
            ftp.af = socket.AF_INET6
            ftp_downloader = FtpDownloader(ftp)
            ftp_downloader.download_zip(source, directory_str, years[year][0], years[year][1])
            ftp.close()
            print(f"[{years[year][0]} - {years[year][1]}] successful\n")
        except:
            continue

    # парсинг
    tender_parser = TenderParser()
    tender_parser.parse_json(directory_str)

def main():
    isStarted = False
    while not isStarted:
        print("Доступные команды: \n1) start\n2) download\n3) parseStructure\n4) parseJson\n5) demoParseJson\n6) exit")
        command_str = input("Введите комманду: ").lower()

        if command_str == "start":
            isStarted = True

        elif command_str == "download":
            ftp = ftplib.FTP(host='95.167.245.94', user='free', passwd='free', timeout=100)
            ftp_downloader = FtpDownloader(ftp)
            ftp_downloader.download_zip(source, directory_str, date(2020, 1, 1), date(2024, 1, 1))

        elif command_str == "parsejson":
            tender_parser = TenderParser()
            tender_parser.parse_json(directory_str)

        elif command_str == "demoparsejson":
            tender_parser = TenderParser()
            tender_parser.demo_parse_json(directory_str)

        elif command_str == "parsestructure":
            structure_tender_parser = StructureTenderParser()
            structure_tender_parser.parse(directory_str)

        elif command_str == "exit":
            break

        else:
            print("Неизвестная каманда!")

        if isStarted:
            schedule.every().day.at("17:02").do(task)
            while True:
                schedule.run_pending()

if __name__ == "__main__":
    main()

