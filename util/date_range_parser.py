from datetime import date
from util.date_range import DateRange

class DataRangeParser:
    def parse(self, file_name: str) -> DateRange:
        data_start = self._get_date_in_str_by_index(file_name, 3)
        data_end = self._get_date_in_str_by_index(file_name, 4)
        
        return DateRange(data_start, data_end)

    def _get_date_in_str_by_index(self, str: str, index: int) -> date:
        year = int(str.split('_')[index][:-2][0:4])
        month = int(str.split('_')[index][:-2][4:6])
        day = int(str.split('_')[index][:-2][6:8])
        return date(year, month, day)