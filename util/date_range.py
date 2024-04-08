from datetime import date

class DateRange:
    def __init__(self, start: date, end: date) -> None:
        self.start: date = start
        self.end: date = end