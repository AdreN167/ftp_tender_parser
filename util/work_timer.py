import time

class WorkTimer():
    def __init__(self) -> None:
        self.start_time: float = 0
        self.seconds_total: float = 0
        self.days: int = 0
        self.hours: int = 0
        self.minutes: int = 0
        self.seconds: int = 0

    def start(self) -> None:
        self.start_time = time.time()

    def calculate_time(self, index: int, count: int) -> None:
        seconds_delta = (time.time() - self.start_time)
        self.seconds_total += seconds_delta
        seconds_mean = (self.seconds_total / (index + 1)) * (count - index)
        days, hours, minutes, seconds = self._convert_seconds(seconds_mean)
        self.days = int(days)
        self.hours = int(hours)
        self.minutes = int(minutes)
        self.seconds = int(seconds)

    def _convert_seconds(self, seconds):
        minutes = seconds // 60
        hours = minutes // 60
        days = hours // 24

        seconds %= 60
        minutes %= 60
        hours %= 24

        return days, hours, minutes, seconds
