
import time
import requests


class WikiApiPull:
    _headers = {
        "User-Agent": 'austin_snyder - austin.snyder55@gmail.com - PoESearchProject'
    }

    def __init__(self,
                 table_name: str,
                 fields: list[str],
                 page_size: int = 200,
                 runtime_seconds_limit: int = 300
                 ):
        self._table_name = table_name
        self._fields = fields
        self._page_size = page_size
        self._runtime_seconds_limit = runtime_seconds_limit

        self._errors = 0
        self._request_offset = 0

    def _determine_backoff_length(self):
        return 0.05 * 1.5**self._errors

    def _should_exit_pull(self,
                          request_time_start: float):
        current_time = time.time()
        time_after_backoff_length = current_time + self._determine_backoff_length()
        mandatory_exit_time = request_time_start + self._runtime_seconds_limit

        return time_after_backoff_length > mandatory_exit_time

    def fetch_table_data(self,
                         table_name: str,
                         fields: list[str],
                         page_size: int = 200,
                         seconds_limit: int = 300):
        request_time_start = time.time()
        current_time = request_time_start

        data = []

        params = {
            "action": "cargoquery",
            "format": "json",
            "tables": table_name,
            "fields": ",".join(fields),
            "limit": page_size
        }

        while not self._should_exit_pull(request_time_start=request_time_start):
            params["offset"] = self._request_offset
            try:
                response = requests.get(
                    "https://www.poewiki.net/w/api.php",
                    params=params,
                    headers=self.__class__._headers
                )
                response.raise_for_status()
            except Exception as e:
                print(f"Encountered error while pulling from Wiki API. Pull details:"
                      f"\n\tTable: {table_name}"
                      f"\n\tFields: {fields}"
                      f"\n\tCurrent offset: {self._request_offset}"
                      f"\n\tPull limit: {page_size}"
                      )
                if self._should_exit_pull(request_time_start=request_time_start):
                    raise e
                time.sleep(self._determine_backoff_length())

                self._errors += 1

            data = response.json()['cargoquery']
        return response.json()['cargoquery']
