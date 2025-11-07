
import time
import requests


class WikiTablePull:
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
        self._pull_page_size = page_size
        self._runtime_seconds_limit = runtime_seconds_limit

        self._pull_start_time = None
        self._current_loop_attempts = 0
        self._request_offset = 0
        self._successfull_loops = 0

        self._data = []

    def __str__(self):
        return (
            f"Pull details:"
            f"\n\tTable: {self._table_name}"
            f"\n\tFields: {self._fields}"
            f"\n\tPull limit: {self._pull_page_size}"
            f"\n\tCurrent offset: {self._request_offset}"
            f"\n\tTime elapsed (s): {time.time() - self._pull_start_time}"
            f"\n\tCurrent loop attempts: {self._current_loop_attempts}"
            f"\n\tSuccessful loops: {self._successfull_loops}"
        )

    @property
    def _pull_params(self):
        return {
            "action": "cargoquery",
            "format": "json",
            "tables": self._table_name,
            "fields": ",".join(self._fields),
            "limit": self._pull_page_size
        }

    def _determine_backoff_length(self):
        return 0.05 * 1.5**self._current_loop_attempts

    def _should_exit_pull(self):
        current_time = time.time()
        time_after_backoff_length = current_time + self._determine_backoff_length()
        mandatory_exit_time = self._pull_start_time + self._runtime_seconds_limit

        return time_after_backoff_length > mandatory_exit_time

    def fetch_table_data(self):
        self._pull_start_time = time.time()

        while True:
            print(f"Attempting pull {self._current_loop_attempts} of loop {self._successfull_loops}."
                  f"\n\tHave pulled {len(self._data)} records.")
            try:
                response = requests.get(
                    "https://www.poewiki.net/w/api.php",
                    params=self._pull_params | {"offset": self._request_offset},
                    headers=self.__class__._headers
                )
                response.raise_for_status()
            except Exception as e:
                print(f"Encountered error while pulling from Wiki API.\n{self.__str__()}")
                if self._should_exit_pull():
                    raise e
                time.sleep(self._determine_backoff_length())

                self._current_loop_attempts += 1

                continue
            page_data = response.json()['cargoquery']
            self._data.extend(page_data)
            num_results = len(page_data)
            self._request_offset += num_results

            if num_results < self._pull_page_size:
                return self._data

            self._successfull_loops += 1
            time.sleep(0.05)

        raise RuntimeError(f"Unexpectedly reached end of fetch_table_data.\n{self.__str__()}")


def pull_image_url(file_name: str):
    return WikiImageUrlPull(file_name).fetch_image_url()


class WikiImageUrlPull:

    def __init__(self,
                 file_name: str,
                 runtime_seconds_limit: int = 300,):
        self._file_name = file_name
        self._runtime_seconds_limit = runtime_seconds_limit

        self._pull_start_time = None
        self._current_loop_attempts = 0

    def __str__(self):
        return (
            f"Pull details:"
            f"\n\tFile name: {self._file_name}"
            f"\n\tTime elapsed (s): {time.time() - self._pull_start_time}"
            f"\n\tCurrent loop attempts: {self._current_loop_attempts}"
        )

    @property
    def _params(self):
        return {
            "action": "query",
            "format": "json",
            "titles": self._file_name,
            "prop": "imageinfo",
            "iiprop": "url"
        }

    def _determine_backoff_length(self):
        return 0.05 * 1.5**self._current_loop_attempts

    def _should_exit_pull(self):
        current_time = time.time()
        time_after_backoff_length = current_time + self._determine_backoff_length()
        mandatory_exit_time = self._pull_start_time + self._runtime_seconds_limit

        return time_after_backoff_length > mandatory_exit_time

    def fetch_image_url(self):
        self._pull_start_time = time.time()
        while True:
            try:
                response = requests.get("https://www.poewiki.net/w/api.php", params=self._params)
                response.raise_for_status()
            except Exception as err:
                print(f"Encountered error while pulling from Wiki API.\n{self.__str__()}")
                if self._should_exit_pull():
                    raise err
                time.sleep(self._determine_backoff_length())

                self._current_loop_attempts += 1

                continue

            data = response.json()

            page = next(iter(data["query"]["pages"].values()))
            url = page["imageinfo"][0]["url"]
            return url

        raise RuntimeError(f"Unexpectedly reached end of fetch_image_url.\n{self.__str__()}")
