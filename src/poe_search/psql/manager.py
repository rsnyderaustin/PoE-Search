
import psycopg2

class PsqlManager:

    def __init__(self,
                 db_name: str = None,
                 user: str = None,
                 host: str = None,
                 port: int = None):
        self._db_name = db_name or "poe_search"
        self._user = user or "austinsnyder"
        self._host = host or "localhost"
        self._port = port or 5432

