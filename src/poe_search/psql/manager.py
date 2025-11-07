
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import ProgrammingError


class PsqlManager:

    def __init__(self,
                 db_password: str,
                 db_name: str = None,
                 user: str = None,
                 host: str = None,
                 port: int = None):
        _db_name = db_name or "poe_search"
        _user = user or "austinsnyder"
        _host = host or "localhost"
        _port = port or 5432

        self._engine = create_engine(f"postgresql+psycopg2://{_user}:{db_password}@{_host}:{_port}/{_db_name}")
        self._metadata = MetaData()
        self._metadata.reflect(self._engine)

        self._conn = self._engine.begin()

    @staticmethod
    def hash_df(df: pd.DataFrame):
        return hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values.tobytes()).hexdigest()

    def _create_table(self,
                      psql_table_name: str) -> Table:
        return Table(psql_table_name, self._metadata, autoload_with=self._engine)

    def fetch_table_hash(self,
                         psql_table_name: str):
        query = text("SELECT data_hash FROM table_hashes WHERE table_name = :name")

        psql_table = self._create_table(psql_table_name)
        with self._engine.begin() as conn:
            result = conn.execute(query, {"name": psql_table}).fetchone()

        return result[0] if result else None

    def update_table_hash(self,
                          df_hash,
                          psql_table_name: str):
        # SQL query: insert or update
        query = text("""
                    INSERT INTO table_hashes (table_name, data_hash)
                    VALUES (:name, :hash)
                    ON CONFLICT (table_name)
                    DO UPDATE SET data_hash = EXCLUDED.data_hash, last_updated = NOW()
                """)

        psql_table = self._create_table(psql_table_name)
        # Execute query with connection
        with self._engine.begin() as conn:
            conn.execute(query, {"name": psql_table.name, "hash": df_hash})
            conn.commit()

    @staticmethod
    def fetch_table_id_column(self,
                              psql_table_name: str):
        query = text("SELECT id_column_name FROM tables_metadata WHERE table_name = :psql_table")

        psql_table = self._create_table(psql_table_name)
        with self._engine.begin() as conn:
            result = conn.execute(query, {"psql_table": psql_table.name}).fetchone()

        return result[0] if result else None

    def fetch_table_data(self,
                         psql_table_name: str) -> pd.DataFrame:
        psql_table = self._create_table(psql_table_name)
        try:
            query = text("SELECT * FROM :psql_table")
            with self._engine.begin() as conn:
                result = conn.execute(query, {"psql_table": psql_table.name})

            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        except ProgrammingError as err:
            print(f"PostgreSql {psql_table.name} does not exist.")
            raise err

        return df

    def update_table(self,
                     psql_table_name: str,
                     new_df: pd.DataFrame,
                     new_df_id_col_name: str):
        records = new_df.to_dict(orient='records')
        psql_table = self._create_table(psql_table_name)
        statement = insert(psql_table).values(records)
        statement = statement.on_conflict_do_update(
            index_elements=[new_df_id_col_name],
            set_={col.name: statement.excluded[col.name] for col in psql_table.columns if col.name != new_df_id_col_name}
        )

        with self._engine.begin() as conn:
            conn.execute(statement)

