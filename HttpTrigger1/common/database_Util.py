import logging
import re
import time
from datetime import timedelta
from io import StringIO

import numpy as np
import pandas as pd
import psycopg2 as dbconnecter


logger = logging.getLogger(__name__)


class Singleton(type):

    def __init__(cls, name, bases, dict):
        super(Singleton, cls).__init__(name, bases, dict)
        cls._instance = None

    def __call__(cls, *args, **kw):
        if cls._instance is None:
            cls._instance = super(Singleton, cls).__call__(*args, **kw)

        return cls._instance

    def reset_singleton(cls):
        cls._instance = None


class DatabaseUtil(metaclass=Singleton):

    def __init__(self,
                 username: str,
                 password: str,
                 host: str,
                 database: str,
                 port: int = 5432,
                 table_schema: str = 'public',
                 autocommit=True,
                 max_to_try: int = 3,
                 sleep_time: int = 5,
                 verbose: int = 1,
                 str_var='%s'):

        # ─── Arguments ───────────────────────────────────────────────────
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
        self.database = database
        self.table_schema = table_schema
        self.autocommit = autocommit

        self.max_to_try = max(max_to_try, 1)
        self.sleep_time = sleep_time

        self.str_var = str_var

        # ─── Declar Variable ─────────────────────────────────────────────
        self.start_time = None
        self.conn = None

        if (logger is not None):
            self.info = logger.info
            self.error = logger.error
            self.debug = logger.debug
        else:
            self.info = self.error = self.debug = print

    def __setattr__(self, name: str, value):
        self.__dict__[name] = value

    def __getattr__(self, name: str):
        return self.__dict__.get(name, None)

    def __enter__(self):
        self.start_connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connect()

    def sql_prep(self, sql_cmd: str):
        sql_out = re.sub(r"['\"]{2,}", '', sql_cmd)
        sql_out = re.sub(r"\s+", ' ', sql_out)
        sql_out = re.sub(r"(%s)|[?]", self.str_var, sql_out)
        return sql_out

    @property
    def connecter(self):
        if self.conn is not None:
            return self.conn

        else:
            for try_con in range(1, self.max_to_try + 1):
                try:
                    conn = dbconnecter.connect(
                        user=self.username,
                        password=self.password,
                        host=self.host,
                        port=self.port,
                        database=self.database,
                    )
                    conn.autocommit = self.autocommit

                    self.conn = conn
                    self.start_time = time.time()
                    # connected
                    return self.conn
                except (dbconnecter.DatabaseError, dbconnecter.InternalError,
                        dbconnecter.InterfaceError,
                        dbconnecter.OperationalError) as e:
                    self.error(f"Error connecting to database : {e}")
                    self.info(
                        f"sleep for {self.sleep_time} seconds before trying again. [{try_con}/{self.max_to_try}]"
                    )
                    time.sleep(self.sleep_time)

        raise ConnectionError('Max time try connect exceed.')

    def start_connect(self):
        self.connecter

    def close_connect(self):
        if self.conn is not None:
            self.conn.close()
            end_time = time.time() - self.start_time
            self.conn = None
            self.start_time = None
            self.info("Close connection to database, time elapsed {}".format(
                str(timedelta(seconds=end_time))))

    def commit(self):
        """
        Commit changes to the current state .
        """
        if self.conn is not None:
            self.conn.commit()

    def rollback(self):
        """
        Roll back the current session .
        """
        if self.conn is not None:
            self.info('Rollback')
            self.conn.rollback()

    def ping(self) -> bool:
        if self.conn is not None:
            try:
                cursor = self.connecter.cursor()
                cursor.execute('select 1;')
                cursor.fetchall()
                return True
            except dbconnecter.Error:
                return False
        else:
            self.info(
                "Ping Fail: Conecter don't exists, please connect before call ping"
            )
            return False

    def cast_types(self, data):
        for i in range(len(data)):
            for j in range(len(data[i])):
                data[i][j] = pass_type_value(data[i][j])
        return data

    # def copy_from_expert(self, df, table, schema, index=False, encoding='UTF-8'):
    #     for try_con in range(self.max_to_try + 1):
    #         """
    #         Here we are going save the dataframe in memory 
    #         and use copy_from() to copy it to the table
    #         """
    #         # save dataframe to an in memory buffer
    #         cursor = self.connecter.cursor()
    #         schema_table_name_query = '{}.{}'.format(schema, table)
    #         column_names_query = f"""SELECT column_name FROM information_schema.columns WHERE table_schema || '.' || table_name = '{schema_table_name_query}' AND column_name NOT ILIKE 'id'"""
    #         cursor.execute(column_names_query)
    #         column_names = [row[0].upper() for row in cursor.fetchall()]

    #         buffer = StringIO()
    #         df.columns = df.columns.str.upper()
    #         selected_columns = df.loc[:, column_names]
    #         selected_columns.to_csv(buffer, index=index, header=True, sep=",", encoding=encoding)
    #         buffer.seek(0)
    #         # print(selected_columns.columns)

    #         columns_db = ', '.join('"{}"'.format(k) for k in column_names)
    

    #         schema_table_name = '"{}"."{}"'.format(schema, table)
    #         # cursor = self.connecter.cursor()
    #         try:
    #             cursor.copy_expert(f"copy {schema_table_name} ({columns_db}) from stdin with csv header", buffer)
    #             # cursor.copy_expert(f"copy {schema_table_name} from stdin with csv header)", buffer)
    #             # cursor.copy_expert(f"copy {schema_table_name} from stdin WITH CSV HEADER", buffer)
    #             return "Done execute SQL"
    #         except dbconnecter.DataError as e:
    #             raise e
    #         except (dbconnecter.DatabaseError, dbconnecter.InternalError,
    #                 dbconnecter.InterfaceError,
    #                 dbconnecter.OperationalError) as e:
    #             self.error(f"Error : {e}, Start reconnect")
    #             self.rollback()
    #             self.info(
    #                 f"sleep for {self.sleep_time} seconds before trying again. [{try_con+1}/{self.max_to_try+1}]"
    #             )
    #             time.sleep(self.sleep_time)
    #             self.start_connect()

    
    def copy_from_expert(self, df, table, schema, index=False, encoding='UTF-8'):
        try_count = 0
        max_attempts = self.max_to_try + 1
    
        # Fetch column names for the table
        schema_table_name_query = '{}.{}'.format(schema, table)
        column_names_query = """SELECT column_name FROM information_schema.columns 
                               WHERE table_schema || '.' || table_name = %s 
                               AND column_name NOT ILIKE 'id'"""
        with self.connecter.cursor() as cursor:
            cursor.execute(column_names_query, (schema_table_name_query,))
            column_names = [row[0].upper() for row in cursor.fetchall()]
    
        # Reorder DataFrame columns to match the table
        df = df[column_names]
    
        while try_count < max_attempts:
            try:
                # Save DataFrame to an in-memory buffer
                buffer = StringIO()
                df.to_csv(buffer, index=index, header=True, sep=",", encoding=encoding)
                buffer.seek(0)
    
                columns_db = ', '.join('"{}"'.format(k) for k in column_names)
                schema_table_name = '"{}"."{}"'.format(schema, table)
    
                # Use copy_from with a context manager for the database cursor
                with self.connecter.cursor() as cursor:
                    cursor.copy_expert(f"COPY {schema_table_name} ({columns_db}) FROM STDIN WITH CSV HEADER", buffer)
    
                return "Done execute SQL"
    
            except dbconnecter.DataError as e:
                raise e
    
            except (dbconnecter.DatabaseError, dbconnecter.InternalError, dbconnecter.InterfaceError, dbconnecter.OperationalError) as e:
                self.error(f"Error: {e}, Start reconnect")
                self.rollback()
                self.info(f"Sleep for {self.sleep_time} seconds before trying again. [{try_count + 1}/{max_attempts}]")
                time.sleep(self.sleep_time)
                self.start_connect()
    
            try_count += 1
    
        # Return an appropriate message or raise an error if the maximum number of retries is reached.
        return "Failed to execute SQL after max attempts"  # or raise an exception here


    def execute_mogrify(self, table, schemas, cols, value, type_value):
        """
        Using cursor.mogrify() to build the bulk insert query
        then cursor.execute() to execute the query
        """
        # Comma-separated dataframe columns
        # SQL quert to execute
        cursor = self.connecter.cursor()
        values = [cursor.mogrify(f"({type_value})", tup).decode('utf8') for tup in value]
        query  = """INSERT INTO "%s"."%s"(%s) VALUES """ % (schemas, table, cols) + ",".join(values)

        try:
            time_start = time.time()
            cursor.execute(query, value)
            self.info("Done execute SQL code, time elapsed {}".format(
                str(timedelta(seconds=time.time() - time_start))))
            return "Done execute SQL"
        except (dbconnecter.DatabaseError, dbconnecter.InternalError,
                dbconnecter.InterfaceError,
                dbconnecter.OperationalError) as error:
            print("Error: %s" % error)
            time.sleep(self.sleep_time)
            raise error
        except Exception as e:
            raise e
        # raise ConnectionError("Failed to connect database")


    def copy_from_stringio(self,
                           df,
                           table_name: str,
                           table_schema: str = None):
        """
        Here we are going save the dataframe in memory 
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=True)
        buffer.seek(0)

        cursor = self.connecter.cursor()
        try:
            # PRE PROCESS
            time_start = time.time()
            # if table_schema:
            #     table_name = '{}.{}'.format(table_schema, table_name)

            cursor.execute(f'SET search_path TO {table_schema}')
            cursor.copy_from(buffer, table_name, sep=",")
            self.info("Done execute SQL code, time elapsed {}".format(
                str(timedelta(seconds=time.time() - time_start))))
            return 'Done execute SQL code'
        except (dbconnecter.DatabaseError, dbconnecter.InternalError,
                dbconnecter.InterfaceError,
                dbconnecter.OperationalError) as error:
            print("Error: %s" % error)
            self.rollback()
            time.sleep(self.sleep_time)
        except dbconnecter.DataError as e:
            raise e
        except Exception as e:
            raise e
        raise ConnectionError("Failed to connect database")
    
    def query_data_to_df(self,
                         sql_cmd: str):
        try:
            time_start = time.time()
            cursor = self.connecter.cursor()
            
            cursor.execute(sql_cmd)
            data = cursor.fetchall()
            cols = []
            for elt in cursor.description:
                cols.append(elt[0])
            df = pd.DataFrame(data=data, columns=cols)
            return df
        except (dbconnecter.DatabaseError, dbconnecter.InternalError,
                dbconnecter.InterfaceError,
                dbconnecter.OperationalError) as error:
            print("Error: %s" % error)
            self.rollback()
            time.sleep(self.sleep_time)
        except Exception as e:
            raise e
        raise ConnectionError("Failed to connect database")

    def execute(self,
                sql_cmd: str,
                data: list = None,
                output_as_list: bool = True):
        output = list() if output_as_list else None
        for try_con in range(1, self.max_to_try + 1):
            try:
                # PRE PROCESS
                time_start = time.time()
                cursor = self.connecter.cursor()

                # EXECUTE PROCESS
                if isinstance(data, list):
                    sql_exec = self.sql_prep(sql_cmd)
                    data = list(map(list, data)) if isinstance(data[0], (tuple)) else data
                    data = self.cast_types(data)
                    data = list(map(tuple, data))
                    cursor.executemany(sql_exec, data)
                    self.info(f'complete execute sql with {len(data)} rows.')
                    self.debug(
                        f"Data size: ({len(data)}), example data: [{data[0] if len(data) > 0 else ''}, sql command: {sql_exec}]"
                    )
                else:
                    cursor.execute(sql_cmd)
                    self.info('complete execute sql.')
                    try:
                        output = cursor.fetchall()
                    except dbconnecter.Error:
                        self.debug("Can't fetch data from cursor")

                # POST PROCESS
                cursor.close()
                self.info("Done execute SQL code, time elapsed {}".format(
                    str(timedelta(seconds=time.time() - time_start))))
                return output

            except (dbconnecter.InterfaceError) as IE:
                self.close_connect()
                self.info(
                    f"sleep for {self.sleep_time} seconds before trying again. [{try_con}/{self.max_to_try}]"
                )
                time.sleep(self.sleep_time)

            except (dbconnecter.DatabaseError, dbconnecter.InternalError,
                    dbconnecter.OperationalError) as e:
                self.rollback()
                self.error(f"Error : {e}, Start reconnect")
                self.info(
                    f"sleep for {self.sleep_time} seconds before trying again. [{try_con}/{self.max_to_try}]"
                )
                time.sleep(self.sleep_time)
            except dbconnecter.DataError as e:
                raise e
            except Exception as e:
                raise e
        raise ConnectionError("Failed to connect database")

    def executeSQL(self, _exec, _param=[], is_many=False, is_result=True):
        for try_con in range(self.max_to_try + 1):
            try:
                time_start = time.time()
                cursor = self.connecter.cursor()

                # convert variable to that mariaDB support
                _param = [_param] if (not is_many) else list(map(list, _param))
                _param = self.cast_types(_param)
                _param = list(map(tuple, _param)) if is_many else _param[-1]

                showsql = self.sql_prep(_exec)
                self.info(f'Start execute SQL code : {showsql}')
                if (is_many and (len(_param) > 0)):
                    cursor.executemany(showsql, _param)
                    output = None
                else:
                    if (len(_param) > 0):
                        cursor.execute(showsql, _param)
                    else:
                        cursor.execute(showsql)
                    output = cursor.fetchall() if is_result else None
                cursor.close()
                end_time = time.time() - time_start
                self.info("Done execute SQL code, time elapsed {}".format(
                    str(timedelta(seconds=end_time))))
                return output
            except (dbconnecter.DatabaseError, dbconnecter.InternalError,
                    dbconnecter.InterfaceError,
                    dbconnecter.OperationalError) as e:
                self.error(f"Error : {e}, Start reconnect")
                self.rollback()
                self.info(
                    f"sleep for {self.sleep_time} seconds before trying again. [{try_con+1}/{self.max_to_try+1}]"
                )
                time.sleep(self.sleep_time)
                self.start_connect()
            except dbconnecter.DataError as e:
                raise e
            except Exception as e:
                raise e
        raise ConnectionError("Failed to connect database")

    def query_columns_name_by_table(self,
                                    table_name: str,
                                    table_schema: str = None,
                                    table_catalog: str = None):
        if table_schema is None:
            table_schema = self.table_schema

        if table_catalog is None:
            table_catalog = self.database

        sql_col = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' \
            AND TABLE_SCHEMA = '{table_schema}' AND TABLE_CATALOG='{table_catalog}'  ORDER BY ORDINAL_POSITION"

        curs = self.execute(sql_col)
        cols = sum(map(list, curs), [])
        return cols

    @classmethod
    def extract_columns_name_from_sql(cls, sql: str):
        str_ = re.search(r"(?<=SELECT).*?(?=FROM)", sql, re.IGNORECASE)
        if str_:
            str_ = str_.group(0)
            cols = str_.split(',')
            cols = list(map(str.strip, cols))
            return cols
        return []

    @classmethod
    def extract_table_name_from_sql(cls, sql: str):
        out = re.search(r"(?<=(FROM|INTO))(.*=?)", sql,
                        re.IGNORECASE).group(0).strip().split()[0]
        out = re.sub('["\']', '', out)
        return out

    def query_dataframe_by_sql(self,
                               sql: str,
                               with_column: bool = True) -> pd.DataFrame:
        cols = self.extract_columns_name_from_sql(sql)
        if '*' in cols and with_column:
            table = self.extract_table_name_from_sql(sql)
            cols = self.query_columns_name_by_table(table)
        cols = [re.sub('["*`]', '', col) for col in cols]
        curs = self.execute(sql)
        dfx = pd.DataFrame(curs,
                           columns=cols) if with_column else pd.DataFrame(curs)
        return dfx


def pass_type_value(data):
    val = data
    val = 0 if val == "-" else val
    val = int(val) if isinstance(val, (int, np.integer, np.int64)) else val
    val = float(val) if isinstance(val,
                                   (float, np.floating, np.float64)) else val
    val = pd.to_datetime(val).to_pydatetime() if isinstance(
        val, (np.datetime64, pd.Timestamp)) else val
    val = None if pd.isnull(val) else val
    return val