from .common.database_Util import DatabaseUtil
import os
from dotenv import load_dotenv


load_dotenv()
connection = DatabaseUtil(username=os.getenv('DB_USER'),
                        password=os.getenv('DB_PASSWORD'),
                        host=os.getenv('DB_HOST'),
                        database=os.getenv('DB_NAME'),
                        port=os.getenv('DB_PORT'),
                        table_schema="DBO")