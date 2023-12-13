import logging
import azure.functions as func
from .common.database_Util import DatabaseUtil
import os
from dotenv import load_dotenv
from .logic.report_ronding_time import run


def main(req: func.HttpRequest) -> func.HttpResponse:
    table_times = req.params.get('table_times')

    
    if not table_times:
        try:
            req_body = req.get_json()
    
        except ValueError:
            pass
        else:
            table_times = req_body.get('table_times')

    if table_times:
        result = run(table_times)

    else:
        result = "Please pass a table_times on the query string or in the request body"
        
    return func.HttpResponse(result, mimetype="application/json")

