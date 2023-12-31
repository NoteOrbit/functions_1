import azure.functions as func
from .services.report_ronding_time import run

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

