import boto3
import os

# athena constants
DATABASE = os.environ["athenaDatabase"]
TABLE = 'artist_data'
S3_OUTPUT = 's3://athena-query-results-udacc/'


def lambda_handler(event, context):
    # dq query
    query = 'SELECT COUNT(1) as cnt FROM "dbsparkify"."artist_data" WHERE name IS NULL;'

    # athena client
    client = boto3.client('athena')

    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': S3_OUTPUT,
        }
    )

    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)

    # check query status
    while True:
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    print(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])

    # retrieve the value of the count column of our `query`
    if result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'] == '0':

        check_result = "quality check passed"

    else:
        check_result = "quality check not passed"


    return check_result