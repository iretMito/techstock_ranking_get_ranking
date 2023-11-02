import datetime
import time

import boto3 as boto3

# athena constant
DATABASE = 'dev_techstock'
TABLE = 'scores'

# S3 constant
S3_OUTPUT = 's3://mito-bucket/test-json/results/'

# number of retries
RETRY_COUNT = 50

# exam list
EXAM_LIST = ['SAA', 'SOA', 'SAP', 'CLF', 'DVA', 'DOP', 'ANS', 'SCS', 'DAS', 'MLS', 'DBS', 'PAS', 'CDL', 'ACE', 'PCA',
             'ALL']


def lambda_handler(event, context):
    print(event)
    exam = event['exam']
    start_date = event['start_date']
    end_date = event['end_date']

    if not exam or not start_date or not end_date:
        return {'result': 'error'}

    if exam not in EXAM_LIST:
        return {'result': 'error'}

    # check if start_date and end_date are in the correct format
    try:
        datetime.datetime.strptime(start_date, '%Y-%m-%d')  # YYYY-MM-DD
        datetime.datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return {'result': 'error'}

    # created query
    if exam == 'ALL':
        query = "SELECT name, SUM(score) AS total_score FROM %s.%s where date('%s') <= \"date\" AND \"date\" <= date('%s') GROUP BY name ORDER BY total_score DESC;" % (
            DATABASE, TABLE, start_date, end_date)
    else:
        query = "SELECT name, SUM(score) AS total_score FROM %s.%s where date('%s') <= \"date\" AND \"date\" <= date('%s') AND \"exam\" = '%s' GROUP BY name ORDER BY total_score DESC;" % (
            DATABASE, TABLE, start_date, end_date, exam)
    print('query: ' + query)

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

    # get query execution id
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)

    # get execution status
    for i in range(1, 1 + RETRY_COUNT):

        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            print('Query Status: ' + query_execution_status)
            response = {'result': 'error'}
            return response

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(0.1 * i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        response = {'result': 'error'}
        return response

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    print(result)

    ranking = []

    for i in range(1, len(result['ResultSet']['Rows'])):
        rank = {
            'rank': i,
            'name': result['ResultSet']['Rows'][i]['Data'][0]['VarCharValue'],
            'score': result['ResultSet']['Rows'][i]['Data'][1]['VarCharValue'],
        }
        ranking.append(rank)

    response = {
        'result': 'success',
        'start_date': start_date,
        'end_date': end_date,
        'exam': exam,
        'ranking': ranking
    }
    
    return response
