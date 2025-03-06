import boto3
import time


def run(
        query: str, aws_region: str, athena_output_bucket: str,
        athena_output_path: str, athena_workgroup: str) -> str:
    """

    Args:
        query (str):
        aws_region (str):
        athena_output_bucket (str): s3 bucket name
        Example: bd-dev-athena-results
        athena_output_path (str): path inside 'athena_output_bucket' where
        athena stores results. Example: folder_name or 'folder1/folder2/'
        Note: path must have slash '/' at the end
        athena_workgroup (str): athena workgroup

    Returns:
        str: query status
    """
    queryid = None
    try:
        # session = boto3.session.Session(profile_name='dev-bigdata')
        # athena_client = session.client('athena', region_name=aws_region)

        athena_client = boto3.client('athena', region_name=aws_region)

        config = {
            'OutputLocation':
            f's3://{athena_output_bucket}/{athena_output_path}'}

        print(f"Running query:\n{query}")
        response = athena_client.start_query_execution(
            QueryString=query, ResultConfiguration=config,
            WorkGroup=athena_workgroup,)
        queryid = response['QueryExecutionId']
        # logger.info(queryid)
        # Athena needs time to store the result file in S3
        for sleep_time in [5, 10, 20]:
            result = athena_client.get_query_execution(
                QueryExecutionId=queryid)
            state_value = result["QueryExecution"]["Status"]["State"]
            if state_value == "RUNNING" or state_value == "QUEUED":
                time.sleep(sleep_time)
            else:
                break

        if state_value == "SUCCEEDED":
            return "SUCCEEDED"
        else:
            message = f"Error in athena query {queryid}. "
            if 'AthenaError' in result['QueryExecution']['Status'].keys() or \
                'StateChangeReason' in \
                    result['QueryExecution']['Status'].keys():
                if 'AthenaError' in result['QueryExecution']['Status'].keys():
                    message += result['QueryExecution']['Status'][
                        'AthenaError']['ErrorMessage']
                else:
                    message += result[
                        'QueryExecution']['Status']['StateChangeReason']
            print(message)
            return "ATHENA_QUERY_ERROR"
    except Exception as e:
        print("Error while running Athena query: "+str(e) +
                     f"\nQueryID:{queryid}")
        return "ATHENA_QUERY_ERROR"
