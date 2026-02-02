from datetime import datetime
import json
import boto3
import os
from datetime import datetime
import logging
from botocore.client import Config
import pprint
import requests
import textwrap
import time
import psycopg2
import re
from botocore.exceptions import ClientError

logger = logging.getLogger("Airflow.task")
acceptable_response_codes = [200, 201]


def get_region():
    global _region_name
    _region_name = 'us-east-1'
    return _region_name


def client(region_name, config):
    global emr
    emr = boto3.client('emr', region_name=region_name, config=config)


def check_emr_cluster(cluster_name):
    cluster_id = None
    try:
        response = emr.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'],
        )
        for cluster in response['Clusters']:
            if cluster['Name'] == cluster_name:
                cluster_id = cluster['Id']
    except Exception as e:
        logger.error(str(e))
    return cluster_id


def create_emr_cluster(emr_conf, region_name, cluster_name, AppCode, Contact):
    log_bucket = emr_conf['log_bucket']
    release_label = emr_conf['release_label']
    master_instance_type = emr_conf['master_instance_type']
    num_core_nodes = emr_conf['num_core_nodes']
    core_node_instance_type = emr_conf['core_node_instance_type']

    EmrManagedSlaveSecurityGroup = emr_conf['EmrManagedSlaveSecurityGroup']
    EmrManagedMasterSecurityGroup = emr_conf['EmrManagedMasterSecurityGroup']
    ServiceAccessSecurityGroup = emr_conf['ServiceAccessSecurityGroup']

    Ec2SubnetId = emr_conf['Ec2SubnetId']

    JobFlowRole = emr_conf['JobFlowRole']
    ServiceRole = emr_conf['ServiceRole']
    AutoScalingRole = emr_conf['AutoScalingRole']

    VolumesPerInstance = 1
    if ('VolumesPerInstance' in emr_conf): VolumesPerInstance= emr_conf['VolumesPerInstance']

    SizeInGB = 64
    if ('SizeInGB' in emr_conf): SizeInGB = emr_conf['SizeInGB']

    # Ec2KeyName = emr_conf['Ec2KeyName']
    logs_path = 's3://{}/emr_logs/'.format(log_bucket)

    try:
        response = emr.run_job_flow(
            Name=cluster_name,
            LogUri=logs_path,
            ReleaseLabel=release_label,
            Applications=[
                {
                    'Name': 'Spark'
                },
                {
                    'Name': 'Hadoop'
                },
                {
                    'Name': 'Hive'
                },
                {
                    'Name': 'Tez'
                },
                {
                    'Name': 'Livy'
                },
                {
                    'Name': 'Ganglia'
                }
            ],
            Configurations=[
                {
                    "Classification": "hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                        "hive.metastore.schema.verification": "false"
                    }
                },
                {
                    "Classification": "spark-hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    }
                },
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.executor.cores": emr_conf['spark_cores'],
                        "spark.driver.cores": emr_conf['spark_cores'],
                        "spark.driver.maxResultSize": "0",
                        "spark.executor.memory": emr_conf['spark_memory'],
                        "spark.driver.memory": emr_conf['spark_memory'],
                        "spark.executor.memoryOverhead": emr_conf['spark_memoryOverhead'],
                        "spark.driver.memoryOverhead": emr_conf['spark_memoryOverhead'],
                        "spark.executor.instances": emr_conf['spark_instances'],
                        "spark.default.parallelism": emr_conf['spark_parallelism'],
                        "spark.dynamicAllocation.enabled": "false",
                        "yarn.nodemanager.vmem-check-enabled": "false",
                        "yarn.nodemanager.pmem-check-enabled": "false",
                        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35  -XX:OnOutOfMemoryError='kill -9 %p'",
                        "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35  -XX:OnOutOfMemoryError='kill -9 %p'",
                        "spark.driver.extraClassPath": ":/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/goodies/lib/emr-spark-goodies.jar:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar:/home/hadoop/jars/*",
                        "spark.executor.extraClassPath": ":/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/goodies/lib/emr-spark-goodies.jar:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar:/home/hadoop/jars/*",
                    }
                },
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {
                                "PYSPARK_PYTHON": "/usr/bin/python3"
                            }
                        }
                    ]
                },
                {
                    "Classification": "livy-conf",
                    "Properties": {
                        "livy.spark.master": "yarn",
                        "livy.spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/usr/bin/python3",
                        "livy.cache-log.size": "2000",
                        "livy.impersonation.enabled": "true"
                    }
                },
                {
                    "Classification": "core-site",
                    "Properties": {
                        "hadoop.proxyuser.livy.groups": "*",
                        "hadoop.proxyuser.livy.hosts": "*"
                    }
                }
            ],
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Primary",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': master_instance_type,
                        'InstanceCount': 1,
                        'EbsConfiguration': {
                                'EbsBlockDeviceConfigs': [
                                    {
                                        'VolumeSpecification': {
                                            'VolumeType': 'gp3',
                                            'SizeInGB': SizeInGB
                                        },
                                    'VolumesPerInstance': VolumesPerInstance,
                                    }
                                ]
                        }
                    },
                    {
                        'Name': "Core",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': core_node_instance_type,
                        'InstanceCount': num_core_nodes,
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp3',
                                        'SizeInGB': SizeInGB
                                    },
                                    'VolumesPerInstance': VolumesPerInstance,
                                }
                            ]
                        }
                    },
                ],
                'KeepJobFlowAliveWhenNoSteps': True,  # False
                'TerminationProtected': False,
                'EmrManagedSlaveSecurityGroup': EmrManagedSlaveSecurityGroup,
                'EmrManagedMasterSecurityGroup': EmrManagedMasterSecurityGroup,
                'ServiceAccessSecurityGroup': ServiceAccessSecurityGroup,
                'Ec2SubnetId': Ec2SubnetId
            },
            BootstrapActions=[
                {
                    'Name': 'Getting Python Package',
                    'ScriptBootstrapAction': emr_conf['ScriptBootstrapAction'],

                },
            ],
            Steps=[
                {
                    'Name': 'Running the Python Package',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': emr_conf['HadoopJarStep']
                }
            ],
            EbsRootVolumeSize=64,
            VisibleToAllUsers=True,
            JobFlowRole=JobFlowRole,
            ServiceRole=ServiceRole,
            AutoScalingRole=AutoScalingRole,
            Tags=[
                {
                    'Key': 'AppCode',
                    'Value': AppCode,
                },
                {
                    'Key': 'Contact',
                    'Value': Contact,
                },
            ],
        )
        logger.info('Created a cluster with id :' + response['JobFlowId'])
        return response['JobFlowId']

    except Exception as e:
        logger.error(str(e))
        return 0, str(e)


def get_cluster_dns(cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']


def wait_for_cluster_creation(cluster_id):
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)


def terminate_cluster(cluster_id):
    emr.terminate_job_flows(JobFlowIds=[cluster_id])


def get_public_ip(cluster_id):
    instances = emr.list_instances(
        ClusterId=cluster_id, InstanceGroupTypes=['MASTER'])
    return instances['Instances'][0]['PublicIpAddress']


def _get_sessions(host):
    response = requests.get(host + '/batches')
    if response.status_code in acceptable_response_codes:
        return response.json()["sessions"]
    else:
        raise Exception("sessions didn't return " + str(acceptable_response_codes) + ". Returned '" + str(
            response.status_code) + "'.")


def _get_session(host, session_id):
    sessions = _get_sessions(host)
    for session in sessions:
        if session["id"] == session_id:
            return session


def submit_spark_script(master_dns, data):
    # 8998 is the port on which the Livy server runs
    host = 'http://' + master_dns + ':8998'
    headers = {'Content-Type': 'application/json'}
    response = requests.post(
        host + '/batches', data=json.dumps(data), headers=headers)
    logger.info(response)
    if response.status_code in acceptable_response_codes:
        response_json = response.json()
        session_id = response_json["id"]
        session_state = response_json["state"]
        logger.info(response_json)

        if session_state == "starting":
            logger.info("Session is starting....")

        session_state_waiting = 5
        while session_state == "starting":
            logger.info("Waiting " + str(session_state_waiting) + " seconds")
            time.sleep(session_state_waiting)
            session_state_response = _get_session(host, session_id=session_id)
            session_state = session_state_response["state"]
            logger.info("latest state as '" + session_state + "'")

        logger.info(session_state_response)
        logger.info("session_id: '" + str(session_id) + "'")
        state_session = get_batch_session_logs(master_dns, response_json["id"])

        return response.headers, response_json["id"]
    else:
        raise Exception("new session didn't return " + str(acceptable_response_codes) + ". Returned '" + str(
            response.status_code) + "'.")


def get_batch_session_logs(master_dns, batch_id):
    host = 'http://' + master_dns + ':8998'
    dashes_count = 50
    line_from = 0
    line_to = 2000
    session_state_waiting = 15
    logs_page = []
    while True:
        session_state_response = _get_session(host, batch_id)
        session_state = session_state_response["state"]

        endpoint = host + "/batches/" + \
            str(batch_id) + "/log" + f"?from={line_from}&size={line_to}"

        response = get_batch_page_logs(endpoint)

        logs = response.json()["log"]
        for log in logs:
            if ((log not in logs_page) & (' INFO ' not in log)):
                logs_page.append(log)

        if ((session_state != "running")):
            for log in logs_page:
                logger.info(log.replace("\\n", "\n"))
            logger.info('session_state :{}'.format(session_state))
            break
        time.sleep(session_state_waiting)

    logger.info(
        f"{'-' * dashes_count}End of full log for batch {batch_id}" f"{'-' * dashes_count}")
    logger.info(session_state)
    return session_state


def get_batch_page_logs(endpoint):
    headers = {'Content-Type': 'application/json'}
    response = requests.get(endpoint, headers=headers)
    return response


def get_connetion_audit(sm, key):
    # get Airflow conn
    conn = None
    error_msg = None
    try:
        conn_info = get_secret_manager(sm, key)

        host = conn_info['url'].split("/")[2].split(':')[0]
        username = conn_info['username']
        password = conn_info['password']
        database = conn_info['url'].split("/")[-1]
        port = conn_info['url'].split("/")[2].split(':')[1]
        # logger.info('Host: {}'.format(host))
        logger.info('database : {}'.format(database))
        conn = psycopg2.connect(host=host, user=username,
                                password=password, database=database, port=port)
    except Exception as e:
        error_msg = e
        logger.error(e)
    return conn, error_msg


def get_audit(audit_id, etl_process_detail_id, sm, key, process_steps=None):
    error_audit = True
    error_msg = None
    process_steps = 1 if process_steps is None else process_steps
    logger.info(process_steps)
    try:
        logger.info('Audit_Id: {}'.format(audit_id))
        logger.info('Process ID: {}'.format(etl_process_detail_id))
        conn, error_msg = get_connetion_audit(sm, key)
        if conn is not None:
            cur = conn.cursor()
            sql = "SELECT etl_process_status, target_name, step, etl_process_rows, etl_process_id, etl_process_run_date, audit_id, etl_process_detail_id FROM audit.css_audit where audit_id = '{}' and etl_process_detail_id = '{}'".format(
                audit_id, etl_process_detail_id)
            logger.info(sql)
            cur.execute(sql)
            r_audit = cur.fetchall()
            print(*r_audit, sep="\n")
            if len(r_audit) > 0:
                target_write = [
                    item for item in r_audit if item[2] == 'target-write']
                only_error = [item for item in r_audit if item[0] == 'error']
                if len(target_write) != process_steps:
                    logger.info('Error: {}'.format(target_write))
                    error_msg = 'ED124 - Audit is not matching with Process step: return: {}'.format(
                        str(target_write))
                elif len(only_error) > 0:
                    logger.info('Error: {}'.format(only_error))
                    error_msg = 'ED126 - Audit encountered error in one or more process step, return: {}'.format(
                        str(only_error))
                else:
                    error_audit = False
            else:
                error_msg = 'ED125 - Audit is must be greater than zero'
            cur.close()
            conn.close()
    except Exception as e:
        error_audit = True
        error_msg = e
        logger.error(e)
    return error_audit, error_msg


def insert_audit(audit_list, sm, key):
    error_audit = False
    error_msg = None
    audit_tuple = tuple(audit_list)
    logger.info(audit_tuple)
    try:
        logger.info('Audit_Id: {}'.format(audit_list))
        conn, error_msg = get_connetion_audit(sm, key)
        if conn is not None:
            cur = conn.cursor()
            sql = "INSERT INTO audit.css_audit(audit_id, etl_process_detail_id, etl_process_run_date,  etl_process_start_dt, etl_process_end_dt, etl_process_status, etl_process_rows, etl_process_id, target_name, step ) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            logger.info(sql)
            cur.executemany(sql, audit_tuple)
            conn.commit()
            cur.close()
            conn.close()
    except Exception as e:
        error_audit = True
        error_msg = e
        logger.error(e)
    return error_audit, error_msg


def get_s3_all_keys(bucket_name, prefix, expression=None):
    s3_client = boto3.client('s3', region_name=_region_name)
    keys = []
    logger.info(expression)
    kwargs = {'Bucket': bucket_name, 'Prefix': prefix, 'Delimiter': '/'}
    while True:
        result = s3_client.list_objects_v2(**kwargs)
        logger.info(result)
        if result['KeyCount'] > 0:
            if 'Contents' in result:
                for obj in result['Contents']:
                    if expression != None:
                        logger.info(obj['Key'])
                        x = re.search(
                            expression, obj['Key'].replace(prefix, ''))
                        if x:
                            keys.append(obj['Key'])
                    else:
                        keys.append(obj['Key'])
                print(keys)
                try:
                    kwargs['ContinuationToken'] = result['NextContinuationToken']
                except Exception as e:
                    # logger.error("Error: {}".format(e))
                    break
            else:
                break
    return keys


def s3_move_file(from_bucket_name, to_bucket_name, old_key, new_key):
    s3_resource = boto3.resource('s3', region_name=_region_name)
    s3_resource.Object(to_bucket_name, new_key).copy_from(
        CopySource=from_bucket_name + "/" + old_key)
    s3_resource.Object(from_bucket_name, old_key).delete()


def s3_delete_files(from_bucket_name, prefix):
    logger.info('from_bucket_name: {}'.format(from_bucket_name))
    logger.info('prefix: {}'.format(prefix))
    s3_resource = boto3.resource('s3', region_name=_region_name)
    bucket = s3_resource.Bucket(from_bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()


def read_s3_success_file(from_bucket_name, prefix, expression, regex_str):
    files_data = []
    logger.info('region: {}'.format(_region_name))
    keys = get_s3_all_keys(from_bucket_name, prefix, expression)
    logger.info(keys)
    s3_resource = boto3.resource('s3', region_name=_region_name)
    bucket = s3_resource.Bucket(from_bucket_name)

    for item in keys:
        file_path = '{}{}'.format(prefix, item.split("/")[-1])
        logger.info(file_path)
        obj = bucket.Object(key=file_path)
        match = re.search(regex_str, file_path)
        file_date = match.group(1)

        # get the object and read the contents of the file
        response = obj.get()['Body'].read()

        files_data.append({
            "file_date": file_date,
            "number_rows": int(response)
        })
    return files_data


def get_config(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)

    conf_file = json.loads(obj['Body'].read())

    logger.info(conf_file)
    return conf_file


def get_secret_manager(connection_key, target=None):
    try:
        sm_client = boto3.client('secretsmanager', region_name=_region_name)
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=connection_key)
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            print(e)
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            print(e)
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            print(e)
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            print(e)
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            print(e)
            raise e

    secretstring = get_secret_value_response['SecretString'].replace('\n', '')

    secret = json.loads(secretstring)
    connection = secret[target]

    return connection


def put_error_cloudwatch(log_group, error_msg):
    client = boto3.client('logs')

    logGroupName = log_group
    logStreamName = f"{log_group}-" + \
        datetime.utcnow().__str__().replace(":", "")

    log_stream = client.create_log_stream(
        logGroupName=logGroupName,
        logStreamName=logStreamName
    )

    log_event = client.put_log_events(
        logGroupName=logGroupName,
        logStreamName=logStreamName,
        logEvents=[
            {
                'timestamp': int(round(time.time() * 1000)),
                'message': error_msg
            }
        ]
    )
