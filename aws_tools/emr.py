# Copyright (C) 2015 Commerce Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import boto3


def get_master_address(cluster_id, profile='default'):
    emr_client = boto3.session.Session(profile_name=profile).client('emr')

    if cluster_id is None:
        raise ValueError("Cluster ID cannot be none.")
    try:
        json_description = emr_client.describe_cluster(ClusterId=cluster_id)
        print(json_description)
        return json_description['Cluster']['MasterPublicDnsName']
    except Exception as aws_except:
        raise ValueError(aws_except)


def launch_emr_cluster(s3_config=None, emr_config=None, profile="default", wait_until_ready=False):
    emr_client = boto3.session.Session(profile_name=profile).client('emr')

    s3_url = "s3://"+s3_config['resourcesBucket']+"/"+s3_config['resourcesPrefix']

    bootstrap_actions = [
        {
            'Name': 'Install Impala',
            'ScriptBootstrapAction': {
                'Path': 's3://us-east-1.elasticmapreduce/libs/impala/setup-impala',
                'Args': [
                    "--base-path",
                    "s3://us-east-1.elasticmapreduce",
                    "--impala-version",
                    "latest"
                ]
            }
        },
        {
            'Name': 'configure-hadoop',
            'ScriptBootstrapAction': {
                'Path': 's3://elasticmapreduce/bootstrap-actions/configure-hadoop',
                'Args': [
                    "--hdfs-key-value",
                    "dfs.replication=3"
                ]
            }
        },
        {
            'Name': 'early_hdfs',
            'ScriptBootstrapAction': {
                'Path': s3_url+'/emr/exec/early_hdfs',
                'Args': []
            }
        },
        {
            'Name': 'setup_drill',
            'ScriptBootstrapAction': {
                'Path': s3_url+'/emr/exec/setup_drill',
                'Args': []
            }
        },
        {
            'Name': 'setup_drill_lib_aws',
            'ScriptBootstrapAction': {
                'Path': s3_url+'/emr/exec/setup_drill_lib_aws',
                'Args': []
            }
        },
        {
            'Name': 'configure_drill_storage_dfs',
            'ScriptBootstrapAction': {
                'Path': s3_url+'/emr/exec/configure_drill_storage_dfs',
                'Args': []
            }
        },
        {
            'Name': 'configure_drill_storage_streams',
            'ScriptBootstrapAction': {
                'Path': s3_url+'/emr/exec/configure_drill_storage_streams',
                'Args': []
            }
        },
        {
            'Name': 'configure_drill_storage_stream_archives',
            'ScriptBootstrapAction': {
                'Path': s3_url+'/emr/exec/configure_drill_storage_stream_archives',
                'Args': [
                    emr_config['archivalDfsUrl'],
                    emr_config['archivesPath']
                ]
            }
        },
        {
            'Name': 'stop_early_hdfs',
            'ScriptBootstrapAction': {
                'Path': s3_url+'/emr/exec/stop_early_hdfs',
                'Args': []
            }
        },
    ]

    steps = [
        {
            'Name': 'hive_create_table_for_stream',
            'HadoopJarStep': {
                'Jar': "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                'Args': [
                    "s3://us-east-1.elasticmapreduce/libs/hive/hive-script",
                    "--run-hive-script",
                    "--hive-versions",
                    "latest",
                    "--args",
                    '-f',
                    s3_url+'/emr/hive/create_table_for_stream',
                    '-hiveconf',
                    'tablename='+emr_config['hive']['tableName'],
                    '-hiveconf',
                    'streamname='+emr_config['kinesis']['streamName']
                ]
            },
            'ActionOnFailure': 'CANCEL_AND_WAIT'
        },
        {
            'Name': 'Install Hive',
            'HadoopJarStep': {
                'Jar': "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                'Args': [
                    "s3://us-east-1.elasticmapreduce/libs/hive/hive-script",
                    "--install-hive",
                    "--base-path",
                    "s3://us-east-1.elasticmapreduce/libs/hive",
                    "--hive-versions",
                    "latest"
                ]
            },
            'ActionOnFailure': 'TERMINATE_CLUSTER'

        }
    ]

    instances = {
        'KeepJobFlowAliveWhenNoSteps': True,
        'InstanceGroups': emr_config['instanceGroups'],
        'Ec2KeyName': emr_config['ec2KeyName'] if emr_config['ec2KeyName'] is not None else "",
        'Ec2SubnetId': emr_config['ec2SubnetId'] if emr_config['ec2SubnetId'] is not None else "",
        'AdditionalMasterSecurityGroups': emr_config['additionalMasterSecurityGroups'] if emr_config['additionalMasterSecurityGroups'] is not None else []
    }
    try:
        return_json = emr_client.run_job_flow(
            Name=emr_config['clusterName'],
            AmiVersion=emr_config['amiVersion'],
            Instances=instances,
            VisibleToAllUsers=True,
            Steps=steps,
            BootstrapActions=bootstrap_actions,
            ServiceRole=emr_config['roles']['service'],
            JobFlowRole=emr_config['roles']['ec2']
        )
        print(return_json)
        if wait_until_ready:
            print("Waiting option selected. Cluster can take more than 5 minutes to start...")
            cluster_id = return_json['JobFlowId']
            cluster_start_waiter = emr_client.get_waiter('cluster_running')
            cluster_start_waiter.wait(ClusterId=cluster_id)
            print("Cluster '{0}' ready.".format(cluster_id))

    except Exception as aws_except:
        raise ValueError(aws_except)


def terminate_emr_cluster(cluster_ids=None, profile='default'):
    emr_client = boto3.session.Session(profile_name=profile).client('emr')

    try:
        emr_client.terminate_job_flows(JobFlowIds=cluster_ids)
        print("Attempted to terminate the following cluster Ids:")
        for cluster_id in cluster_ids:
            print("  * '"+cluster_id+"'")
    except Exception as aws_except:
        raise ValueError(aws_except)
