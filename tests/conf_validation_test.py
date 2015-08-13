import os
import subprocess
import requests
import json
import boto3
from nose import with_setup
from nose.tools import assert_equal, assert_in
from signal import SIGINT
from time import sleep


def run_drill_query(query):
    for retry in range(0, 60):
        try:
            r = requests.post(
                'http://localhost:8047/query.json',
                query,
                headers={
                    "Content-Type": "application/json"
                }
            )
        except Exception:
            pass

        if r is not None and hasattr(r, "status_code"):
            assert_equal(r.status_code, 200, "from query: \"{0}\"".format(json.loads(query)['query']))
            return json.loads(r.text)
        sleep(1)


def setup_forwarding():
    global conf
    global private_key_file
    global cluster_id
    global forwarding_process

    conf_file = os.environ.get('WATERSHED_CONFIG')
    assert os.path.isfile(conf_file)
    conf = json.load(open(conf_file))

    private_key_file = os.environ.get('PRIVATE_KEY_FILE')
    assert os.path.isfile(private_key_file)

    cluster_id = os.environ.get('CLUSTER_ID')
    print('Using profile \'' + conf['AWS']['profile'] + '\' to query cluster')
    emr_client = boto3.session.Session(profile_name=conf['AWS']['profile']).client('emr')
    cluster_description = emr_client.describe_cluster(ClusterId=cluster_id)
    assert_in("Cluster", cluster_description)
    assert_equal(cluster_description["Cluster"]["Status"]["State"], "WAITING")

    forwarding_process = None

    forwarding_process = subprocess.Popen(
        [
            "python3",
            "-m",
            "watershed",
            "f",
            "-i",
            cluster_id,
            "-k",
            private_key_file
        ],
        preexec_fn=os.setsid,
        stdout=open(os.devnull, 'wb')
    )
    sleep(1)


def clean_up_forwarding():
    if forwarding_process is not None:
        forwarding_process.send_signal(SIGINT)


@with_setup(setup_forwarding, clean_up_forwarding)
def validate_tables_test():
    queries = []

    for stream in conf['AWS']['streams']:
        queries.append(json.dumps({
            "queryType": "SQL",
            "query": "SELECT 1=1 FROM streams.`default`.`{0}` LIMIT 1".format(stream['table'].lower())
        }))

    r = None
    for query in queries:
        run_drill_query(query)


@with_setup(setup_forwarding, clean_up_forwarding)
def validate_archives_test():
    configured_archives = set()
    for archive in conf['AWS']['archives']:
        configured_archives.add(archive['dfsUrl'].split('@')[1] + '.' + archive['name'])

    query = json.dumps({
        "queryType": "SQL",
        "query": "SELECT SCHEMA_NAME, TYPE FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE TYPE='file'"
    })

    query_response = run_drill_query(query)

    for file_schema in query_response['rows']:
        if file_schema['SCHEMA_NAME'] in configured_archives:
            configured_archives.remove(file_schema['SCHEMA_NAME'])

    assert_equal(len(configured_archives), 0)
