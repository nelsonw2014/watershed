import os
import subprocess
import requests
import json
from signal import SIGINT
from time import sleep
from nose.tools import assert_equal, assert_in

def end_to_end_test():
    """Launch and query cluster"""

    conf_file = os.environ.get('WATERSHED_CONFIG')
    assert os.path.isfile(conf_file)

    private_key_file = os.environ.get('PRIVATE_KEY_FILE')
    assert os.path.isfile(private_key_file)

    assert_equal(0, subprocess.check_call(
        [
            "python3",
            "-m",
            "watershed",
            "u",
            "-c",
            conf_file,
            "-f"
        ]
    ))

    print("Launching cluster.")
    launch_output = subprocess.check_output(
        [
            "python3",
            "-m",
            "watershed",
            "l",
            "-c",
            conf_file,
            "-w"
        ]
    ).decode()

    beg_index = launch_output.rfind('j-')
    end_index = launch_output.rfind('\'')

    cluster_id = launch_output[beg_index:end_index]

    forwarding_process = None
    try:
        print("Forwarding ports.")
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
            preexec_fn=os.setsid
        )
        sleep(1)

        r = None
        for retry in range(0, 60):
            try:
                r = requests.post(
                    'http://localhost:8047/query.json',
                    json.dumps({
                        "queryType": "SQL",
                        "query": "SELECT COUNT(employee_id) cnt FROM cp.`employee.json`"
                    }),
                    headers={
                        "Content-Type": "application/json"
                    }
                )
            except Exception:
                pass

            if r is not None and hasattr(r, "status_code"):
                assert_equal(r.status_code, 200)
                query = json.loads(r.text)
                assert_equal(len(query["rows"]), 1)
                assert_in("cnt", query["rows"][0])
                assert_equal(query["rows"][0]["cnt"] == "1155")
                break
            sleep(1)

    finally:
        if forwarding_process is not None:
            forwarding_process.send_signal(SIGINT)

        subprocess.call(
            [
                "python3",
                "-m",
                "watershed",
                "t",
                "-i",
                cluster_id,
            ]
        )
