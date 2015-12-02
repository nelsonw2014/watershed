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

import argparse
import requests
import json
import os

_pump_port = 8080
_base_url = 'http://localhost:{}/pump'.format(_pump_port)

def get_argument_parser():
    _fc_description = "Client to manage and monitor Pump."

    _query_args = ["-q", "--query"]
    _query_kwargs = dict(action="store", help="Specify the query Pump will use to pull records out of Drill.", required=True, type=str)
    
    _stream_args = ["-s", "--stream"]
    _stream_kwargs = dict(action="store", help="Specify the Kinesis stream that Pump will emit records to.", required=True, type=str)
    
    _num_records_args = ["-n", "--num-records"]
    _num_records_kwargs = dict(action="store", help="Specify the number of records to return in the preview.", required=True, type=int)
    
    _job_id_args = ["-id", "--job-id"]
    _job_id_kwargs = dict(action="store", help="Specify the Job ID to retrieve.", required=True, type=str)
    
    _show_progress_args = ["-p", "--show-progress"]
    _show_progress_kwargs = dict(action="store_const", const=True, required=False, help="Poll Pump for progress of job")
    
    _summary_only_args = ["-t", "--summary-only"]
    _summary_only_kwargs = dict(action="store_const", const=True, required=False, help="Only show summary information about the Job")

    parser = argparse.ArgumentParser(
        prog=os.path.dirname(__file__),
        description=_fc_description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers()
    
    create_job_parser = subparsers.add_parser('create-job', help="Enqueue a job for Pump to work on.")
    create_job_parser.set_defaults(which="create-job")
    create_job_parser.add_argument(*_query_args, **_query_kwargs)
    create_job_parser.add_argument(*_stream_args, **_stream_kwargs)
    create_job_parser.add_argument(*_show_progress_args, **_show_progress_kwargs)
    
    preview_job_parser = subparsers.add_parser('preview-job', help="Preview a job before Pump runs it.")
    preview_job_parser.set_defaults(which="preview-job")
    preview_job_parser.add_argument(*_query_args, **_query_kwargs)
    preview_job_parser.add_argument(*_num_records_args, **_num_records_kwargs)
    
    get_job_parser = subparsers.add_parser('get-job', help="Retreive Job status from Pump.")
    get_job_parser.set_defaults(which="get-job")
    get_job_parser.add_argument(*_job_id_args, **_job_id_kwargs)
    get_job_parser.add_argument(*_show_progress_args, **_show_progress_kwargs)
    get_job_parser.add_argument(*_summary_only_args, **_summary_only_kwargs)
    
    get_all_jobs_parser = subparsers.add_parser('get-all-jobs', help="Retreive all Jobs and their statuses from Pump.")
    get_all_jobs_parser.set_defaults(which="get-all-jobs")    
    get_all_jobs_parser.add_argument(*_summary_only_args, **_summary_only_kwargs)

    return parser
        
def get_all_jobs(summary_only):
    print('Showing all jobs...')

    url = _base_url + '/jobs'
    jobs = []
    try:
        r = requests.get(url)
        jobs = json.loads(r.text)
    except requests.exceptions.RequestException as e: 
        print(e)
        
    for job in jobs:
        print_job(job, summary_only)
    return


def get_job(job_id, poll_for_progress, summary_only):        
    print('Showing job...')
    print('Job: {}'.format(job_id))
    
    url = _base_url + '/jobs/' + job_id
        
    while True:
        job = dict()
        
        try:
            r = requests.get(url)
            print(r)
            job = json.loads(r.text)
        except requests.exceptions.RequestException as e: 
            print(e)

        if("jobId" not in job):
            print("Invalid job!")
            print(job)
            return
        
        print_job(job, summary_only)
        
        if(not poll_for_progress or job["stage"] == "COMPLETED_ERROR" or job["stage"] == "COMPLETED_SUCCESS"):
            break
            
        sleep(1)
    
    return


def print_job(job, summary_only):
    if(summary_only):        
        print('{jobId}: {stage}({elapsedTime}, {meanRate}) {successCount} successful, {failureCount} failed, {pendingCount} pending'.format(
                jobId=job["jobId"],
                stage=job["stage"],
                elapsedTime=job["elapsedTimePretty"],
                meanRate=job["meanRatePretty"],
                successCount=job["successfulRecordCount"],
                failureCount=job["failureRecordCount"],
                pendingCount=job["pendingRecordCount"]
            ))
    else:
        print(json.dumps(job, sort_keys=True, indent=4, separators=(',', ': ')))
    

def create_job(query_in, stream_out, poll_for_progress):    
    print('Creating job...')
    print('Query: {}'.format(query_in))
    print('Stream: {}'.format(stream_out))
    print('Polling?: {}'.format(poll_for_progress))
    
    url = _base_url + '/jobs'
    payload = { 'queryIn': query_in, 'streamOut': stream_out }
    
    job = dict()
    try:
        r = requests.post(url, data=json.dumps(payload), headers={'content-type': 'application/json'})
        job = json.loads(r.text)
    except requests.exceptions.RequestException as e: 
        print(e)
        
    if(not poll_for_progress):
        print(job)
        return
        
    if(not 'jobId' in job):
        return

    get_job(job.jobId, poll_for_progress, True)
    return

def preview_job(query_in, num_records):
    print('Previewing job...')
    print('Query: {}'.format(query_in))
    print('Num records: {}'.format(num_records))
    
    url = _base_url + '/jobs/preview'
    jobPreview = dict()
    try:
        payload = { 'queryIn': query_in, 'previewCount': num_records }
        r = requests.post(url, data=json.dumps(payload), headers={'content-type': 'application/json'})
        print(r)
        jobPreview = json.loads(r.text)
    except requests.exceptions.RequestException as e: 
        print(e)
    
    return


def main():
    parser = get_argument_parser()
    args = parser.parse_args()
    if(not vars(args)):
        parser.print_help()
        parser.exit(1)
    
    config = dict()
    if hasattr(args, 'which'):
        if args.which == "create-job":
            create_job(args.query, args.stream, args.show_progress)
              
        elif args.which == "get-job":
            get_job(args.job_id, args.show_progress, args.summary_only)
        
        elif args.which == "get-all-jobs":
            get_all_jobs(args.summary_only)
        
        elif args.which == "preview-job":
            preview_job(args.query, args.num_records)
        
        else:
            parser.print_help()
            exit(2)
            
    else:
        parser.print_help()
        exit(2)
        
main()