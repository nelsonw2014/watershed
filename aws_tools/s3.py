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
import os


def upload_resources(s3_config=None, profile="default", force_upload=False):
    s3_resource = boto3.session.Session(profile_name=profile).resource('s3')

    try:
        file_upload_count = 0
        if s3_resource.Bucket(s3_config['resourcesBucket']) in s3_resource.buckets.all() and not force_upload:
            raise ValueError("Bucket already exists, run with -F to force upload.")
        s3_resource.create_bucket(Bucket=s3_config['resourcesBucket'])
        for folder in os.walk('resources/s3'):
            # for all directories within the local_resources_directory
            if folder[2]:
                # if there are files in the directory
                for file in folder[2]:
                    # for every file in the directory
                    s3_file_path = s3_config['resourcesDirectory']+folder[0].split('resources/s3')[1]+"/"+file
                    s3_resource.Object(s3_config['resourcesBucket'], s3_file_path).put(Body=open(folder[0]+'/'+file, 'rb'))
                    file_upload_count += 1
        print("Upload Successful, {0} files uploaded.".format(file_upload_count))
    except Exception as aws_except:
        raise ValueError(aws_except)
