#!/usr/bin/env python

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

import json
import sys
from urllib2 import urlopen, Request
from simples3.bucket import S3Bucket


if __name__ == "__main__":
    with open('/mnt/var/lib/info/instance.json', 'r') as f:
        if not json.load(f)["isMaster"]:
            exit()
    s = S3Bucket(
        sys.argv[1],
        sys.argv[2],
        sys.argv[3]
    )

    storage_stream_archive_config_files = sys.argv[4:]

    for config_file_path in storage_stream_archive_config_files:

        config_file = s.get(config_file_path)
        json_name = config_file_path.split('/')[len(config_file_path.split('/'))-1]
        drill_upload_request = Request(
            'http://localhost:8047/storage/'+json_name,
            config_file.read(),
            {
                "Content-Type": "application/json"
            }
        )
        urlopen(drill_upload_request)
        s.delete(config_file_path)

