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
import json
from aws_tools.s3 import upload_resources
from aws_tools.emr import launch_emr_cluster, terminate_emr_cluster
from ssh_tools.ssh import forward_necessary_ports


def parse_arguments():
    _fc_description = """
Python/Boto solution which compliments kinesis with:
  - Long-term archival of records
  - Making current and archived records accesible to SQL-based exploration and analysis
  - Replay of archived records:
    + supports key-value compaction
    + supports bounded replay
    + supports filtered replay
    + supports record annotation"""

    _config_file_args = [
        "-c",
        "--config-file"
    ]
    _config_file_kwargs = dict(
        action="store",
        help="Specify the configuration json file",
        required=True,
        type=argparse.FileType('r')
    )
    _cluster_id_args = [
        "-i",
        "--cluster-id"
    ]
    _cluster_id_kwargs = dict(
        action="store",
        help="Specify the cluster id for the operation to be preformed on",
        required=True,
        type=str
    )
    _private_key_args = [
        "-k",
        "--private-key"
    ]
    _private_key_kwargs = dict(
        action="store",
        help="Specify the location of the private key file",
        required=True
    )
    _profile_args = [
        "-p",
        "--profile"
    ]
    _profile_kwargs = dict(
        action="store",
        help="Specify the AWS profile to use. Defaults to 'default'",
        default='default'
    )

    parser = argparse.ArgumentParser(
        prog="Watershed",
        description=_fc_description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers()
    upload_resources_parser = subparsers.add_parser(
        'upload-resources',
        help="Upload resources for a cluster."
    )
    upload_resources_parser.set_defaults(which="upload-resources")
    upload_resources_parser.add_argument(*_config_file_args, **_config_file_kwargs)
    upload_resources_parser.add_argument(
        '-F',
        '--force-upload',
        action="store_const",
        const=True,
        help="Force upload to occur"
    )
    launch_cluster_parser = subparsers.add_parser(
        'launch-cluster',
        help="Launch a cluster."
    )
    launch_cluster_parser.set_defaults(which="launch-cluster")
    launch_cluster_parser.add_argument(*_config_file_args, **_config_file_kwargs)
    launch_cluster_parser.add_argument(
        '-w',
        '--wait-until-ready',
        action="store_const",
        const=True,
        help="Wait until cluster launches (takes up to 10 minutes at most)"
    )
    forward_local_ports_parser = subparsers.add_parser(
        'forward-local-ports',
        help="Forward the local ports for ssh access."
    )
    forward_local_ports_parser.set_defaults(which="forward-local-ports")
    forward_local_ports_parser.add_argument(*_cluster_id_args, **_cluster_id_kwargs)
    forward_local_ports_parser.add_argument(*_private_key_args, **_private_key_kwargs)
    forward_local_ports_parser.add_argument(*_profile_args, **_profile_kwargs)
    terminate_clusters_parser = subparsers.add_parser(
        'terminate-clusters',
        help="Terminate the cluster(s) that are associated with the provided luster ID(s)"
    )
    terminate_clusters_parser.set_defaults(which="terminate-clusters")
    terminate_clusters_parser.add_argument(
        '-i',
        '--cluster-ids',
        metavar='CLUSTER_ID',
        type=str,
        nargs='+',
        help='Clusters to terminate.',
        required=True
    )
    terminate_clusters_parser.add_argument(*_profile_args, **_profile_kwargs)

    return parser.parse_args()


def load_configuration(configuration_file):
    return json.load(configuration_file)

if __name__ == "__main__":
    args = parse_arguments()
    config = dict()
    if hasattr(args, 'which'):
        if hasattr(args.config_file):
            config = load_configuration(args.config_file)

        if args.which == "upload-resources":
            upload_resources(
                config['AWS']['S3'],
                config['AWS']['profile'],
                args.force_upload
            )
        elif args.which == "launch-cluster":
            launch_emr_cluster(
                config['AWS']['S3'],
                config['AWS']['EMR'],
                config['AWS']['profile'],
                args.wait_until_ready
            )
        elif args.which == "forward-local-ports":
            forward_necessary_ports(
                args.cluster_id,
                args.private_key,
                args.profile
            )
        elif args.which == "terminate-clusters":
            terminate_emr_cluster(
                args.cluster_ids,
                args.profile
            )
    else:
        raise ValueError("No command specified.")