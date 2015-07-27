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

from sshtunnel import SSHTunnelForwarder
from aws_tools.emr import get_master_address
from time import sleep


def forward_necessary_ports(cluster_id=None, private_key_path=None, profile='default'):
    master_public_address = str(get_master_address(cluster_id, profile))

    ports = {
        'drill_ui_port': 8047,
        'hue_port': 8888,
        'resmgr_port': 9026,
        'namenode_port': 9101,
        'zookeeper_port': 2181,
        'drill_user_port': 31010,
        'hive_port': 10000
    }

    port_forwarders = []

    for port in ports.keys():
        port_number = ports[port]
        port_forwarders.append(
            SSHTunnelForwarder(
                master_public_address,
                22,
                ssh_username='hadoop',
                ssh_private_key=private_key_path,
                local_bind_address=('localhost', port_number),
                remote_bind_address=(master_public_address, port_number)
            )
        )

    try:
        for forwarder in port_forwarders:
            forwarder.start()
        print("\n\nForwarding necessary ports. Ctrl-C will terminate port forwarding.\n")
        print("Ports forwarded:\n==============================\n")
        for port in ports.keys():
            print("  * "+(port.split('_port')[0]+":").ljust(20)+" 127.0.0.1:{0}".format(ports[port]))
        print("\n")
        while True:
            sleep(1)
    except KeyboardInterrupt:
        for forwarder in port_forwarders:
            forwarder.stop()
        exit()
