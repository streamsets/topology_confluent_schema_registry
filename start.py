# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging

from clusterdock.models import Cluster, Node
from clusterdock.utils import wait_for_condition

DEFAULT_NAMESPACE = 'clusterdock'
REST_PORT = 8081

logger = logging.getLogger('clusterdock.{}'.format(__name__))

def success(time):
    logger.info('Conditions satisfied after %s seconds.', time)


def failure(timeout):
    raise TimeoutError('Timed out after {} seconds waiting.'.format(timeout))


# Validate that Zookeeper is up and running by connecting using shell
def validate_zookeeper(node, quiet):
    return node.execute('/confluent/bin/zookeeper-shell localhost:2181 ls /', quiet=quiet).exit_code == 0


# Validate that Kafka is up by checking that all brokers are registered in zookeeper
def validate_kafka(node, broker_count, quiet):
    command = node.execute('/confluent/bin/zookeeper-shell localhost:2181 <<< "ls /brokers/ids" | tail -n 1', quiet=quiet)

    if command.exit_code != 0:
        return False

    nodes = command.output
    if not nodes.startswith('['):
        return False

    return len(json.loads(nodes)) == broker_count


def main(args):
    quiet = not args.verbose

    # Image name
    image = '{}/{}/topology_confluent_schema_registry:schema_registry-{}'.format(args.registry,
                                                                                 args.namespace or DEFAULT_NAMESPACE,
                                                                                 args.confluent_version)

    # Nodes in the Kafka cluster
    nodes = [Node(hostname=hostname,
                  group='brokers',
                  ports=[{REST_PORT : REST_PORT}],
                  image=image)
             for hostname in args.nodes]

    cluster = Cluster(*nodes)
    cluster.start(args.network, pull_images=args.always_pull)

    # Create distributed zookeeper configuration
    zookeeper_config = ['tickTime=2000',
                        'dataDir=/zookeeper',
                        'clientPort=2181',
                        'initLimit=5',
                        'syncLimit=2']

    for idx, node in enumerate(cluster):
        zookeeper_config.append('server.{}={}:2888:3888'.format(idx, node.hostname))

    # Start all zookeepers
    for idx, node in enumerate(cluster):
        logger.info('Starting Zookeeper on node {}'.format(node.hostname))
        node.execute('mkdir -p /zookeeper')
        node.put_file('/zookeeper/myid', str(idx))
        node.put_file('/zookeeper.properties', '\n'.join(zookeeper_config))
        node.execute('/start_zookeeper &', detach=True)

    # Validate that Zookeepr is alive from each node
    for node in cluster:
        logger.info('Validating Zookeeper on node %s', node.hostname)
        wait_for_condition(condition=validate_zookeeper,
                           condition_args=[node, quiet],
                           time_between_checks=3,
                           timeout=60,
                           success=success,
                           failure=failure)

    # Start all brokers
    for idx, node in enumerate(cluster):
        logger.info('Starting Kafka on node {}'.format(node.hostname))

        kafka_config = node.get_file('/confluent/etc/kafka/server.properties')
        kafka_config = kafka_config.replace('broker.id=0', 'broker.id={}'.format(idx))
        node.put_file('/kafka.properties', kafka_config)

        node.execute('/start_kafka &', detach=True)

    # Verify that all Kafka brokers up
    logger.info('Waiting on all brokers to register in zookeeper')
    wait_for_condition(condition=validate_kafka,
                       condition_args=[nodes[0], len(nodes), quiet],
                       time_between_checks=3,
                       timeout=60,
                       success=success,
                       failure=failure)

    # Start schema registry on all nodes
    for idx, node in enumerate(cluster):
        logger.info('Starting Schema Registry on node {}'.format(node.hostname))
        node.execute('/start_schema_registry &', detach=True)
