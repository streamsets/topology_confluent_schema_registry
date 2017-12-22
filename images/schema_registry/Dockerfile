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
ARG CENTOS_VERSION=6.6
FROM clusterdock/topology_nodebase:centos${CENTOS_VERSION}

# Build arguments.
ARG CONFLUENT_VERSION=4.0.0
ARG CONFLUENT_VERSION_SHORT=4.0
ARG SCALA_VERSION=2.11

# We need Java version to run Kafka & Zookeeper
RUN yum install java -y

# Install Confluent Open Source Distribution to /confluent
RUN wget http://packages.confluent.io/archive/${CONFLUENT_VERSION_SHORT}/confluent-oss-${CONFLUENT_VERSION}-${SCALA_VERSION}.tar.gz -O /root/dist.tar.gz \
    && tar -xvf /root/dist.tar.gz -C /root/ \
    && rm -rf /root/dist.tar.gz \
    && mv /root/confluent-* /confluent

# And we provide custom start up scripts
ADD start_kafka /
ADD start_zookeeper /
ADD start_schema_registry /

# Create structures required by our automation
RUN mkdir /logs/
