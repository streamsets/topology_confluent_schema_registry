==================================================
Confluent Schema Registry topology for clusterdock
==================================================

This repository houses the **Confluent Schema Registry** topology for `clusterdock`_.

.. _clusterdock: https://github.com/clusterdock/clusterdock

Usage
=====

Assuming you've already installed **clusterdock** (if not, go `read the docs`_),
you use this topology by cloning it to a local folder and then running commands
with the ``clusterdock`` script:

.. _read the docs: http://clusterdock.readthedocs.io/en/latest/

.. code-block:: console

    $ git clone https://github.com/clusterdock/topology_confluent_schema_registry
    $ clusterdock start topology_confluent_schema_registry --confluent-version 4.0.0 --confluent-short-version 4.0

To see full usage instructions for the ``start`` action, use ``-h``/``--help``:                                                 

.. code-block:: console

    $ clusterdock start topology_apache_kafka -h
    usage: clusterdock start [--always-pull] [--namespace ns] [--network nw]
                             [-o sys] [-r url] [-h] [--confluent-version ver]
                             [--nodes node [node ...]]
                             topology

    Start a Confluent Schema Registry cluster

    positional arguments:
      topology              A clusterdock topology directory

    optional arguments:
      --always-pull         Pull latest images, even if they're available locally
                            (default: False)
      --namespace ns        Namespace to use when looking for images (default:
                            None)
      --network nw          Container network to use (default: cluster)
      -o sys, --operating-system sys
                            Operating system to use for cluster nodes (default:
                            None)
      -r url, --registry url
                            Image registry from which to pull images (default:
                            docker.io)
      -h, --help            show this help message and exit

    Confluent Schema Registry arguments:
      --confluent-version ver
                            Confluent version (default: 4.0.0)

    Node groups:
      --nodes node [node ...]
                            Nodes of the nodes group (default: ['registry-1'])

