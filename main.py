#!/usr/bin/env python

from __future__ import with_statement

import argparse
import csv
import getpass
import os
import sys

from glob import glob
from subprocess import check_output


def which(app):
    return check_output(['which', app]).strip()


def find_cassandra_driver():
    def find_zip(paths, prefix):
        for path in paths:
            zips = glob(os.path.join(path, prefix + '*.zip'))
            if zips:
                return max(zips)
        return None

    cqlsh_path = which('cqlsh')
    cassandra_path = os.path.join(os.path.dirname(cqlsh_path), '..')
    cassandra_lib_path = [os.path.join(cassandra_path, 'lib')]

    cql_lib_prefix = 'cassandra-driver-internal-only-'
    third_parties_prefixes = ('futures-', 'six-')

    cql_zip = find_zip(cassandra_lib_path, cql_lib_prefix)
    if not cql_zip:
        raise Exception('{} not found'.format(cql_lib_prefix))

    cql_ver = os.path.splitext(os.path.basename(cql_zip))[0][len(cql_lib_prefix):]
    sys.path.insert(0, os.path.join(cql_zip, 'cassandra-driver-' + cql_ver))

    for prefix in third_parties_prefixes:
        lib_zip = find_zip(cassandra_lib_path, prefix)
        if not lib_zip:
            raise Exception('{} not found'.format(lib_zip))
        sys.path.insert(0, lib_zip)


find_cassandra_driver()

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=9042)
    parser.add_argument('--keyspace', type=str, default='system')
    parser.add_argument('--username', type=str, default='cassandra')
    parser.add_argument('--password', type=str, default='')
    parser.add_argument('--timeout', type=int, default=30)
    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)

    auth_provider = PlainTextAuthProvider(
        username=args.username,
        password=args.password,
        )

    cluster = Cluster(
        [args.host],
        auth_provider=auth_provider,
        port=args.port,
        connect_timeout=args.timeout,
        )

    session = cluster.connect()
    session.set_keyspace(args.keyspace)
    session.default_timeout = args.timeout

    cluster.shutdown()
    return 0


if '__main__' == __name__:
    sys.exit(main(sys.argv[1:]))
