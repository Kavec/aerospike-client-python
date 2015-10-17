#!/usr/bin/env python2
# -*- coding: utf-8 -*-
################################################################################
# Copyright ©2015 Aerospike, Inc.
#
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
################################################################################
"""
Delete an entire set from an aerospike namespace.
"""
from __future__ import (
    division,
    print_function,
    absolute_import,
    unicode_literals)

import os
import sys
import argparse
import aerospike

from traceback import print_tb

def parse_args():
    """Sets up and parses command line flags using argparse."""
    class HelpOnError(argparse.ArgumentParser):
        def error(self, message):
            """Print help text on parse error."""
            sys.stderr.write('error: {}\n\n'.format(message))
            self.print_help()
            sys.exit(64) # EX_USAGE in sysexits.h

    prog = HelpOnError(
        add_help=False,
        description='   Delete an entire set from an aerospike namespace.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    add_arg = prog.add_argument
    add_arg('-u', '--usage', action='help', help='Show usage information and exit')
    add_arg(
        '-h', '--host',  help='Server host', 
        metavar='HOST',  type=str, default='localhost')
    add_arg(
        '-p', '--port',  help='Server port', 
        metavar='HOST',  type=int, default=3200)
    add_arg(
        '-U', '--user',  help='Server username', 
        metavar='USER',  type=str, default=None)
    add_arg(
        '-P', '--pass',  help='Server password', dest='passwd',
        metavar='PASS',  type=str, default=None)
    add_arg(
        '-R', '--remote', action='store_true', default=False,
        help='Deletes via an info command to cluster; requires sys-admin role if security is enabled')

    add_arg('namespace', type=str, help="Namespace to delete from")
    add_arg('set',       type=str, help="Set to delete")

    return prog.parse_args()

class spike_client(object):
    """Defines the aerospike client class for use with 'with'."""
    def __init__(self, g_args):
        self._args = g_args
    def __enter__(self):
        """
        Set up the client and connect to the cluster.

        self._args = {
            'host': str, # Server hostname
            'port': int, # Server port
            'user': str, # Username or None
            'pass': str  # password or None
        }
        """
        lua_sys_path = os.path.dirname(aerospike.__file__) \
            + "/../../../aerospike/lua"

        config = {
            'hosts': [(self._args.host, self._args.port)],
            'lua':   { 'user_path':   '.',
                       'system_path': lua_sys_path }}

        self._client = aerospike.client(config)

        try:
            self._client.connect(self._args.user, self._args.passwd)
        except:
            self.__exit__(*sys.exc_info(), connected=False)
        return self
    def __exit__(self, exc_type, exc_value, traceback, connected=True):
        """Safely close out the client and trap any errors."""
        if connected:
            self._client.close()
        if exc_type and exc_value:
            print('Client encountered error, shutting down.')
            print('----------------------------------------')
            print('{}: {}'.format(exc_type.__name__, str(exc_value)))
            print('Traceback (most recent call last):')
            print_tb(traceback)
            sys.exit(1)

    def scan_delete(self, namespace, del_set):
        """
        Delete set from namespace and return the total number of records deleted.
        """
        scan    = self._client.scan(namespace, del_set)
        deleted = 0
        def delete(record):
            if not record: return
            self._client.remove(record[0])
            deleted += 1
            if deleted % 25000 == 0:
                print('Deleted {} records.'.format(deleted))
        scan.foreach(delete, options={'concurrent':True, 'nobins':True})
        return 'Delete complete! {} total records removed.'.format(deleted)

    def info_delete(self, namespace, del_set):
        """
        Using an info command, delete set from namespace.

        (Requires sys-admin role when security is enabled)
        """
        cmd    = 'set-config:context=namespace;id={ns};set={set};set-delete=true;'
        cmd    = cmd.format(ns=namespace, set=del_set)
        result = self._client.info(cmd)

        node_err = []
        for node, resp in result:
            if resp[1] != 'ok\n':
                node_err.append((node, resp[0], resp[1]))
        
        if node_err:
            print('Encountered errors:')
            for (node, r1, r2) in node_err:
                print('{}:\n  {}\n  {}\n'.format(node, r1, r2))
            # Return last line as status
            return 'WARN: Some deletes may have been scheduled.'
        return 'Delete for set {} scheduled! Delete will be processed on next nsup.'


def main():
    g_args = parse_args()
    with spike_client(g_args) as client:
        status = ""
        if not g_args.remote:
            status = client.scan_delete(g_args.namespace, g_args.set)
        else:
            status = client.info_delete(g_args.namespace, g_args.set)
        print(status)   
    return 0

if __name__ == '__main__':
    main()
