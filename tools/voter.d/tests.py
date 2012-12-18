# This file is part of VoltDB.
# Copyright (C) 2008-2012 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
import os
import shutil
import time
import traceback
import glob
from voltcli import utility, environment
import voter_utility

def snapshots(runner):
    tester = Tester(runner, runner.opts.legacy)
    tester.snapshots()

def master(runner):
    tester = Tester(runner, runner.opts.legacy)
    tester.master()

def replica(runner):
    tester = Tester(runner, runner.opts.legacy)
    tester.replica()

def tail_replication(runner):
    tester = Tester(runner, runner.opts.legacy)
    tester.tail_replication()

@VOLT.Multi_Command(
    description  = 'Run a test.',
    options = (
        VOLT.BooleanOption('-L', None, 'legacy', 'Run in legacy mode.'),
    ),
    modifiers = [
        VOLT.Modifier('snapshots', snapshots,
                      'Snapshot test to compare IV2 and legacy logging.'),
        VOLT.Modifier('master', master,
                      'Run replication master server and populate database.'),
        VOLT.Modifier('replica', replica,
                      'Run replica server and dragent.'),
        VOLT.Modifier('tail_replication', tail_replication,
                      'Tail master and replica logs (requires multitail).'),
    ],
)
def test(runner):
    runner.go()

class Tester(object):

    class Paths(dict):
        def __init__(self, edition, tag, mode, **kwargs):
            self.edition = edition
            self.tag     = tag
            self.mode    = mode
            self.kwargs  = kwargs
        def __getattr__(self, name):
            subdir = '%(edition)s/%(tag)s/%(mode)s' % dict(
                            edition = self.edition,
                            tag     = self.tag,
                            mode    = self.mode)
            return os.path.join(voter_utility.tmpdir, subdir, self.kwargs[name])

    def __init__(self, runner, legacy):
        self.runner  = runner
        if legacy:
            os.environ['VOLT_ENABLEIV2'] = 'false'
            self.mode = 'legacy'
        else:
            os.environ['VOLT_ENABLEIV2'] = 'true'
            self.mode = 'IV2'
        if voter_utility.license_path is not None:
            self.license_option = ['-l', voter_utility.license_path]
        else:
            self.license_option = []
        print 'license_option=%s' % self.license_option
        self.server_pid = None

    def get_paths(self, tag):
        return Tester.Paths(voter_utility.edition, tag, self.mode,
                log        = 'volt.log',
                log4j      = 'log4j.xml',
                deployment = 'deployment.xml',
                temp       = 'tmp',
                snapshots  = 'tmp/snapshots',
                commandlog = 'tmp/commandlog',
                voltdbroot = 'tmp/voltdbroot',
        )

    def prepare(self, tag):
        print '\n=== Prepare test ===\n'
        # Look for and shutdown a running server.
        if self.runner.connect('localhost', quiet = True, retries = 0):
            self.shutdown()
        # Set up the paths generator
        self.paths = self.get_paths(tag)
        # Wipe and recreate the needed directories.
        shutil.rmtree(self.paths.temp, ignore_errors = True)
        os.makedirs(self.paths.temp)
        os.makedirs(self.paths.snapshots)
        os.makedirs(os.path.join(self.paths.commandlog, 'snapshots'))
        os.makedirs(self.paths.voltdbroot)
        # Generate log4j.xml
        log4j_xml = voter_utility.log4j_xml.replace('${VOLT_LOG}', self.paths.log)
        open(self.paths.log4j, 'w').write(log4j_xml)
        deployment_xml = voter_utility.deployment_xml % dict(
                voltdbroot = self.paths.voltdbroot,
                snapshots  = self.paths.snapshots,
                commandlog = self.paths.commandlog,
        )
        # Point at the generated log4j.xml
        os.environ['LOG4J_CONFIG_PATH'] = self.paths.log4j
        # Generate deployment.xml
        open(self.paths.deployment, 'w').write(deployment_xml)

    def populate(self, duration = 5):
        print '\n=== Populate ===\n'
        self.runner.java.execute('voter.AsyncBenchmark', None, '--duration=%d' % duration,
                                 '--warmup=0', classpath = 'obj')

    def snapshot(self, nonce, blocking, csv):
        print '\n=== Snapshot save %s ===\n' % nonce
        args = []
        if blocking:
            args.append('-b')
        if csv:
            args.extend(('-f', 'csv'))
        args.extend((self.paths.snapshots, nonce))
        self.runner.call('voltadmin.save', *args)
        retcode = self.check_snapshot(nonce)
        while retcode == 1:
            print 'Waiting for snapshot to complete...'
            time.sleep(5)
            retcode = self.check_snapshot(nonce)
        if retcode != 0:
            utility.abort('Snapshot "%s" failed.' % nonce)

    def check_snapshot(self, nonce):
        response = self.runner.call_proc('@SnapshotStatus', [], [])
        if response.status() != 1:
            utility.error('Snapshot "%s" failed.' % nonce)
            return -1
        # Return 0 if it succeeded, 1 if it is in-progress, or -1 if it failed.
        retcode = 1
        for i in range(response.table(0).tuple_count()):
            tuple = response.table(0).tuple(i)
            if tuple.column_string(6) == nonce:
                if tuple.column_string(13) == 'SUCCESS':
                    retcode = 0
                else:
                    retcode = -1
                    break
        return retcode

    def restore(self, nonce, fails):
        print '\n=== Snapshot restore %s from %s ===\n' % (nonce, self.paths.snapshots)
        self.runner.call('voltadmin.restore', self.paths.snapshots, nonce, stayalive = fails)

    def shutdown(self):
        print '\n=== Shutdown server ===\n'
        self.runner.call('voltadmin.shutdown', stayalive = True)
        if self.server_pid:
            retry = 0
            while os.waitpid(self.server_pid, os.WNOHANG) == (0, 0):
                retry += 1
                print 'Waiting for server (pid=%d) to exit (%d)...' % (self.server_pid, retry)
                if retry == 5:
                    utility.abort('Server never exited.')
                time.sleep(5)
            print 'Server exited.'
            self.server_pid = 0

    def wait_for_server(self):
        if self.server_pid:
            os.waitpid(self.server_pid, 0)
            print 'Server exited.'
            self.server_pid = 0
        else:
            print '* Server is not running. *'

    def start_server(self, action):
        print('\n=== Start %s server (pid=%d, mode=%s) ===\n'
                    % (voter_utility.edition, os.getpid(), self.mode))
        self.runner.call('volt.%s' % action, '-d', self.paths.deployment,
                         'voter.jar', *self.license_option)
        print '\n=== Exit server (pid=%d) ===\n' % os.getpid()

    def spawn_server(self, action):
        self.server_pid = os.fork()
        if self.server_pid == 0:
            self.start_server(action)
            os._exit(0)
        else:
            time.sleep(10)
            return

    def snapshots(self):

        # Prepare and compile.
        self.prepare('snapshots')
        self.runner.call('build', '-C')

        # Test sequence 1:
        #   - Create database.
        #   - Perform good and bad snapshots.
        #   - Add extra unsnapshotted logged transactions for recovery.
        #   - Shutdown database.
        # Spawn a parallel server process.
        self.spawn_server('create')
        try:
            print '\n=== Start snapshot client (pid=%d) ===\n' % os.getpid()
            time.sleep(10)
            self.runner.connect('localhost', retries = 10)
            self.populate()
            self.snapshot('s1', False, False)
            self.populate()
            self.snapshot('s2', False,  True)
            self.populate()
            self.snapshot('s3',  True, False)
            self.snapshot('s3',  True, False)   # Fails
            self.populate()
        finally:
            self.shutdown()

        # Test sequence 2:
        #   - Recover database.
        #   - Perform good and bad snapshot restore.
        #   - Shutdown database.
        self.start_server(voter_utility.recover_action)
        try:
            print '\n=== Start restore client (pid=%d) ===\n' % os.getpid()
            time.sleep(10)
            self.runner.connect('localhost', retries = 10)
            # In a Pro recovery startup snapshots are are automatically restored.
            if voter_utility.edition == 'community':
                self.restore('s1', False)
            self.restore('xx',  True)           # Fails
        finally:
            self.shutdown()

    def master(self):

        # Look for and shutdown a running server.
        if self.runner.connect('localhost', quiet = True, retries = 0):
            self.shutdown()

        # Prepare and compile.
        self.prepare('master')
        self.runner.call('build', '-C')

        # Test sequence:
        #   - Create database.
        #   - Populate database.
        #   - Shutdown database.
        # Spawn a parallel server process.
        self.spawn_server('create')
        try:
            print '\n=== Start replication server client (pid=%d) ===\n' % os.getpid()
            time.sleep(20)
            self.runner.connect('localhost', retries = 10)
            while True:
                self.populate(duration = 120)
                s = '???'
                while s and s != 'q':
                    sys.stdout.write("Press Enter to continue or 'q' to quit: ")
                    sys.stdout.flush()
                    s = raw_input()
                if s == 'q':
                    self.shutdown()
                    break
            self.wait_for_server()
        except Exception:
            # Make sure the server shuts down no matter what.
            self.shutdown()

    def tail_replication(self):
        master_log  = self.get_paths('master').log
        replica_log = self.get_paths('replica').log
        os.system('multitail -iw %s 10 -iw %s 10' % (master_log, replica_log))
