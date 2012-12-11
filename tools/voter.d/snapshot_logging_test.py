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
from voltcli import utility

# Pro tests are a little different. Assume the working directory is examples/voter.
distdir = os.path.join(os.path.dirname(os.path.dirname(os.getcwd())), 'obj', 'release', 'dist')
if os.environ.get('VOLTPRO'):
    ispro = True
    recover_action = 'recover'
    log_tag = "pro"
    license_path = os.path.join(distdir, 'voltdb', 'license.xml')
    cmdlog_xml = '''\
    <commandlog enabled="true">
        <frequency time="100" transactions="1000" />
    </commandlog>'''
else:
    ispro = False
    recover_action = 'start'
    log_tag = "community"
    license_path = None
    cmdlog_xml = ''

tmpdir     = '/tmp/snapshot_logging'
snapdir    = os.path.join(tmpdir, 'snapshots')
clogdir    = os.path.join(tmpdir, 'clog')
csnapdir   = os.path.join(tmpdir, 'csnap')
deployment = os.path.join(tmpdir, 'deployment.xml')

# Deployment file contents with command logging.
deployment_xml = '''\
<?xml version="1.0"?>
<deployment>
    <cluster hostcount="1" sitesperhost="2" kfactor="0" />
    <httpd enabled="true">
        <jsonapi enabled="true" />
    </httpd>
%(cmdlog_xml)s
    <paths>
        <commandlog path="%(clogdir)s" />
        <commandlogsnapshot path="%(csnapdir)s" />
    </paths>
</deployment>''' % globals()

@VOLT.Command(
    description = 'Snapshot test to compare IV2 and legacy logging.',
    options = (
        VOLT.BooleanOption('-L', None, 'legacy', 'Run in legacy mode.'),
    ),
)
def snapshot_logging(runner):
    tester = Tester(runner, runner.opts.legacy)
    tester.run()

class Tester(object):

    def __init__(self, runner, legacy):
        self.runner  = runner
        self.legacy  = legacy
        if license_path is not None:
            self.license_option = ['-l', license_path]
        else:
            self.license_option = []
        if self.legacy:
            os.environ['VOLT_ENABLEIV2'] = 'false'
            self.mode_name = 'legacy'
        else:
            os.environ['VOLT_ENABLEIV2'] = 'true'
            self.mode_name = 'IV2'
        self.server_pid = None
        if self.legacy:
            self.log = '%s-legacy.log' % log_tag
        else:
            self.log = '%s-iv2.log' % log_tag

    def cleanup(self):
        print '\n=== Cleanup ===\n'
        shutil.rmtree(snapdir, ignore_errors = True)
        os.makedirs(snapdir)

    def populate(self):
        print '\n=== Populate ===\n'
        self.runner.java.execute('voter.AsyncBenchmark', None, '--duration=5', '--warmup=0',
                                 classpath = 'obj')

    def snapshot(self, nonce, blocking, csv):
        print '\n=== Snapshot save %s ===\n' % nonce
        args = []
        if blocking:
            args.append('-b')
        if csv:
            args.extend(('-f', 'csv'))
        args.extend((snapdir, nonce))
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
        print '\n=== Snapshot restore %s ===\n' % nonce
        self.runner.call('voltadmin.restore', snapdir, nonce, stayalive = fails)

    def shutdown(self):
        print '\n=== Shutdown server ===\n'
        # Reconnect in case the current client is bad.
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

    def copylog(self):
        target = os.path.join(tmpdir, self.log)
        print '\n=== Copy log "%s" ===\n' % target
        os.system('/bin/cp -vf log/volt.log %s' % target)

    def create_deployment(self):
        open(deployment, 'w').write(deployment_xml)

    def server(self, action):
        self.server_pid = os.fork()
        if self.server_pid == 0:
            print('\n=== Start %s server (pid=%d, mode=%s) ===\n'
                        % (log_tag, os.getpid(), self.mode_name))
            self.runner.call('volt.%s' % action, '-d', deployment,
                             'voter.jar', *self.license_option)
            print '\n=== Exit server (pid=%d) ===\n' % os.getpid()
            os._exit(0)
        else:
            time.sleep(10)
            return

    def snapshot_test(self):
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
        self.snapshot('s4',  True,  True)
        self.populate()

    def restore_test(self):
        print '\n=== Start restore client (pid=%d) ===\n' % os.getpid()
        time.sleep(10)
        self.runner.connect('localhost', retries = 10)
        # In a Pro recovery startup snapshots are are automatically restored.
        if not ispro:
            self.restore('s1', False)
        self.restore('xx',  True)           # Fails

    def run(self):

        # Look for and shutdown a running server.
        if self.runner.connect('localhost', quiet = True, retries = 0):
            self.shutdown()

        # Spawn a parallel server process.
        shutil.rmtree('log', ignore_errors = True)

        # Prepare and compile.
        self.cleanup()
        self.create_deployment()
        self.runner.call('build', '-C')

        # Test sequence 1:
        #   - Create database.
        #   - Perform good and bad snapshots.
        #   - Add extra unsnapshotted logged transactions for recovery.
        #   - Shutdown database.
        self.server('create')
        try:
            self.snapshot_test()
        finally:
            self.shutdown()

        # Test sequence 2:
        #   - Recover database.
        #   - Perform good and bad snapshot restore.
        #   - Shutdown database.
        self.server(recover_action)
        try:
            self.restore_test()
        finally:
            self.shutdown()
        self.copylog()