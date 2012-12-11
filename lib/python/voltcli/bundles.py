# This file is part of VoltDB.

# Copyright (C) 2008-2012 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
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

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# Bundles are objects that can mix in various behaviors to commands by
# assigning bundle instances to the "bundles" keyword in @Volt.command()
# decorators.  Bundle classes handle events by implementing the following
# optional call-back methods.
#
#   initialize(verb)     for tweaking CLI option/argument metadata
#   start(verb, runner)  for performing preliminary setup
#   go(verb, runner)     for implementing the default runner.go() behavior
#   stop(verb, runner)   for clean up activity
#
# Bundle classes only have to implement the call-back methods required to get
# the job done.

__author__ = 'scooper'

import copy

from voltdbclient import *
from voltcli import cli
from voltcli import utility

#===============================================================================
class ConnectionBundle(object):
#===============================================================================
    """
    Bundle class to add host(s), port(s), user, and password connection
    options. Use by assigning an instance to the "bundles" keyword inside a
    decorator invocation.
    """
    def __init__(self, default_port = None, min_count = 1, max_count = 1):
        self.default_port = default_port
        self.min_count    = min_count
        self.max_count    = max_count

    def initialize(self, verb):
        verb.add_options(
            cli.HostOption('-H', '--host', 'host', 'connection',
                           default      = 'localhost',
                           min_count    = self.min_count,
                           max_count    = self.max_count,
                           default_port = self.default_port),
            cli.StringOption('-p', '--password', 'password', "the connection password"),
            cli.StringOption('-u', '--user', 'username', 'the connection user name'))

#===============================================================================
class BaseClientBundle(ConnectionBundle):
#===============================================================================
    """
    Bundle class to automatically create a client connection.  Use by
    assigning an instance to the "bundles" keyword inside a decorator
    invocation.
    """
    def __init__(self, default_port, retries = 5):
        self.retries = retries
        ConnectionBundle.__init__(self, default_port = default_port, min_count = 1, max_count = 1)

    def start(self, verb, runner):
        runner.connect(runner.opts.host.host,
                       port     = runner.opts.host.port,
                       username = runner.opts.username,
                       password = runner.opts.password,
                       retries  = self.retries)

    def stop(self, verb, runner):
        if runner.client:
            runner.client.close()

#===============================================================================
class ClientBundle(BaseClientBundle):
#===============================================================================
    """
    Bundle class to automatically create an non-admin client connection.  Use
    by assigning an instance to the "bundles" keyword inside a decorator
    invocation.
    """
    def __init__(self, retries = 5):
        BaseClientBundle.__init__(self, 21212, retries = retries)

#===============================================================================
class AdminBundle(BaseClientBundle):
#===============================================================================
    """
    Bundle class to automatically create an admin client connection.  Use by
    assigning an instance to the "bundles" keyword inside a decorator
    invocation.
    """
    def __init__(self, retries = 5):
        BaseClientBundle.__init__(self, 21211, retries = retries)

#===============================================================================
class JavaBundle(object):
#===============================================================================
    """
    Base bundle class to add appropriate Java options. Not used directly.
    """
    def __init__(self, java_class, passthrough = False, java_opts_override = []):
        self.java_class         = java_class
        self.java_opts_override = java_opts_override
        self.passthrough        = passthrough

    def initialize(self, verb):
        verb.set_defaults(passthrough = self.passthrough)
        verb.add_options(
            cli.IntegerOption(None, '--debugport', 'debugport',
                              'enable remote Java debugging on the specified port'),
            cli.BooleanOption(None, '--dry-run', 'dryrun', None))
        if self.java_opts_override:
            verb.merge_java_options('java_opts_override', *self.java_opts_override)

    def go(self, verb, runner):
        self.run_java(verb, runner, *runner.args)

    def run_java(self, verb, runner, *args):
        opts_override = verb.get_attr('java_opts_override', default = [])
        kw = {}
        try:
            if runner.opts.debugport:
                kw = {'debugport': debugport}
        except AttributeError:
            pass
        final_args = list(copy.copy(args))
        if args:
            final_args.extend(args)
        runner.java.execute(self.java_class, opts_override, *args)

#===============================================================================
class ServerBundle(JavaBundle):
#===============================================================================
    """
    Bundle class to add appropriate VoltDB server options and to start a server.
    Use by assigning an instance to the "bundles" keyword inside a decorator
    invocation.
    """
    def __init__(self, server_subcommand):
        # Add appropriate server-ish Java options.
        JavaBundle.__init__(self, 'org.voltdb.VoltDB',
                                  java_opts_override = [
                                        '-server',
                                        '-XX:+HeapDumpOnOutOfMemoryError',
                                        '-XX:HeapDumpPath=/tmp',
                                        '-XX:-ReduceInitialCardMarks'])
        self.server_subcommand = server_subcommand

    def initialize(self, verb):
        verb.add_options(
            cli.StringOption('-d', '--deployment', 'deployment',
                             'the deployment configuration file path',
                             default = 'deployment.xml'),
            cli.HostOption('-H', '--host', 'host', 'the host', default = 'localhost'),
            cli.StringOption('-l', '--license', 'license', 'the license file path'))
        verb.add_arguments(
            cli.StringArgument('catalog',
                               'the application catalog jar file path'))

    def go(self, verb, runner):
        args = [self.server_subcommand]
        catalog = runner.opts.catalog
        if not catalog:
            catalog = runner.config.get('volt.catalog')
        if catalog is None:
            utility.abort('A catalog path is required.')
        args.extend(['catalog', catalog])
        if runner.opts.deployment:
            args.extend(['deployment', runner.opts.deployment])
        if runner.opts.host:
            args.extend(['host', runner.opts.host.host])
            if runner.opts.host.port is not None:
                args.extend(['port', runner.opts.host.port])
        if runner.opts.license:
            args.extend(['license', runner.opts.license])
        self.run_java(verb, runner, *args)

#===============================================================================
class HelpBundle(object):
#===============================================================================
    """
    Bundle class to provide standard help.  Use by assigning an instance to the
    "bundles" keyword inside a decorator invocation.
    """
    def initialize(self, verb):
        verb.set_defaults(description = 'Display general or verb-specific help.',
                          baseverb = True)
        verb.add_options(
            cli.BooleanOption('-a', '--all', 'all',
                              'display all available help, including verb usage'))
        verb.add_arguments(
            cli.StringArgument('verb', 'verb name', min_count = 0, max_count = None))

    def go(self, verb, runner):
        runner.help(all = runner.opts.all, *runner.opts.verb)

#===============================================================================
class PackageBundle(object):
#===============================================================================
    """
    Bundle class to create a runnable Python package.  Use by assigning an
    instance to the "bundles" keyword inside a decorator invocation.
    """
    def initialize(self, verb):
        verb.set_defaults(description  = 'Create a runnable Python program package.',
                          baseverb     = True,
                          hideverb     = True,
                          description2 = '''\
The optional NAME argument(s) allow package generation for base commands other
than the current one. If no NAME is provided the current base command is
packaged.''')
        verb.add_options(
            cli.BooleanOption('-f', '--force', 'force',
                              'overwrite existing file without asking',
                              default = False),
            cli.StringOption('-o', '--output_dir', 'output_dir',
                             'specify the output directory (defaults to the working directory)'))
        verb.add_arguments(
            cli.StringArgument('name', 'base command name', min_count = 0, max_count = None))

    def go(self, verb, runner):
        runner.package(runner.opts.output_dir, runner.opts.force, *runner.opts.name)
