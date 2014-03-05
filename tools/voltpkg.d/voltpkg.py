# This file is part of VoltDB.
# Copyright (C) 2008-2014 VoltDB Inc.
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

"""
Main voltpkg command module.
"""

import sys
import os
import glob
from voltcli import utility
from voltcli import environment

#===============================================================================
class ConfigProperty:
#===============================================================================
    def __init__(self, description, default=None):
        self.description = description
        self.default = default

#===============================================================================
class Global:
#===============================================================================
    tool_name = 'voltpkg'
    config_key = tool_name
    config_properties = dict(
        app_name      = ConfigProperty('application name',
                                       default=os.path.basename(os.getcwd())),
        base_image    = ConfigProperty('base docker image name',
                                       default='ubuntu'),
        base_tag      = ConfigProperty('base docker image tag (e.g. OS version)',
                                       default='12.04'),
        repo_url      = ConfigProperty('package repository URL',
                                       default='http://mirror.anl.gov/pub/ubuntu/'),
        repo_name     = ConfigProperty('package repository name',
                                       default='precise'),
        repo_sections = ConfigProperty('package repository name',
                                       default='main restricted universe'),
    )

#===============================================================================
def config_key(name):
#===============================================================================
    """
    Generate a configuration property key from a property name.
    """
    return '.'.join([Global.config_key, name])

#===============================================================================
def config_help(samples=None):
#===============================================================================
    if samples:
        paren = ' (using actual property name)'
    else:
        paren = ''
        samples.append('name')
    set_samples = []
    for name in samples:
        set_samples.append('   %s config set %s=%s'
                                % (environment.command_name, name, '%s_VALUE' % name.upper()))
    format_dict = dict(
        command=environment.command_name,
        config=environment.config_name,
        set_samples='\n'.join(set_samples),
        paren=paren,
    )
    return '''\
Use the "config" verb to modify and view properties as follows.

To set a property%(paren)s:
%(set_samples)s

To display one, many, or all properties:
   %(command)s config get name
   %(command)s config get name1 name2 ...
   %(command)s config get

To get "config" command help:
   %(command)s help config

You can also edit "%(config)s" directly in a text editor.''' % format_dict

#===============================================================================
class Configuration(dict):
#===============================================================================
    """
    Dictionary that also allows attribute-based access.
    """
    def __getattr__(self, name):
        return self[name]
    def __setattr__(self, name, value):
        self[name] = value

#===============================================================================
def get_config(runner, reset = False):
#===============================================================================
    """
    Utility function to look for and validate a set of configuration properties.
    """
    config = Configuration()
    missing = []
    defaults = []
    msgblocks = []
    for name in sorted(Global.config_properties.keys()):
        config_property = Global.config_properties[name]
        key = config_key(name)
        value = runner.config.get(key)
        if not value or reset:
            if config_property.default is None:
                missing.append(name)
                runner.config.set_permanent(key, '')
            else:
                defaults.append(name)
                value = Global.config_properties[name].default
                runner.config.set_permanent(key, value)
                setattr(config, name, value)
        else:
            # Use an existing config value.
            config[name] = value
    samples = []
    if not reset and missing:
        table = [(name, Global.config_properties[name].description) for name in missing]
        msgblocks.append([
            'The following settings must be configured before proceeding:',
            '',
            utility.format_table(table, headings=['PROPERTY', 'DESCRIPTION'],
                                        indent=3, separator='  ')
        ])
        samples.extend(missing)
        config = None
    if defaults:
        msgblocks.append([
            'The following setting defaults were applied and saved permanently:',
            '',
            utility.format_table(
                [(name, Global.config_properties[name].default) for name in defaults],
                indent=3, separator='  ', headings=['PROPERTY', 'VALUE'])
        ])
    if reset:
        config = None
    elif config is None:
        msgblocks.append([config_help(samples=samples)])
    if msgblocks:
        for msgblock in msgblocks:
            print ''
            for msg in msgblock:
                print msg
        print ''
    return config

#===============================================================================
def run_config_get(runner):
#===============================================================================
    """
    Implementation of "config get" sub-command."
    """
    if not runner.opts.arg:
        # All labels.
        for (key, value) in runner.config.query_pairs():
            sys.stdout.write('%s=%s\n' % (key, value))
    else:
        # Specific keys requested.
        for arg in runner.opts.arg:
            n = 0
            for (key, value) in runner.config.query_pairs(filter=config_key(arg)):
                sys.stdout.write('%s=%s\n' % (key, value))
                n += 1
            if n == 0:
                sys.stdout.write('%s *not found*\n' % arg)

#===============================================================================
def run_config_set(runner):
#===============================================================================
    """
    Implementation of "config set" sub-command.
    """
    bad = []
    for arg in runner.opts.arg:
        if arg.find('=') == -1:
            bad.append(arg)
    if bad:
        runner.abort('Bad arguments (must be KEY=VALUE format):', bad)
    for arg in runner.opts.arg:
        key, value = [s.strip() for s in arg.split('=', 1)]
        if key.find('.') == -1:
            key = config_key(key)
        runner.config.set_permanent(key, value)
        print 'set %s=%s' % (key, value)

#===============================================================================
def run_config_reset(runner):
#===============================================================================
    """
    Implementation of "config reset" sub-command.
    """
    utility.info('Clearing configuration settings...')
    # Perform the reset.
    get_config(runner, reset=True)
    # Display the help.
    get_config(runner)

#===============================================================================
@VOLT.Multi_Command(
    description  = 'Manipulate and view configuration properties.',
    modifiers = [
        VOLT.Modifier('get', run_config_get,
                      'Show one or more configuration properties.',
                      arg_name = 'KEY'),
        VOLT.Modifier('reset', run_config_reset,
                      'Reset configuration properties to default values.'),
        VOLT.Modifier('set', run_config_set,
                      'Set one or more configuration properties (use KEY=VALUE format).',
                      arg_name = 'KEY_VALUE'),
    ]
)
def config(runner):
#===============================================================================
    runner.go()

#===============================================================================
@VOLT.Command(
    description='Create a VoltDB application Docker configuration (Dockerfile).',
    options = [
        VOLT.BooleanOption('-O', '--overwrite', 'overwrite',
                           'overwrite existing Dockerfile', default=False),
    ],
)
def docker(runner):
#===============================================================================
    config = get_config(runner)
    if config is None:
        sys.exit(1)
    if os.path.exists('Dockerfile') and not runner.opts.overwrite:
        runner.abort('Dockerfile exists. Delete the file or add the -O/--overwrite option.')
    config['in_base']    = environment.voltdb_base
    config['in_lib']     = environment.voltdb_lib
    config['in_voltdb']  = environment.voltdb_voltdb
    config['out_base']   = '/opt/%(app_name)s' % config
    config['out_lib']    = os.path.join(config['out_base'], 'lib')
    config['out_voltdb'] = os.path.join(config['out_base'], 'voltdb')
    # Build up Dockerfile lines, expanding config symbols as we go.
    lines = []
    def add_line(keyword, *args):
        args_out = [keyword]
        for arg in args:
            if arg.find('%(') != -1:
                args_out.append(arg % config)
            else:
                args_out.append(arg)
        lines.append(' '.join(args_out))
    add_line('#', 'Generated by the VoltDB "%s docker" command.' % environment.command_name)
    add_line('FROM', '%(base_image)s:%(base_tag)s')
    add_line('ADD', '%(in_lib)s %(out_lib)s')
    add_line('ADD', '%(in_voltdb)s %(out_voltdb)s')
    for path in glob.glob(os.path.join(environment.voltdb_base, 'README*')):
        add_line('ADD', path, '%(out_base)s/')
    add_line('ADD', '%(in_base)s/version.txt %(out_base)s')
    if config['base_image'].lower() == 'ubuntu':
        if 'repo_url' in config and 'repo_name' in config:
            add_line('RUN', 'echo "deb %(repo_url)s %(repo_name)s %(repo_sections)s"',
                            '> /etc/apt/sources.list')
        else:
            runner.abort('Configuration options "repo_url" and "repo_name" must both be set.')
        add_line('RUN', 'apt-get update')
        add_line('RUN', 'apt-get install -y openjdk-7-jre-headless')
        add_line('ENTRYPOINT', '["%(out_base)s/bin/voltdb"]')
        add_line('EXPOSE', '21212')
    else:
        runner.abort('Unknown base image "%s".' % config['base_image'])
    try:
        f = utility.File('Dockerfile', mode='w')
        f.open()
        for line in lines:
            f.write('%s\n' % line)
        runner.info('Saved Dockerfile.')
    finally:
        f.close()
