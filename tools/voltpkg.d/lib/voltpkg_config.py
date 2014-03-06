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
voltpkg configuration management
"""

import sys
from voltcli import utility
from voltcli import environment


class Property:
    def __init__(self, description, default=None):
        self.description = description
        self.default = default


class Configuration(dict):
    """
    Dictionary that also allows attribute-based access.
    """
    def __getattr__(self, name):
        return self[name]
    def __setattr__(self, name, value):
        self[name] = value


class ConfigurationTool(object):
    """
    Configuration management tool.
    """
    def __init__(self, key, **properties):
        """
        Configuration management tool constructor.
        """
        self.key = key
        self.properties = properties

    def config_key(self, name):
        """
        Generate a configuration property key from a property name.
        """
        return '.'.join([self.key, name])

    def config_help(self, samples=None):
        """
        Format configuration help text.
        """
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


    def get_config(self, runner, reset = False):
        """
        Utility function to look for and validate a set of configuration properties.
        """
        config = Configuration()
        missing = []
        defaults = []
        msgblocks = []
        for name in sorted(self.properties.keys()):
            prop = self.properties[name]
            key = self.config_key(name)
            value = runner.config.get(key)
            if not value or reset:
                if prop.default is None:
                    missing.append(name)
                    runner.config.set_permanent(key, '')
                else:
                    defaults.append(name)
                    value = self.properties[name].default
                    runner.config.set_permanent(key, value)
                    setattr(config, name, value)
            else:
                # Use an existing config value.
                config[name] = value
        samples = []
        if not reset and missing:
            table = [(name, self.properties[name].description) for name in missing]
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
                    [(name, self.properties[name].default) for name in defaults],
                    indent=3, separator='  ', headings=['PROPERTY', 'VALUE'])
            ])
        if reset:
            config = None
        elif config is None:
            msgblocks.append([self.config_help(samples=samples)])
        if msgblocks:
            for msgblock in msgblocks:
                print ''
                for msg in msgblock:
                    print msg
            print ''
        return config


    def run_config_get(self, runner):
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
                for (key, value) in runner.config.query_pairs(filter=self.config_key(arg)):
                    sys.stdout.write('%s=%s\n' % (key, value))
                    n += 1
                if n == 0:
                    sys.stdout.write('%s *not found*\n' % arg)


    def run_config_set(self, runner):
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
                key = self.config_key(key)
            runner.config.set_permanent(key, value)
            print 'set %s=%s' % (key, value)


    def run_config_reset(self, runner):
        """
        Implementation of "config reset" sub-command.
        """
        utility.info('Clearing configuration settings...')
        # Perform the reset.
        self.get_config(runner, reset=True)
        # Display the help.
        self.get_config(runner)

