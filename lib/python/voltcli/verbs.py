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

__author__ = 'scooper'

import sys
from voltdbclient import *
from voltcli import cli
from voltcli import environment
from voltcli import utility

#===============================================================================
class BaseVerb(object):
#===============================================================================
    """
    Base class for verb implementations. Used by the @Volt.Command decorator.
    """
    def __init__(self, name, **kwargs):
        self.name = name
        self.classpath = utility.kwargs_get_string(kwargs, 'classpath', default = None)
        self.cli_spec = cli.CLISpec(**kwargs)
        self.dirty_opts = False
        self.dirty_args = False
        self.command_arguments = utility.kwargs_get_list(kwargs, 'command_arguments',
                                                         default = None)
        utility.debug(str(self))

    def execute(self, runner):
        utility.abort('%s "%s" object does not implement the required execute() method.'
                            % (self.__class__.__name__, self.name))

    def add_options(self, *args):
        """
        Add options if not already present as an option or argument.
        """
        for o in args:
            dest_name = o.get_dest()
            if self.cli_spec.find_option(dest_name):
                utility.debug('Not adding "%s" option more than once.' % dest_name)
            else:
                self.cli_spec.add_to_list('options', o)
                self.dirty_opts = True

    def add_arguments(self, *args):
        self.cli_spec.add_to_list('arguments', *args)
        self.dirty_args = True

    def add_command_arguments(self, *args):
        self.command_arguments.extend(args)

    def add_to_classpath(self, *paths):
        if self.classpath is None:
            self.classpath = ''
        self.classpath = ':'.join([self.classpath] + list(paths))

    def get_attr(self, name, default = None):
        return self.cli_spec.get_attr(name)

    def pop_attr(self, name, default = None):
        return self.cli_spec.pop_attr(name)

    def merge_java_options(self, name, *options):
        return self.cli_spec.merge_java_options(name, *options)

    def set_defaults(self, **kwargs):
        return self.cli_spec.set_defaults(**kwargs)

    def __cmp__(self, other):
        return cmp(self.name, other.name)

    def __str__(self):
        return '%s: %s\n%s' % (self.__class__.__name__, self.name, self.cli_spec)

    def get_option_count(self):
        if not self.cli_spec.options:
            return 0
        return len(self.cli_spec.options)

    def get_argument_count(self):
        if not self.cli_spec.arguments:
            return 0
        return len(self.cli_spec.arguments)

    def iter_options(self):
        if self.cli_spec.options:
            self._check_options()
            for o in self.cli_spec.options:
                yield o

    def iter_arguments(self):
        """
        Iterate all arguments in the specification.
        """
        if self.cli_spec.arguments:
            self._check_arguments()
            for a in self.cli_spec.arguments:
                yield a

    def iter_required_arguments(self):
        """
        Iterate arguments not supplied via the command_arguments attribute.
        """
        if self.cli_spec.arguments:
            self._check_arguments()
            for a in self.cli_spec.arguments[len(self.command_arguments):]:
                yield a

    def _check_options(self):
        if self.dirty_opts:
            self.cli_spec.options.sort()
            self.dirty_opts = False


    def _check_arguments(self):
        if self.dirty_args:
            # Use a local function to sanity check an argument's min/max counts,
            # with an additional check applied to arguments other than the last
            # one since they cannot repeat or be missing.
            def check_argument(cli_spec_arg, is_last):
                if cli_spec_arg.min_count < 0 or cli_spec_arg.max_count < 0:
                    utility.abort('%s argument (%s) has a negative min or max count declared.'
                                        % (self.name, self.cli_spec_arg.name))
                if cli_spec_arg.min_count == 0 and cli_spec_arg.max_count == 0:
                    utility.abort('%s argument (%s) has zero min and max counts declared.'
                                        % (self.name, self.cli_spec_arg.name))
                if not is_last and (cli_spec_arg.min_count != 1 or cli_spec_arg.max_count != 1):
                    utility.abort('%s argument (%s) is not the last argument, '
                                  'but has min/max counts declared.'
                                        % (self.name, self.cli_spec_arg.name))
            nargs = len(self.cli_spec.arguments)
            if nargs > 1:
                # Check all arguments except the last.
                for i in range(nargs-1):
                    check_argument(self.cli_spec.arguments[i], False)
                # Check the last argument.
                check_argument(self.cli_spec.arguments[-1], True)
            self.dirty_args = False

#===============================================================================
class CommandVerb(BaseVerb):
#===============================================================================
    """
    Verb that wraps a command function. Used by the @VOLT.Command decorator.
    """
    def __init__(self, name, function, **kwargs):
        BaseVerb.__init__(self, name, **kwargs)
        self.function = function
        self.bundles = utility.kwargs_get_list(kwargs, 'bundles')
        # Allow the bundles to adjust options.
        for bundle in self.bundles:
            bundle.initialize(self)

    def execute(self, runner):
        # Start the bundles, e.g. to create client a connection.
        for bundle in self.bundles:
            if hasattr(bundle, 'start'):
                bundle.start(self, runner)
        try:
            # Set up the go() method for use as the default implementation.
            runner.set_default_func(self.default_func)
            # Execute the verb function.
            self.function(runner)
        finally:
            # Stop the bundles in reverse order.
            for i in range(len(self.bundles)-1, -1, -1):
                if hasattr(self.bundles[i], 'stop'):
                    self.bundles[i].stop(self, runner)

    def default_func(self, runner):
        # Give the bundles a chance to do something useful.
        for bundle in self.bundles:
            if hasattr(bundle, 'go'):
                bundle.go(self, runner)
        # Invoke the verb object's go() method.
        self.go(runner)

    def go(self, runner):
        pass

#===============================================================================
class Modifier(object):
#===============================================================================
    """
    Class for declaring multi-command modifiers.
    """
    def __init__(self, name, function, description, arg_name = ''):
        self.name = name
        self.description = description
        self.function = function
        self.arg_name = arg_name.upper()

#===============================================================================
class MultiVerb(CommandVerb):
#===============================================================================
    """
    Verb to create multi-commands with modifiers and optional arguments.
    """
    def __init__(self, name, function, **kwargs):
        CommandVerb.__init__(self, name, function, **kwargs)
        self.modifiers = utility.kwargs_get_list(kwargs, 'modifiers', default = [])
        if not self.modifiers:
            utility.abort('Multi-command "%s" must provide a "modifiers" list.' % self.name)
        valid_modifiers = '|'.join([mod.name for mod in self.modifiers])
        has_args = 0
        rows = []
        for mod in self.modifiers:
            if mod.arg_name:
                usage = '%s %s [ %s ... ]' % (self.name, mod.name, mod.arg_name)
                has_args += 1
            else:
                usage = '%s %s' % (self.name, mod.name)
            rows.append((usage, mod.description))
        caption = '"%s" Command Variations' % self.name
        description2 = utility.format_table(rows, caption = caption, separator = '  ')
        self.set_defaults(description2 = description2.strip())
        args = [
            cli.StringArgument('modifier',
                               'command modifier (valid modifiers: %s)' % valid_modifiers)]
        if has_args > 0:
            if has_args == len(self.modifiers):
                arg_desc = 'optional arguments(s)'
            else:
                arg_desc = 'optional arguments(s) (where applicable)'
            args.append(cli.StringArgument('arg', arg_desc, min_count = 0, max_count = None))
        self.add_arguments(*args)

    def go(self, runner):
        mod_name = runner.opts.modifier.lower()
        for mod in self.modifiers:
            if mod.name == mod_name:
                mod.function(runner)
                break
        else:
            utility.error('Invalid "%s" modifier "%s". Valid modifiers are listed below:'
                                % (self.name, mod_name),
                          [mod.name for mod in self.modifiers])
            runner.help(self.name)

#===============================================================================
class VerbSpace(object):
#===============================================================================
    """
    Manages a collection of Verb objects that support a particular CLI interface.
    """
    def __init__(self, name, version, description, VOLT, verbs_by_name):
        self.name          = name
        self.version       = version
        self.description   = description.strip()
        self.VOLT          = VOLT
        self.verbs_by_name = verbs_by_name
        self.verb_names    = self.verbs_by_name.keys()
        self.verb_names.sort()
