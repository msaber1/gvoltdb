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

import vcli_util

class VerbConfig(VOLT.Verb):

    def __init__(self):
        VOLT.Verb.__init__(self, 'config',
                           description = 'Configure project settings.',
                           usage       = 'NAME=VALUE ...')

    def execute(self, runner):
        if not runner.args:
            vcli_util.abort('At least one argument is required.')
        bad = []
        for arg in runner.args:
            if arg.find('=') == -1:
                bad.append(arg)
        if bad:
            vcli_util.abort('Bad arguments (must be NAME=VALUE format):', bad)
        with runner.config:
            for arg in runner.args:
                full_name, value = [s.strip() for s in arg.split('=', 1)]
                try:
                    section, option = full_name.split('.')
                except ValueError:
                    vcli_util.abort('Parameter name must be in section.name format. E.g.',
                                        ['SECTION.%s=%s' % (full_name, value)])
                runner.config.set(section, option, value)
                vcli_util.info('Configuration: %s.%s=%s' % (section, option, value))
