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

import os
from voltcli import utility, environment

tmpdir = '/tmp/voter'

# Pro tests are a little different. Assume the working directory is examples/voter.
# Provide license.xml if pro was built.
if os.environ.get('VOLTPRO'):
    recover_action = 'recover'
    edition = "pro"
    license_path = os.path.join(os.environ['VOLTPRO'], 'obj', 'pro',
                                'voltdb-ent-%s' % environment.version,
                                'voltdb', 'license.xml')
    if not os.path.exists(license_path):
        utility.warning('License file "%s" not found.' % license_path,
                        'Build pro in "%s" to generate a license.' % os.environ['VOLTPRO'])
    cmdlog_xml = '''
    <commandlog enabled="true">
        <frequency time="100" transactions="1000" />
    </commandlog>'''
else:
    recover_action = 'start'
    edition = "community"
    license_path = None
    cmdlog_xml = ''

# Deployment file contents with command logging for pro.
# Paths are substituted later to allow differences between master and replica, etc..
deployment_xml = '''\
<?xml version="1.0"?>
<deployment>
    <cluster hostcount="1" sitesperhost="2" kfactor="0" />
    <httpd enabled="true">
        <jsonapi enabled="true" />
    </httpd>%(cmdlog_xml)s
    <paths>
        <voltdbroot path="%%(voltdbroot)s" />
        <snapshots path="%%(snapshots)s" />
        <commandlog path="%%(commandlog)s" />
        <commandlogsnapshot path="%%(commandlog)s/snapshots" />
    </paths>
</deployment>''' % locals()

log4j_xml = '''\
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">
<log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/">
    <appender name="console" class="org.apache.log4j.ConsoleAppender">
        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern"
            value="%m%n"/>
        </layout>
        <filter class="org.apache.log4j.varia.LevelRangeFilter">
            <param name="levelMin" value="TRACE"/>
            <param name="levelMax" value="INFO"/>
        </filter>
    </appender>
    <appender name="consolefiltered" class="org.apache.log4j.ConsoleAppender">
        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern"
            value="%p: %m%n"/>
        </layout>
        <filter class="org.apache.log4j.varia.LevelRangeFilter">
            <param name="levelMin" value="WARN"/>
            <param name="levelMax" value="FATAL"/>
        </filter>
    </appender>
    <appender name="file" class="org.apache.log4j.DailyRollingFileAppender">
        <param name="file" value="${VOLT_LOG}"/>
        <param name="DatePattern" value="'.'yyyy-MM-dd" />
        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern" value="%d %-5p [%t] %c: %m%n"/>
        </layout>
    </appender>
    <logger name="DRAGENT">
        <level value="INFO"/>
        <appender-ref ref="console"/>
    </logger>
    <logger name="DRSTATS">
        <level value="INFO"/>
        <appender-ref ref="console"/>
    </logger>
    <logger name="TM">
        <level value="INFO"/>
        <appender-ref ref="console"/>
    </logger>
    <logger name="REJOIN">
        <level value="INFO"/>
        <appender-ref ref="console"/>
    </logger>
    <logger name="CONSOLE">
        <level value="INFO"/>
        <appender-ref ref="console"/>
    </logger>
    <root>
        <priority value="info" />
        <appender-ref ref="file" />
        <appender-ref ref="consolefiltered" />
    </root>
</log4j:configuration>'''
