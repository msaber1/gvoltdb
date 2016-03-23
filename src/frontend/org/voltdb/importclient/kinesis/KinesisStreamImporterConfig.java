/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importclient.kinesis;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.AbstractFormatterFactory;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * ImporterConfig implementation for pull socket importer. There will be an ImporterConfig per
 * resource ID.
 */
public class KinesisStreamImporterConfig implements ImporterConfig
{
    private final URI m_resourceID;
    private final String m_region;
    private final String m_streamName;
    private final String m_appName;
    private final String m_procedure;
    private final AbstractFormatterFactory m_formatterFactory;

    public KinesisStreamImporterConfig(String region,
            String streamName,
            String appName,
            String procedure,
            AbstractFormatterFactory formatterFactory)
    {
        m_region = region;
        m_streamName = streamName;
        m_appName = appName;
        m_resourceID = URI.create("kinesis://" + region + "." + streamName);
        m_procedure = procedure;
        m_formatterFactory = formatterFactory;
    }

    @Override
    public URI getResourceID()
    {
        return m_resourceID;
    }

    @Override
    public AbstractFormatterFactory getFormatterFactory()
    {
        return m_formatterFactory;
    }

    String getProcedure() {
        return m_procedure;
    }
    
    String getRegion() {
        return m_region;
    }
    
    String getStreamName() {
        return m_streamName;
    }
    
    String getAppName() {
        return m_appName;
    }
}
