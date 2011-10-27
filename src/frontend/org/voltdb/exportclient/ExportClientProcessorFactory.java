package org.voltdb.exportclient;

public interface ExportClientProcessorFactory {
    ExportClientProcessor factory(String advertisement);
}
