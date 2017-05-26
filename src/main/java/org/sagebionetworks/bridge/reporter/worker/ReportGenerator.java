package org.sagebionetworks.bridge.reporter.worker;

import java.io.IOException;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;

public interface ReportGenerator {

    public void generate(BridgeReporterRequest request, BridgeHelper bridgeHelper) throws IOException;
    
}
