package org.sagebionetworks.bridge.reporter.worker;

import java.io.IOException;

import org.sagebionetworks.bridge.rest.model.Study;

public interface ReportGenerator {

    public Report generate(BridgeReporterRequest request, Study study) throws IOException;
    
}
