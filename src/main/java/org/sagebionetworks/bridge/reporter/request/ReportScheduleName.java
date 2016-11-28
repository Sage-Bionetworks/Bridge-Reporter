package org.sagebionetworks.bridge.reporter.request;

import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

public enum ReportScheduleName {
    DAILY("DAILY"),
    WEEKLY("WEEKLY");

    private final String name;

    ReportScheduleName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String getSuffix() throws PollSqsWorkerBadRequestException {
        return "-" + this.name().toLowerCase() + "-upload-report";
    }
}
