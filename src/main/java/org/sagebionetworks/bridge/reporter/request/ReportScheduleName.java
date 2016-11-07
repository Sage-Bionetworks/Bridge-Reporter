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
        if (this == DAILY) {
            return "-daily-upload-report";
        } else if (this == WEEKLY) {
            return "-weekly-upload-report";
        }
        throw new PollSqsWorkerBadRequestException("Invalid report schedule type: " + this.getName());
    }
}
