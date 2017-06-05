package org.sagebionetworks.bridge.reporter.request;

import java.util.EnumSet;

public enum ReportType {
    DAILY("DAILY"),
    WEEKLY("WEEKLY"),
    DAILY_SIGNUPS("DAILY_SIGNUPS");
    
    private static final EnumSet<ReportType> UPLOAD_REPORTS = EnumSet.of(DAILY,WEEKLY);

    private final String name;

    ReportType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String getSuffix() {
        // Upload reports were initially assumed, so relax this for future reports
        return (UPLOAD_REPORTS.contains(this)) ?
            ("-" + this.name().toLowerCase() + "-upload-report") :
            ("-" + this.name().replaceAll("_", "-").toLowerCase() + "-report");
    }
}
