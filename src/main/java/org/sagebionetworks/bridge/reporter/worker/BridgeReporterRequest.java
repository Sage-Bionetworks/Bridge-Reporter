package org.sagebionetworks.bridge.reporter.worker;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.json.DateTimeDeserializer;
import org.sagebionetworks.bridge.json.DateTimeToStringSerializer;
import org.sagebionetworks.bridge.reporter.request.ReportType;

/** Represents a request to the Bridge Reporting Service. */
@JsonDeserialize(builder = BridgeReporterRequest.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BridgeReporterRequest {

    private final DateTime startDateTime;
    private final DateTime endDateTime;
    private final String scheduler;
    private final ReportType reportType;

    public BridgeReporterRequest(DateTime startDateTime, DateTime endDateTime, String scheduler, ReportType scheduleType) {
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
        this.scheduler = scheduler;
        this.reportType = scheduleType;
    }

    @JsonSerialize(using = DateTimeToStringSerializer.class)
    public DateTime getStartDateTime() {
        return this.startDateTime;
    }

    @JsonSerialize(using = DateTimeToStringSerializer.class)
    public DateTime getEndDateTime() {
        return this.endDateTime;
    }


    public String getScheduler() {
        return this.scheduler;
    }

    public ReportType getScheduleType() {
        return this.reportType;
    }
    /*
    Bridge-Reporter request builder
     */
    public static class Builder {
        private DateTime startDateTime;
        private DateTime endDateTime;
        private String scheduler;
        private ReportType reportType;

        @JsonDeserialize(using = DateTimeDeserializer.class)
        public Builder withStartDateTime(DateTime startDateTime) {
            this.startDateTime = startDateTime;
            return this;
        }

        @JsonDeserialize(using = DateTimeDeserializer.class)
        public Builder withEndDateTime(DateTime endDateTime) {
            this.endDateTime = endDateTime;
            return this;
        }

        public Builder withScheduler(String scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder withScheduleType(ReportType reportType) {
            this.reportType = reportType;
            return this;
        }

        public BridgeReporterRequest build() {
            if (Strings.isNullOrEmpty(scheduler)) {
                throw new IllegalStateException("scheduler must be specified.");
            }

            if (reportType == null) {
                throw new IllegalStateException("scheduleType must be specified.");
            }

            if (startDateTime == null) {
                throw new IllegalStateException("startDateTime must be specified.");
            }

            if (endDateTime == null) {
                throw new IllegalStateException("endDateTime must be specified.");
            }

            if (startDateTime.isAfter(endDateTime)) {
                throw new IllegalStateException("startDateTime can't be after endDateTime.");
            }

            return new BridgeReporterRequest(startDateTime, endDateTime, scheduler, reportType);
        }
    }
}
