package org.sagebionetworks.bridge.reporter.worker;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.sdk.models.ResourceList;
import org.sagebionetworks.bridge.sdk.models.reports.ReportData;
import org.sagebionetworks.bridge.sdk.models.studies.StudySummary;
import org.sagebionetworks.bridge.sdk.models.upload.Upload;
import org.sagebionetworks.bridge.sqs.PollSqsCallback;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;


/**
 * SQS callback. Called by the PollSqsWorker. This handles a reporting request.
 */
@Component
public class BridgeReporterSqsCallback implements PollSqsCallback {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeReporterSqsCallback.class);

    private BridgeHelper bridgeHelper;

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Parses the SQS message. */
    @Override
    public void callback(String messageBody) throws IOException, PollSqsWorkerBadRequestException {
        BridgeReporterRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.readValue(messageBody, BridgeReporterRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }

        DateTime startDateTime = request.getStartDateTime();
        DateTime endDateTime = request.getEndDateTime();
        String scheduler = request.getScheduler();
        BridgeReporterRequest.ReportScheduleType scheduleType = request.getScheduleType();

        String reportId;
        if (scheduleType == BridgeReporterRequest.ReportScheduleType.DAILY) {
            reportId = scheduler + "-daily-upload-report";
        } else if (scheduleType == BridgeReporterRequest.ReportScheduleType.WEEKLY) {
            reportId = scheduler + "-weekly-upload-report";
        } else {
            throw new PollSqsWorkerBadRequestException("Invalid report schedule type: " + scheduleType.toString());
        }

        LOG.info("Received request for hash[scheduler]=" + scheduler + ", scheduleType=" + scheduleType + ", startDate=" +
                startDateTime + ",endDate=" + endDateTime);

        ResourceList<StudySummary> allStudiesSummary = bridgeHelper.getAllStudiesSummary();

        Stopwatch requestStopwatch = Stopwatch.createStarted();

        // main block to generate and save reports
        try {
            allStudiesSummary.forEach(studySummary -> {
                ObjectNode reportData = JsonNodeFactory.instance.objectNode();
                String studyId = studySummary.getIdentifier();

                // get all uploads for this studyid
                ResourceList<Upload> uploadsForStudy = bridgeHelper.getUploadsForStudy(studyId, startDateTime, endDateTime);

                // aggregate and grouping by upload status
                uploadsForStudy.getItems().stream()
                        .collect(Collectors.groupingBy(Upload::getStatus, counting()))
                        .forEach((status, cnt) -> reportData.put(status.toString(), cnt.intValue()));

                // set date and reportData
                LocalDate startDate = startDateTime.toLocalDate();
                ReportData report = new ReportData(startDate, reportData);

                // finally save report for this study
                bridgeHelper.saveReportForStudy(studyId, reportId, report);

                LOG.info("Save report for hash[studyId]=" + studyId + ", scheduleType=" + scheduleType + ", startDate=" +
                        startDateTime + ",endDate=" + endDateTime + ", reportId=" + reportId + ", reportData=" + reportData.toString());
            });
        } finally {
            LOG.info("Request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                    " seconds for hash[scheduler]=" + scheduler + ", scheduleType=" + scheduleType + ", startDate=" +
                    startDateTime + ",endDate=" + endDateTime);
        }
    }
}
