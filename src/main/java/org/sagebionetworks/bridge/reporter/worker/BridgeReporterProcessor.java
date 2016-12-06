package org.sagebionetworks.bridge.reporter.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportScheduleName;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;

/**
 * SQS callback. Called by the PollSqsWorker. This handles a reporting request.
 */
@Component
public class BridgeReporterProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeReporterProcessor.class);

    private BridgeHelper bridgeHelper;

    @Autowired
    @Qualifier("ReporterHelper")
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Process the passed sqs msg as JsonNode. */
    public void process(JsonNode body) throws IOException, PollSqsWorkerBadRequestException {
        BridgeReporterRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(body, BridgeReporterRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }

        DateTime startDateTime = request.getStartDateTime();
        DateTime endDateTime = request.getEndDateTime();
        String scheduler = request.getScheduler();
        ReportScheduleName scheduleType = request.getScheduleType();

        String reportId = scheduler + scheduleType.getSuffix();

        LOG.info("Received request for hash[scheduler]=" + scheduler + ", scheduleType=" + scheduleType + ", startDate=" +
                startDateTime + ",endDate=" + endDateTime);

        Stopwatch requestStopwatch = Stopwatch.createStarted();

        List<Study> allStudiesSummary = bridgeHelper.getAllStudiesSummary();

        // main block to generate and save reports
        try {
            for (Study studySummary : allStudiesSummary) {
                Map<String, Integer> reportData = new HashMap<>();
                String studyId = studySummary.getIdentifier();

                // get all uploads for this studyid, differentiating by scheduleType
                List<Upload> uploadsForStudy = getUploadsForStudyHelper(studyId, startDateTime, endDateTime);

                // aggregate and grouping by upload status
                uploadsForStudy.stream()
                        .collect(Collectors.groupingBy(Upload::getStatus, counting()))
                        .forEach((status, cnt) -> reportData.put(status.toString(), cnt.intValue()));

                // set date and reportData
                LocalDate startDate = startDateTime.toLocalDate();
                ReportData report = new ReportData().date(startDate).data(reportData);

                // finally save report for this study
                bridgeHelper.saveReportForStudy(studyId, reportId, report);

                LOG.info("Save report for hash[studyId]=" + studyId + ", scheduleType=" + scheduleType + ", startDate=" +
                        startDateTime + ",endDate=" + endDateTime + ", reportId=" + reportId + ", reportData=" + reportData.toString());
            }
        } finally {
            LOG.info("Request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                    " seconds for hash[scheduler]=" + scheduler + ", scheduleType=" + scheduleType + ", startDate=" +
                    startDateTime + ",endDate=" + endDateTime);
        }
    }


    /**
     * Helper method to call getUploadsForStudy distinguishing by daily or weekly
     */
    private List<Upload> getUploadsForStudyHelper(String studyId, DateTime startDateTime, DateTime endDateTime)
            throws IOException {
        List<Upload> uploadList = new ArrayList<>();

        if (Days.daysBetween(startDateTime, endDateTime).isLessThan(Days.days(1))) {
            uploadList = bridgeHelper.getUploadsForStudy(studyId, startDateTime, endDateTime);
        } else {
            while (startDateTime.isBefore(endDateTime)) {
                uploadList.addAll(bridgeHelper.getUploadsForStudy(studyId, startDateTime,
                        startDateTime.plusDays(1).minusMillis(1)));
                startDateTime = startDateTime.plusDays(1);
                // sleep for a while for ddb to meet read capacity
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    LOG.error("InterruptedException occurred for thread sleep", e);
                }
            }
        }

        return uploadList;
    }
}
