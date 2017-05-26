package org.sagebionetworks.bridge.reporter.worker;

import static java.util.stream.Collectors.counting;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportScheduleName;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.Upload;

public class UploadsReportGenerator implements ReportGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(UploadsReportGenerator.class);
    
    @Override
    public void generate(BridgeReporterRequest request, BridgeHelper bridgeHelper) throws IOException {
        DateTime startDateTime = request.getStartDateTime();
        DateTime endDateTime = request.getEndDateTime();
        String scheduler = request.getScheduler();
        ReportScheduleName scheduleType = request.getScheduleType();

        String reportId = scheduler + scheduleType.getSuffix();
        
        List<Study> allStudiesSummary = bridgeHelper.getAllStudiesSummary();

        // main block to generate and save reports
        for (Study studySummary : allStudiesSummary) {
            Map<String, Integer> reportData = new HashMap<>();
            String studyId = studySummary.getIdentifier();

            // get all uploads for this studyid
            List<Upload> uploadsForStudy = bridgeHelper.getUploadsForStudy(studyId, startDateTime, endDateTime);

            // aggregate and grouping by upload status
            uploadsForStudy.stream()
                    .collect(Collectors.groupingBy(Upload::getStatus, counting()))
                    .forEach((status, cnt) -> reportData.put(status.toString(), cnt.intValue()));

            // set date and reportData
            LocalDate startDate = startDateTime.toLocalDate();
            ReportData report = new ReportData().date(startDate).data(reportData);

            // finally save report for this study
            bridgeHelper.saveReportForStudy(studyId, reportId, report);

            LOG.info("Save uploads report for hash[studyId]=" + studyId + ", scheduleType=" + scheduleType
                    + ", startDate=" + startDateTime + ",endDate=" + endDateTime + ", reportId=" + reportId
                    + ", reportData=" + reportData.toString());
        }
    }
}
