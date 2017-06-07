package org.sagebionetworks.bridge.reporter.worker;

import static java.util.stream.Collectors.counting;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.Upload;

public class UploadsReportGenerator implements ReportGenerator {
    
    @Override
    public Report generate(BridgeReporterRequest request, Study study, BridgeHelper bridgeHelper) throws IOException {
        DateTime startDateTime = request.getStartDateTime();
        DateTime endDateTime = request.getEndDateTime();
        String scheduler = request.getScheduler();
        ReportType scheduleType = request.getScheduleType();

        String reportId = scheduler + scheduleType.getSuffix();
        
        Map<String, Integer> data = new HashMap<>();
        String studyId = study.getIdentifier();

        // get all uploads for this studyid
        List<Upload> uploadsForStudy = bridgeHelper.getUploadsForStudy(studyId, startDateTime, endDateTime);

        // aggregate and grouping by upload status
        uploadsForStudy.stream()
                .collect(Collectors.groupingBy(Upload::getStatus, counting()))
                .forEach((status, cnt) -> data.put(status.toString(), cnt.intValue()));

        // set date and reportData
        LocalDate startDate = startDateTime.toLocalDate();
        ReportData reportData = new ReportData().date(startDate).data(data);

        return new Report.Builder().withStudyId(studyId).withReportId(reportId).withDate(startDate)
                .withReportData(reportData).build();
    }
}
