package org.sagebionetworks.bridge.reporter.worker;

import static org.testng.AssertJUnit.assertEquals;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

public class ReportTest {
    
    @Test
    public void ReportWorks() {
        LocalDate date = LocalDate.parse("2017-05-30");
        Object data = new Object();
        
        Report report = new Report.Builder()
                .withStudyId("studyId")
                .withReportId("reportId")
                .withDate(date)
                .withReportData(data).build();
        assertEquals("studyId", report.getStudyId());
        assertEquals("reportId", report.getReportId());
        assertEquals(date, report.getDate());
        assertEquals(data, report.getData());
    }

}
