package org.sagebionetworks.bridge.reporter.worker;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.joda.time.DateTime;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.Study;

import com.google.common.collect.Lists;

public class SignUpsReportGeneratorTest {
    
    private static final String STUDY_ID = "test-study";
    private static final DateTime START_DATE = DateTime.parse("2017-06-09T00:00:00.000Z");
    private static final DateTime END_DATE = DateTime.parse("2017-06-09T23:59:59.999Z");

    @Test
    public void test() throws Exception {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder()
                .withScheduleType(ReportType.DAILY_SIGNUPS)
                .withScheduler("test-scheduler")
                .withStartDateTime(START_DATE)
                .withEndDateTime(END_DATE).build();
        BridgeHelper bridgeHelper = mock(BridgeHelper.class);

        Study study = mock(Study.class);
        when(study.getIdentifier()).thenReturn(STUDY_ID);
        when(bridgeHelper.getAllStudiesSummary()).thenReturn(Lists.newArrayList(study));
        
        SignUpsReportGenerator generator = new SignUpsReportGenerator();
        Report report = generator.generate(request, study, bridgeHelper);
        
        assertEquals(STUDY_ID, report.getStudyId());
        assertEquals("test-scheduler-daily-signups-report", report.getReportId());
        assertEquals("2017-06-09", report.getDate().toString());
        assertNotNull(report.getData());
        
        verify(bridgeHelper).getParticipantsForStudy(STUDY_ID, START_DATE, END_DATE);
    }
}
