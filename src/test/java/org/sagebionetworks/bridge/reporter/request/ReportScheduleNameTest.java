package org.sagebionetworks.bridge.reporter.request;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ReportScheduleNameTest {
    // branch coverage test to satisfy jacoco
    @Test
    public void valueOf() {
        assertEquals(ReportScheduleName.valueOf("DAILY"), ReportScheduleName.DAILY);
        assertEquals(ReportScheduleName.valueOf("WEEKLY"), ReportScheduleName.WEEKLY);
        assertEquals(ReportScheduleName.valueOf("DAILY_SIGNUPS"), ReportScheduleName.DAILY_SIGNUPS);
    }
}
