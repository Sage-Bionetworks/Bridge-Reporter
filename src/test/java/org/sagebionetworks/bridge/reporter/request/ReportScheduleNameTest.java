package org.sagebionetworks.bridge.reporter.request;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ReportScheduleNameTest {
    // branch coverage test to satisfy jacoco
    @Test
    public void valueOf() {
        assertEquals(ReportType.valueOf("DAILY"), ReportType.DAILY);
        assertEquals(ReportType.valueOf("WEEKLY"), ReportType.WEEKLY);
        assertEquals(ReportType.valueOf("DAILY_SIGNUPS"), ReportType.DAILY_SIGNUPS);
    }
}
