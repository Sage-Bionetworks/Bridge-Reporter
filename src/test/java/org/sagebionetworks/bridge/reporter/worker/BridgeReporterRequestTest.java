package org.sagebionetworks.bridge.reporter.worker;

import org.joda.time.DateTime;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.request.ReportScheduleName;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class BridgeReporterRequestTest {
    private static final String TEST_SCHEDULER = "test-scheduler";
    private static final ReportScheduleName TEST_SCHEDULE_TYPE = ReportScheduleName.DAILY;
    private static final DateTime TEST_START_DATETIME = DateTime.parse("2016-10-19T00:00:00Z");
    private static final DateTime TEST_END_DATETIME = DateTime.parse("2016-10-20T23:59:59Z");

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*scheduler.*")
    public void nullScheduler() {
        new BridgeReporterRequest.Builder().withScheduleType(TEST_SCHEDULE_TYPE).withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_END_DATETIME).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*scheduler.*")
    public void emptyScheduler() {
        new BridgeReporterRequest.Builder().withScheduler("").withScheduleType(TEST_SCHEDULE_TYPE).withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_END_DATETIME).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*scheduleType.*")
    public void nullScheduleType() {
        new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER).withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_END_DATETIME).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*startDateTime.*")
    public void nullStartDateTime() {
        new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER).withScheduleType(TEST_SCHEDULE_TYPE)
                .withEndDateTime(TEST_END_DATETIME).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*endDate.*")
    public void nullEndDateTime() {
        new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER).withScheduleType(TEST_SCHEDULE_TYPE)
                .withStartDateTime(TEST_START_DATETIME).build();
    }

    @Test
    public void startDateBeforeEndDate() {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER)
                .withScheduleType(TEST_SCHEDULE_TYPE)
                .withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_END_DATETIME).build();
        assertEquals(request.getScheduler(), TEST_SCHEDULER);
        assertEquals(request.getScheduleType(), TEST_SCHEDULE_TYPE);
        assertEquals(request.getStartDateTime(), TEST_START_DATETIME);
        assertEquals(request.getEndDateTime(), TEST_END_DATETIME);
    }

    @Test
    public void startDateSameAsEndDate() {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER)
                .withScheduleType(TEST_SCHEDULE_TYPE)
                .withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_START_DATETIME).build();
        assertEquals(request.getScheduler(), TEST_SCHEDULER);
        assertEquals(request.getScheduleType(), TEST_SCHEDULE_TYPE);
        assertEquals(request.getStartDateTime(), TEST_START_DATETIME);
        assertEquals(request.getEndDateTime(), TEST_START_DATETIME);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void startDateAfterEndDate() {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER)
                .withScheduleType(TEST_SCHEDULE_TYPE)
                .withStartDateTime(TEST_END_DATETIME)
                .withEndDateTime(TEST_START_DATETIME).build();
    }

    @Test
    public void jsonSerialization() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"scheduler\":\"test-scheduler\",\n" +
                "   \"scheduleType\":\"DAILY\",\n" +
                "   \"startDateTime\":\"2016-10-19T00:00:00.000Z\",\n" +
                "   \"endDateTime\":\"2016-10-20T23:59:59.000Z\"\n" +
                "}";

        // convert to POJO
        BridgeReporterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeReporterRequest.class);
        assertEquals(request.getScheduler(), TEST_SCHEDULER);
        assertEquals(request.getScheduleType(), TEST_SCHEDULE_TYPE);
        assertEquals(request.getStartDateTime(), TEST_START_DATETIME);
        assertEquals(request.getEndDateTime(), TEST_END_DATETIME);

        // convert back to JSON
        String convertedJson =DefaultObjectMapper.INSTANCE.writeValueAsString(request);

        // then convert to a map so we can validate the raw JSON
        Map<String, String> jsonMap = DefaultObjectMapper.INSTANCE.readValue(convertedJson, Map.class);
        assertEquals(4, jsonMap.size());
        assertEquals(jsonMap.get("scheduler"), "test-scheduler");
        assertEquals(jsonMap.get("scheduleType"), "DAILY");
        assertEquals(jsonMap.get("startDateTime"), "2016-10-19T00:00:00.000Z");
        assertEquals(jsonMap.get("endDateTime"), "2016-10-20T23:59:59.000Z");
    }
}
