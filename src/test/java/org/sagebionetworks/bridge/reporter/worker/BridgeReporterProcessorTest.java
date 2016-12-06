package org.sagebionetworks.bridge.reporter.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.Tests;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportScheduleName;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class BridgeReporterProcessorTest {
    private static final String TEST_STUDY_ID = "api";
    private static final String TEST_STUDY_ID_2 = "parkinson";
    private static final String TEST_REPORT_ID = "test-scheduler-daily-upload-report";
    private static final String TEST_REPORT_ID_WEEKLY = "test-scheduler-weekly-upload-report";
    private static final String TEST_SCHEDULER = "test-scheduler";
    private static final ReportScheduleName TEST_SCHEDULE_TYPE = ReportScheduleName.DAILY;
    private static final ReportScheduleName TEST_SCHEDULE_TYPE_WEEKLY = ReportScheduleName.WEEKLY;
    private static final DateTime TEST_START_DATETIME = DateTime.parse("2016-10-19T00:00:00Z");
    private static final DateTime TEST_END_DATETIME = DateTime.parse("2016-10-19T23:59:59Z");

    private static final Map<String, Integer> TEST_REPORT_DATA = ImmutableMap.of("succeeded", 1);
    private static final Map<String, Integer> TEST_REPORT_DATA_WEEKLY = ImmutableMap.of("succeeded", 7);
    private static final ReportData TEST_REPORT = new ReportData().date(TEST_START_DATETIME.toLocalDate()).data(
            TEST_REPORT_DATA);
    private static final ReportData TEST_REPORT_WEEKLY = new ReportData().date(TEST_START_DATETIME.toLocalDate()).data(
            TEST_REPORT_DATA_WEEKLY);

    private static final Map<String, Integer> TEST_REPORT_DATA_2 = ImmutableMap.<String, Integer>builder()
            .put("succeeded", 2).put("requested", 1).build();
    private static final ReportData TEST_REPORT_2 = new ReportData().date(TEST_START_DATETIME.toLocalDate()).data(
            TEST_REPORT_DATA_2);

    private static final Study TEST_STUDY_SUMMARY = new Study().identifier(TEST_STUDY_ID).name(TEST_STUDY_ID);
    private static final Study TEST_STUDY_SUMMARY_2 = new Study().identifier(TEST_STUDY_ID_2).name(TEST_STUDY_ID_2);
    private static final List<Study> TEST_STUDY_SUMMARY_LIST = ImmutableList.of(TEST_STUDY_SUMMARY);
    private static final List<Study> TEST_STUDY_SUMMARY_LIST_2 = ImmutableList.of(TEST_STUDY_SUMMARY,
            TEST_STUDY_SUMMARY_2);

    // test request
    private static final String UPLOAD_TEXT = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'succeeded','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");

    private static final String UPLOAD_TEXT_2 = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'succeeded','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");

    private static final String UPLOAD_TEXT_3 = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'requested','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");


    private static final String REQUEST_JSON_TEXT = "{\n" +
            "   \"scheduler\":\"" + TEST_SCHEDULER +"\",\n" +
            "   \"scheduleType\":\"" + TEST_SCHEDULE_TYPE.toString() + "\",\n" +
            "   \"startDateTime\":\"2016-10-19T00:00:00Z\",\n" +
            "   \"endDateTime\":\"2016-10-19T23:59:59Z\"\n" +
            "}";

    private static final String REQUEST_JSON_TEXT_WEEKLY = "{\n" +
            "   \"scheduler\":\"" + TEST_SCHEDULER +"\",\n" +
            "   \"scheduleType\":\"" + TEST_SCHEDULE_TYPE_WEEKLY.toString() + "\",\n" +
            "   \"startDateTime\":\"2016-10-19T00:00:00Z\",\n" +
            "   \"endDateTime\":\"2016-10-25T23:59:59Z\"\n" +
            "}";

    private static final String REQUEST_JSON_TEXT_INVALID = "{\n" +
            "   \"scheduler\":\"" + TEST_SCHEDULER +"\",\n" +
            "   \"scheduleType\":\"Invalid_Schedule_Type\",\n" +
            "   \"startDateTime\":\"2016-10-19T00:00:00Z\",\n" +
            "   \"endDateTime\":\"2016-10-20T23:59:59Z\"\n" +
            "}";

    private JsonNode requestJson;
    private JsonNode requestJsonWeekly;
    private JsonNode requestJsonInvalid;

    private List<Upload> testUploads;
    private List<Upload> testUploads2;

    private BridgeHelper mockBridgeHelper;
    private BridgeReporterProcessor processor;

    @BeforeClass
    public void generalSetup() throws IOException {
        requestJson = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT, JsonNode.class);
        requestJsonWeekly = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT_WEEKLY, JsonNode.class);
        requestJsonInvalid = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT_INVALID, JsonNode.class);

        Upload testUpload = RestUtils.GSON.fromJson(UPLOAD_TEXT, Upload.class);
        Upload testUpload2 = RestUtils.GSON.fromJson(UPLOAD_TEXT_2, Upload.class);
        Upload testUpload3 = RestUtils.GSON.fromJson(UPLOAD_TEXT_3, Upload.class);

        testUploads = ImmutableList.of(testUpload);
        testUploads2 = ImmutableList.of(testUpload, testUpload2, testUpload3);
    }

    @BeforeMethod
    public void setup() throws Exception {
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAllStudiesSummary()).thenReturn(TEST_STUDY_SUMMARY_LIST);
        when(mockBridgeHelper.getUploadsForStudy(any(), any(), any())).thenReturn(testUploads);

        // set up callback
        processor = new BridgeReporterProcessor();
        processor.setBridgeHelper(mockBridgeHelper);
    }

    @Test
    public void testNormalCase() throws Exception {
        // execute
        processor.process(requestJson);

        // verify
        verify(mockBridgeHelper).getAllStudiesSummary();
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        verify(mockBridgeHelper).saveReportForStudy(eq(TEST_STUDY_ID), eq(TEST_REPORT_ID), eq(TEST_REPORT));
    }

    @Test
    public void testNormalCaseWeekly() throws Exception {
        // execute
        processor.process(requestJsonWeekly);

        // verify
        verify(mockBridgeHelper).getAllStudiesSummary();
        verify(mockBridgeHelper, times(7)).getUploadsForStudy(eq(TEST_STUDY_ID), any(), any());

        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_START_DATETIME.plusDays(1).minusMillis(1)));
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME.plusDays(1)), eq(TEST_START_DATETIME.plusDays(2).minusMillis(1)));
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME.plusDays(2)), eq(TEST_START_DATETIME.plusDays(3).minusMillis(1)));
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME.plusDays(3)), eq(TEST_START_DATETIME.plusDays(4).minusMillis(1)));
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME.plusDays(4)), eq(TEST_START_DATETIME.plusDays(5).minusMillis(1)));
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME.plusDays(5)), eq(TEST_START_DATETIME.plusDays(6).minusMillis(1)));
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME.plusDays(6)), eq(TEST_START_DATETIME.plusDays(7).minusMillis(1)));

        verify(mockBridgeHelper).saveReportForStudy(eq(TEST_STUDY_ID), eq(TEST_REPORT_ID_WEEKLY), eq(TEST_REPORT_WEEKLY));
    }

    @Test
    public void testMultipleStudies() throws Exception {
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAllStudiesSummary()).thenReturn(TEST_STUDY_SUMMARY_LIST_2);
        when(mockBridgeHelper.getUploadsForStudy(any(), any(), any())).thenReturn(testUploads);

        // set up callback
        processor = new BridgeReporterProcessor();
        processor.setBridgeHelper(mockBridgeHelper);

        // execute
        processor.process(requestJson);

        // verify
        verify(mockBridgeHelper).getAllStudiesSummary();
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID_2), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        verify(mockBridgeHelper).saveReportForStudy(eq(TEST_STUDY_ID), eq(TEST_REPORT_ID), eq(TEST_REPORT));
        verify(mockBridgeHelper).saveReportForStudy(eq(TEST_STUDY_ID_2), eq(TEST_REPORT_ID), eq(TEST_REPORT));
    }

    @Test
    public void testMultipleuploads() throws Exception {
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAllStudiesSummary()).thenReturn(TEST_STUDY_SUMMARY_LIST);
        when(mockBridgeHelper.getUploadsForStudy(any(), any(), any())).thenReturn(testUploads2);

        // set up callback
        processor = new BridgeReporterProcessor();
        processor.setBridgeHelper(mockBridgeHelper);


        // execute
        processor.process(requestJson);

        // verify
        verify(mockBridgeHelper).getAllStudiesSummary();
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        verify(mockBridgeHelper).saveReportForStudy(eq(TEST_STUDY_ID), eq(TEST_REPORT_ID), eq(TEST_REPORT_2));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void testInvalidScheduleType() throws Exception {
        // execute
        processor.process(requestJsonInvalid);
    }
}
