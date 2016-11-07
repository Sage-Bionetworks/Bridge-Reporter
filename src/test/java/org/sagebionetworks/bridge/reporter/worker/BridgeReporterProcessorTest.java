package org.sagebionetworks.bridge.reporter.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.Tests;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportScheduleName;
import org.sagebionetworks.bridge.sdk.models.ResourceList;
import org.sagebionetworks.bridge.sdk.models.reports.ReportData;
import org.sagebionetworks.bridge.sdk.models.studies.StudySummary;
import org.sagebionetworks.bridge.sdk.models.upload.Upload;
import org.sagebionetworks.bridge.sdk.utils.BridgeUtils;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class BridgeReporterProcessorTest {
    public BridgeReporterProcessorTest() throws IOException {
    }

    private static final String TEST_STUDY_ID = "api";
    private static final String TEST_STUDY_ID_2 = "parkinson";
    private static final String TEST_REPORT_ID = "test-scheduler-daily-upload-report";
    private static final String TEST_REPORT_ID_WEEKLY = "test-scheduler-weekly-upload-report";
    private static final String TEST_SCHEDULER = "test-scheduler";
    private static final ReportScheduleName TEST_SCHEDULE_TYPE = ReportScheduleName.DAILY;
    private static final ReportScheduleName TEST_SCHEDULE_TYPE_WEEKLY = ReportScheduleName.WEEKLY;
    private static final DateTime TEST_START_DATETIME = DateTime.parse("2016-10-19T00:00:00Z");
    private static final DateTime TEST_END_DATETIME = DateTime.parse("2016-10-20T23:59:59Z");

    private static final ObjectNode TEST_REPORT_DATA = JsonNodeFactory.instance.objectNode();
    static {
        TEST_REPORT_DATA.put("SUCCEEDED", 1);
    }
    private static final ReportData TEST_REPORT = new ReportData(TEST_START_DATETIME.toLocalDate(), TEST_REPORT_DATA);

    private static final ObjectNode TEST_REPORT_DATA_2 = JsonNodeFactory.instance.objectNode();
    static {
        TEST_REPORT_DATA_2.put("SUCCEEDED", 2);
        TEST_REPORT_DATA_2.put("REQUESTED", 1);
    }
    private static final ReportData TEST_REPORT_2 = new ReportData(TEST_START_DATETIME.toLocalDate(), TEST_REPORT_DATA_2);


    private static final StudySummary TEST_STUDY_SUMMARY;
    private static final StudySummary TEST_STUDY_SUMMARY_2;
    static {
        TEST_STUDY_SUMMARY = new StudySummary();
        TEST_STUDY_SUMMARY.setIdentifier(TEST_STUDY_ID);
        TEST_STUDY_SUMMARY.setName(TEST_STUDY_ID);

        TEST_STUDY_SUMMARY_2 = new StudySummary();
        TEST_STUDY_SUMMARY_2.setIdentifier(TEST_STUDY_ID_2);
        TEST_STUDY_SUMMARY_2.setName(TEST_STUDY_ID_2);
    }
    private static final ResourceList<StudySummary> TEST_STUDY_SUMMARY_LIST = new ResourceList<>(Arrays.asList(TEST_STUDY_SUMMARY), 1);
    private static final ResourceList<StudySummary> TEST_STUDY_SUMMARY_LIST_2 = new ResourceList<>(Arrays.asList(TEST_STUDY_SUMMARY, TEST_STUDY_SUMMARY_2), 2);


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
            "   \"endDateTime\":\"2016-10-20T23:59:59Z\"\n" +
            "}";

    private final JsonNode REQUEST_JSON = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT, JsonNode.class);

    private static final String REQUEST_JSON_TEXT_WEEKLY = "{\n" +
            "   \"scheduler\":\"" + TEST_SCHEDULER +"\",\n" +
            "   \"scheduleType\":\"" + TEST_SCHEDULE_TYPE_WEEKLY.toString() + "\",\n" +
            "   \"startDateTime\":\"2016-10-19T00:00:00Z\",\n" +
            "   \"endDateTime\":\"2016-10-20T23:59:59Z\"\n" +
            "}";

    private final JsonNode REQUEST_JSON_WEEKLY = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT_WEEKLY, JsonNode.class);


    private static final String REQUEST_JSON_TEXT_INVALID = "{\n" +
            "   \"scheduler\":\"" + TEST_SCHEDULER +"\",\n" +
            "   \"scheduleType\":\"Invalid_Schedule_Type\",\n" +
            "   \"startDateTime\":\"2016-10-19T00:00:00Z\",\n" +
            "   \"endDateTime\":\"2016-10-20T23:59:59Z\"\n" +
            "}";

    private final JsonNode REQUEST_JSON_INVALID = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT_INVALID, JsonNode.class);


    private final Upload TEST_UPLOAD = BridgeUtils.getMapper().readValue(UPLOAD_TEXT, Upload.class);
    private final ResourceList<Upload> TEST_UPLOADS = new ResourceList<>(Arrays.asList(TEST_UPLOAD), 1);

    private final Upload TEST_UPLOAD_2 = BridgeUtils.getMapper().readValue(UPLOAD_TEXT_2, Upload.class);
    private final Upload TEST_UPLOAD_3 = BridgeUtils.getMapper().readValue(UPLOAD_TEXT_3, Upload.class);
    private final ResourceList<Upload> TEST_UPLOADS_2 = new ResourceList<>(Arrays.asList(TEST_UPLOAD, TEST_UPLOAD_2, TEST_UPLOAD_3), 3);


    private BridgeHelper mockBridgeHelper;
    private BridgeReporterProcessor processor;

    @BeforeMethod
    public void setup() throws Exception {
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAllStudiesSummary()).thenReturn(TEST_STUDY_SUMMARY_LIST);
        when(mockBridgeHelper.getUploadsForStudy(any(), any(), any())).thenReturn(TEST_UPLOADS);

        // set up callback
        processor = new BridgeReporterProcessor();
        processor.setBridgeHelper(mockBridgeHelper);
    }

    @Test
    public void testNormalCase() throws Exception {
        // execute
        processor.process(REQUEST_JSON);

        // verify
        verify(mockBridgeHelper).getAllStudiesSummary();
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        verify(mockBridgeHelper).saveReportForStudy(eq(TEST_STUDY_ID), eq(TEST_REPORT_ID), eq(TEST_REPORT));
    }

    @Test
    public void testNormalCaseWeekly() throws Exception {
        // execute
        processor.process(REQUEST_JSON_WEEKLY);

        // verify
        verify(mockBridgeHelper).getAllStudiesSummary();
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        verify(mockBridgeHelper).saveReportForStudy(eq(TEST_STUDY_ID), eq(TEST_REPORT_ID_WEEKLY), eq(TEST_REPORT));
    }

    @Test
    public void testMultipleStudies() throws Exception {
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAllStudiesSummary()).thenReturn(TEST_STUDY_SUMMARY_LIST_2);
        when(mockBridgeHelper.getUploadsForStudy(any(), any(), any())).thenReturn(TEST_UPLOADS);

        // set up callback
        processor = new BridgeReporterProcessor();
        processor.setBridgeHelper(mockBridgeHelper);

        // execute
        processor.process(REQUEST_JSON);

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
        when(mockBridgeHelper.getUploadsForStudy(any(), any(), any())).thenReturn(TEST_UPLOADS_2);

        // set up callback
        processor = new BridgeReporterProcessor();
        processor.setBridgeHelper(mockBridgeHelper);


        // execute
        processor.process(REQUEST_JSON);

        // verify
        verify(mockBridgeHelper).getAllStudiesSummary();
        verify(mockBridgeHelper).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        verify(mockBridgeHelper).saveReportForStudy(eq(TEST_STUDY_ID), eq(TEST_REPORT_ID), eq(TEST_REPORT_2));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void testInvalidScheduleType() throws Exception {
        // execute
        processor.process(REQUEST_JSON_INVALID);
    }
}
