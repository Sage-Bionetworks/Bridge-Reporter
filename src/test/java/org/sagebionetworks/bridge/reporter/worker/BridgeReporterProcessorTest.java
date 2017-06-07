package org.sagebionetworks.bridge.reporter.worker;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.Tests;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;


public class BridgeReporterProcessorTest {
    private static final String TEST_STUDY_ID = "api";
    private static final String TEST_STUDY_ID_2 = "parkinson";
    private static final String TEST_SCHEDULER = "test-scheduler";
    private static final ReportType TEST_SCHEDULE_TYPE = ReportType.DAILY;
    private static final ReportType TEST_SCHEDULE_TYPE_WEEKLY = ReportType.WEEKLY;
    private static final DateTime TEST_START_DATETIME = DateTime.parse("2016-10-19T00:00:00Z");
    private static final DateTime TEST_END_DATETIME = DateTime.parse("2016-10-19T23:59:59Z");

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
    
    private static final String PARTICIPANT_1 = Tests.unescapeJson("{" +
            "'id':'user1'," +
            "'sharingScope':'no_sharing'," +
            "'status':'enabled'" +
            "}");

    private static final String PARTICIPANT_2 = Tests.unescapeJson("{" +
            "'id':'user2'," +
            "'sharingScope':'all_qualified_researchers'," +
            "'status':'enabled'" +
            "}");

    private static final String PARTICIPANT_3 = Tests.unescapeJson("{" +
            "'id':'user3'," +
            "'sharingScope':'no_sharing'," +
            "'status':'unverified'" +
            "}");

    private static final String REQUEST_JSON_TEXT = Tests.unescapeJson("{" +
            "'scheduler':'" + TEST_SCHEDULER +"'," +
            "'scheduleType':'" + TEST_SCHEDULE_TYPE.toString() + "'," +
            "'startDateTime':'2016-10-19T00:00:00Z'," +
            "'endDateTime':'2016-10-19T23:59:59Z'}");

    private static final String REQUEST_JSON_TEXT_WEEKLY = Tests.unescapeJson("{" +
            "'scheduler':'" + TEST_SCHEDULER +"'," +
            "'scheduleType':'" + TEST_SCHEDULE_TYPE_WEEKLY.toString() + "'," +
            "'startDateTime':'2016-10-19T00:00:00Z'," +
            "'endDateTime':'2016-10-25T23:59:59Z'}");

    private static final String REQUEST_JSON_TEXT_INVALID = Tests.unescapeJson("{" +
            "'scheduler':'" + TEST_SCHEDULER +"'," +
            "'scheduleType':'Invalid_Schedule_Type'," +
            "'startDateTime':'2016-10-19T00:00:00Z'," +
            "'endDateTime':'2016-10-20T23:59:59Z'}");
    
    private static final String REQUEST_JSON_DAILY_SIGNUPS = Tests.unescapeJson("{" +
            "'scheduler':'" + TEST_SCHEDULER +"'," +
            "'scheduleType':'" + ReportType.DAILY_SIGNUPS.toString() + "'," +
            "'startDateTime':'2016-10-19T00:00:00Z'," +
            "'endDateTime':'2016-10-19T23:59:59Z'}");

    private JsonNode requestJson;
    private JsonNode requestJsonWeekly;
    private JsonNode requestJsonInvalid;
    private JsonNode requestJsonDailySignUps;

    private List<Upload> testUploads;
    private List<Upload> testUploads2;
    private List<StudyParticipant> testParticipants;

    private BridgeHelper mockBridgeHelper;
    private BridgeReporterProcessor processor;

    @BeforeClass
    public void generalSetup() throws IOException {
        requestJson = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT, JsonNode.class);
        requestJsonWeekly = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT_WEEKLY, JsonNode.class);
        requestJsonInvalid = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT_INVALID, JsonNode.class);
        requestJsonDailySignUps = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_DAILY_SIGNUPS, JsonNode.class);

        Upload testUpload = RestUtils.GSON.fromJson(UPLOAD_TEXT, Upload.class);
        Upload testUpload2 = RestUtils.GSON.fromJson(UPLOAD_TEXT_2, Upload.class);
        Upload testUpload3 = RestUtils.GSON.fromJson(UPLOAD_TEXT_3, Upload.class);

        testUploads = ImmutableList.of(testUpload);
        testUploads2 = ImmutableList.of(testUpload, testUpload2, testUpload3);
        
        StudyParticipant user1 = RestUtils.GSON.fromJson(PARTICIPANT_1, StudyParticipant.class);
        StudyParticipant user2 = RestUtils.GSON.fromJson(PARTICIPANT_2, StudyParticipant.class);
        StudyParticipant user3 = RestUtils.GSON.fromJson(PARTICIPANT_3, StudyParticipant.class);
        testParticipants = ImmutableList.of(user1, user2, user3);
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
        verify(mockBridgeHelper).saveReportForStudy(any(Report.class));
    }

    @Test
    public void testNormalCaseWeekly() throws Exception {
        // execute
        processor.process(requestJsonWeekly);

        // verify
        verify(mockBridgeHelper).getAllStudiesSummary();
        verify(mockBridgeHelper, times(1)).getUploadsForStudy(eq(TEST_STUDY_ID), any(), any());

        verify(mockBridgeHelper).saveReportForStudy(any(Report.class));
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
        
        verify(mockBridgeHelper, times(2)).saveReportForStudy(any(Report.class));
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
        verify(mockBridgeHelper).saveReportForStudy(any(Report.class));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void testInvalidScheduleType() throws Exception {
        // execute
        processor.process(requestJsonInvalid);
    }
    
    @Test
    public void testDailySignIns() throws Exception {
        ArgumentCaptor<Report> reportCaptor = ArgumentCaptor.forClass(Report.class);
        
        mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getAllStudiesSummary()).thenReturn(TEST_STUDY_SUMMARY_LIST);
        when(mockBridgeHelper.getParticipantsForStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME))
                .thenReturn(testParticipants);
        
        processor = new BridgeReporterProcessor();
        processor.setBridgeHelper(mockBridgeHelper);
        
        processor.process(requestJsonDailySignUps);
        
        verify(mockBridgeHelper).getAllStudiesSummary();
        verify(mockBridgeHelper).getParticipantsForStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME);
        verify(mockBridgeHelper).saveReportForStudy(reportCaptor.capture());
        
        Report report = reportCaptor.getValue();
        assertEquals(TEST_STUDY_ID, report.getStudyId());
        assertEquals("test-scheduler-daily-signups-report", report.getReportId());
        assertEquals("2016-10-19", report.getDate().toString());
        assertNotNull(report.getData());
    }
}
