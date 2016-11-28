package org.sagebionetworks.bridge.reporter.helper;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.reporter.Tests;
import org.sagebionetworks.bridge.sdk.Session;
import org.sagebionetworks.bridge.sdk.StudyClient;
import org.sagebionetworks.bridge.sdk.WorkerClient;
import org.sagebionetworks.bridge.sdk.exceptions.NotAuthenticatedException;
import org.sagebionetworks.bridge.sdk.models.ResourceList;
import org.sagebionetworks.bridge.sdk.models.reports.ReportData;
import org.sagebionetworks.bridge.sdk.models.studies.StudySummary;
import org.sagebionetworks.bridge.sdk.models.upload.Upload;
import org.sagebionetworks.bridge.sdk.utils.BridgeUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class BridgeHelperTest {
    private final String json = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'succeeded','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");

    private static final String TEST_STUDY_ID = "api";
    private static final String TEST_REPORT_ID = "test-report";
    private static final DateTime TEST_START_DATETIME = new DateTime();
    private static final DateTime TEST_END_DATETIME = new DateTime();

    private static final ObjectNode TEST_REPORT_DATA = JsonNodeFactory.instance.objectNode();
    static {
        TEST_REPORT_DATA.put("field1", "test");
    }
    private static final ReportData TEST_REPORT = new ReportData(TEST_START_DATETIME.toLocalDate(), TEST_REPORT_DATA);

    private static final StudySummary TEST_STUDY_SUMMARY;
    static {
        TEST_STUDY_SUMMARY = new StudySummary();
        TEST_STUDY_SUMMARY.setIdentifier(TEST_STUDY_ID);
        TEST_STUDY_SUMMARY.setName(TEST_STUDY_ID);
    }
    private final ResourceList<StudySummary> TEST_STUDY_SUMMARY_LIST = new ResourceList<>(Arrays.asList(TEST_STUDY_SUMMARY), 1);

    private Upload testUpload;
    private ResourceList<Upload> testUploads;

    private static BridgeHelper setupBridgeHelperWithSession(Session session) {
        // Spy bridge helper, because signIn() statically calls ClientProvider.signIn()
        BridgeHelper bridgeHelper = spy(new BridgeHelper());
        doReturn(session).when(bridgeHelper).signIn();
        return bridgeHelper;
    }

    @BeforeClass
    public void setup() throws IOException {
        testUpload = BridgeUtils.getMapper().readValue(json, Upload.class);
        ResourceList<Upload> TEST_UPLOADS = new ResourceList<>(Arrays.asList(testUpload), 1);
    }

    @Test
    public void testGetAllStudiesSummary() {
        StudyClient mockStudyClient = mock(StudyClient.class);
        when(mockStudyClient.getAllStudiesSummary()).thenReturn(TEST_STUDY_SUMMARY_LIST);
        Session mockSession = mock(Session.class);
        when(mockSession.getStudyClient()).thenReturn(mockStudyClient);
        BridgeHelper bridgeHelper = setupBridgeHelperWithSession(mockSession);

        ResourceList<StudySummary> retSummaryList = bridgeHelper.getAllStudiesSummary();
        verify(mockStudyClient).getAllStudiesSummary();
        assertEquals(retSummaryList, TEST_STUDY_SUMMARY_LIST);
    }

    @Test
    public void testGetUploadsForStudy() {
        WorkerClient mockWorkerClient = mock(WorkerClient.class);
        when(mockWorkerClient.getUploadsForStudy(any(), any(), any())).thenReturn(testUploads);
        Session mockSession = mock(Session.class);
        when(mockSession.getWorkerClient()).thenReturn(mockWorkerClient);
        BridgeHelper bridgeHelper = setupBridgeHelperWithSession(mockSession);

        ResourceList<Upload> retUploadsForStudy = bridgeHelper.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME);
        verify(mockWorkerClient).getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME));
        assertEquals(retUploadsForStudy, testUploads);
    }

    @Test
    public void testSaveReportForStudy() {
        WorkerClient mockWorkerClient = mock(WorkerClient.class);
        Session mockSession = mock(Session.class);
        when(mockSession.getWorkerClient()).thenReturn(mockWorkerClient);
        BridgeHelper bridgeHelper = setupBridgeHelperWithSession(mockSession);

        bridgeHelper.saveReportForStudy(TEST_STUDY_ID, TEST_REPORT_ID, TEST_REPORT);
        verify(mockWorkerClient).saveReportForStudy(eq(TEST_STUDY_ID), eq(TEST_REPORT_ID), eq(TEST_REPORT));
    }

    @Test
    public void testSessionHelper() throws Exception {
        WorkerClient mockWorkerClient1 = mock(WorkerClient.class);
        when(mockWorkerClient1.getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME))).thenReturn(testUploads)
                .thenReturn(testUploads).thenThrow(NotAuthenticatedException.class);

        WorkerClient mockWorkerClient2 = mock(WorkerClient.class);
        when(mockWorkerClient2.getUploadsForStudy(eq(TEST_STUDY_ID), eq(TEST_START_DATETIME), eq(TEST_END_DATETIME))).thenReturn(testUploads);

        // Create 2 mock sessions. Each mock session simply returns its corresponing worker client.
        Session mockSession1 = mock(Session.class);
        when(mockSession1.getWorkerClient()).thenReturn(mockWorkerClient1);

        Session mockSession2 = mock(Session.class);
        when(mockSession2.getWorkerClient()).thenReturn(mockWorkerClient2);

        // Spy BridgeHelper.signIn(), which returns these sessions.
        BridgeHelper bridgeHelper = spy(new BridgeHelper());
        doReturn(mockSession1).doReturn(mockSession2).when(bridgeHelper).signIn();

        ResourceList<Upload> retUploads = bridgeHelper.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME);
        assertEquals(retUploads, testUploads);

        verify(mockWorkerClient1, times(1)).getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME);
    }
}
