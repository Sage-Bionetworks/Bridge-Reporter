package org.sagebionetworks.bridge.reporter.helper;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.reporter.helper.BridgeHelper.MAX_PAGE_SIZE;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.mockito.InOrder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import retrofit2.Call;
import retrofit2.Response;

import org.sagebionetworks.bridge.reporter.Tests;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.NotAuthenticatedException;
import org.sagebionetworks.bridge.rest.model.Message;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadList;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;

@SuppressWarnings("unchecked")
public class BridgeHelperTest {
    private final String json = Tests.unescapeJson("{'contentLength':10000,"+
            "'status':'succeeded','requestedOn':'2016-07-26T22:43:10.392Z',"+
            "'completedOn':'2016-07-26T22:43:10.468Z','completedBy':'s3_worker',"+
            "'uploadDate':'2016-10-10','uploadId':'DEF','validationMessageList':"+
            "['message 1','message 2'],'schemaId':'schemaId','schemaRevision':2,'type':'Upload'}");

    private static final SignIn TEST_SIGN_IN = new SignIn();
    private static final String TEST_STUDY_ID = "api";
    private static final String TEST_REPORT_ID = "test-report";
    private static final DateTime TEST_START_DATETIME = new DateTime();
    private static final DateTime TEST_END_DATETIME = new DateTime();

    private static final Map<String, String> TEST_REPORT_DATA = ImmutableMap.of("field1", "test");
    private static final ReportData TEST_REPORT = new ReportData().date(TEST_START_DATETIME.toLocalDate())
            .data(TEST_REPORT_DATA);

    private static final Study TEST_STUDY_SUMMARY = new Study().identifier(TEST_STUDY_ID).name(TEST_STUDY_ID);

    private Upload testUpload;

    @BeforeClass
    public void setup() throws IOException {
        testUpload = RestUtils.GSON.fromJson(json, Upload.class);
    }

    @Test
    public void testGetAllStudiesSummary() throws Exception {
        // mock SDK get studies call
        StudyList studySummaryList = new StudyList().addItemsItem(TEST_STUDY_SUMMARY);
        Response<StudyList> response = Response.success(studySummaryList);

        Call<StudyList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        StudiesApi mockStudyClient = mock(StudiesApi.class);
        when(mockStudyClient.getStudies(true)).thenReturn(mockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(StudiesApi.class)).thenReturn(mockStudyClient);

        // set up BridgeHelper
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        List<Study> retSummaryList = bridgeHelper.getAllStudiesSummary();
        assertEquals(retSummaryList, ImmutableList.of(TEST_STUDY_SUMMARY));
    }

    @Test
    public void testGetUploadsForStudy() throws Exception {
        // mock SDK get uploads call
        UploadList uploadList = new UploadList().addItemsItem(testUpload);
        Response<UploadList> response = Response.success(uploadList);

        Call<UploadList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);

        ForWorkersApi mockWorkerClient = mock(ForWorkersApi.class);
        when(mockWorkerClient.getUploadsInStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME, MAX_PAGE_SIZE, null)).thenReturn(
                mockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        // set up BridgeHelper
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        List<Upload> retUploadsForStudy = bridgeHelper.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME,
                TEST_END_DATETIME);
        assertEquals(retUploadsForStudy, ImmutableList.of(testUpload));
    }

    @Test
    public void testGetUploadsForStudyPaginated() throws Exception {
        // mock SDK get uploads call
        UploadList uploadList = new UploadList().addItemsItem(testUpload);
        uploadList.setOffsetKey("offsetKey");
        Response<UploadList> response = Response.success(uploadList);
        UploadList secondUploadList = new UploadList().addItemsItem(testUpload);
        Response<UploadList> secondResponse = Response.success(secondUploadList);

        // return twice
        Call<UploadList> mockCall = mock(Call.class);
        when(mockCall.execute()).thenReturn(response);
        Call<UploadList> secondMockCall = mock(Call.class);
        when(secondMockCall.execute()).thenReturn(secondResponse);

        ForWorkersApi mockWorkerClient = mock(ForWorkersApi.class);
        when(mockWorkerClient.getUploadsInStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME, MAX_PAGE_SIZE, null)).thenReturn(
                mockCall);
        when(mockWorkerClient.getUploadsInStudy(TEST_STUDY_ID, TEST_START_DATETIME, TEST_END_DATETIME, MAX_PAGE_SIZE, "offsetKey")).thenReturn(
                secondMockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        // set up BridgeHelper
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        // execute
        List<Upload> retUploadsForStudy = bridgeHelper.getUploadsForStudy(TEST_STUDY_ID, TEST_START_DATETIME,
                TEST_END_DATETIME);

        // verify
        // called twice
        verify(mockWorkerClient, times(2)).getUploadsInStudy(any(), any(), any(), any(), any());
        // contain 2 test uploads
        assertEquals(retUploadsForStudy, ImmutableList.of(testUpload, testUpload));
    }

    @Test
    public void testSaveReportForStudy() throws Exception {
        // mock SDK save report call
        Call<Message> mockCall = mock(Call.class);
        ForWorkersApi mockWorkerClient = mock(ForWorkersApi.class);
        when(mockWorkerClient.saveReportForStudy(TEST_STUDY_ID, TEST_REPORT_ID, TEST_REPORT)).thenReturn(mockCall);

        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkerClient);

        // set up BridgeHelper
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        bridgeHelper.saveReportForStudy(TEST_STUDY_ID, TEST_REPORT_ID, TEST_REPORT);
        verify(mockCall).execute();
    }

    @Test
    public void testSessionHelper() throws Exception {
        // 3 test cases:
        // 1. first request initializes session
        // 2. second request re-uses same session
        // 3. third request gets 401'ed, refreshes session
        //
        // This necessitates 4 calls to our test server call (we'll use save report):
        // 1. Initial call succeeds.
        // 2. Second call also succeeds.
        // 3. Third call throws 401.
        // 4. Fourth call succeeds, to complete our call pattern.

        // Mock ForWorkersApi - third call throws
        Call<Message> mockReportCall1 = mock(Call.class);
        Call<Message> mockReportCall2 = mock(Call.class);
        Call<Message> mockReportCall3a = mock(Call.class);
        Call<Message> mockReportCall3b = mock(Call.class);
        when(mockReportCall3a.execute()).thenThrow(NotAuthenticatedException.class);

        ForWorkersApi mockWorkersApi = mock(ForWorkersApi.class);
        when(mockWorkersApi.saveReportForStudy(TEST_STUDY_ID, TEST_REPORT_ID, TEST_REPORT)).thenReturn(mockReportCall1,
                mockReportCall2, mockReportCall3a, mockReportCall3b);

        // Mock AuthenticationApi
        Call<UserSessionInfo> mockSignInCall = mock(Call.class);
        AuthenticationApi mockAuthApi = mock(AuthenticationApi.class);
        when(mockAuthApi.signIn(TEST_SIGN_IN)).thenReturn(mockSignInCall);

        // Mock Client Manager
        ClientManager mockClientManager = mock(ClientManager.class);
        when(mockClientManager.getClient(AuthenticationApi.class)).thenReturn(mockAuthApi);
        when(mockClientManager.getClient(ForWorkersApi.class)).thenReturn(mockWorkersApi);

        // Set up Bridge Helper
        BridgeHelper bridgeHelper = new BridgeHelper();
        bridgeHelper.setBridgeClientManager(mockClientManager);

        // execute
        for (int i = 0; i < 3; i++) {
            bridgeHelper.saveReportForStudy(TEST_STUDY_ID, TEST_REPORT_ID, TEST_REPORT);
        }

        // Validate behind the scenes, in order.
        InOrder inOrder = inOrder(mockReportCall1, mockReportCall2, mockReportCall3a, mockSignInCall,
                mockReportCall3b);
        inOrder.verify(mockReportCall1).execute();
        inOrder.verify(mockReportCall2).execute();
        inOrder.verify(mockReportCall3a).execute();
        inOrder.verify(mockSignInCall).execute();
        inOrder.verify(mockReportCall3b).execute();
    }
}
