package org.sagebionetworks.bridge.reporter.helper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.NotAuthenticatedException;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadList;

/**
 * Helper to call Bridge Server to get information such as schemas. Also wraps some of the calls to provide caching.
 */
@Component("ReporterHelper")
public class BridgeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeHelper.class);

    // match read capacity in ddb table
    static final long MAX_PAGE_SIZE = 30L;
    private static final long THREAD_SLEEP_INTERVAL = 1000L;

    private ClientManager bridgeClientManager;
    private SignIn bridgeCredentials;

    /** Bridge Client Manager, with credentials for Exporter account. This is used to refresh the session. */
    @Autowired
    public final void setBridgeClientManager(ClientManager bridgeClientManager) {
        this.bridgeClientManager = bridgeClientManager;
    }

    /** Bridge credentials, used by the session helper to refresh the session. */
    @Autowired
    public final void setBridgeCredentials(SignIn bridgeCredentials) {
        this.bridgeCredentials = bridgeCredentials;
    }

    /*
     * Helper method to get all studies summary as list from sdk
     */
    public List<Study> getAllStudiesSummary() throws IOException {
        return sessionHelper(() -> bridgeClientManager.getClient(StudiesApi.class).getStudies(true).execute().body()
                .getItems());
    }

    /*
     * Helper method to get all uploads for specified study and date range
     * Paginated results should be added in one list altogether
     */
    public List<Upload> getUploadsForStudy(String studyId, DateTime startDateTime, DateTime endDateTime)
            throws IOException, InterruptedException {

        List<Upload> retList = new ArrayList<>();
        String offsetKey = null;

        do {
            final String temOffsetKey = offsetKey;
            UploadList retBody = sessionHelper(() -> bridgeClientManager.getClient(ForWorkersApi.class).getUploadsInStudy(studyId,
                    startDateTime, endDateTime, MAX_PAGE_SIZE, temOffsetKey).execute().body());
            retList.addAll(retBody.getItems());
            offsetKey = retBody.getOffsetKey();
            // sleep a second
            Thread.sleep(THREAD_SLEEP_INTERVAL);
        } while (offsetKey != null);

        return retList;
    }

    /**
     * Helper method to save report for specified study with report id and report data
     */
    public void saveReportForStudy(String studyId, String reportId, ReportData reportData) throws IOException {
        sessionHelper(() -> bridgeClientManager.getClient(ForWorkersApi.class).saveReportForStudy(studyId, reportId,
                reportData).execute());
    }

    // Helper method, which wraps a Bridge Server call with logic for initializing and refreshing a session.
    private <T> T sessionHelper(BridgeCallable<T> callable) throws IOException {
        // First attempt. This should be enough for most cases.
        try {
            return callable.call();
        } catch (NotAuthenticatedException ex) {
            // Code readability reasons, the error handling will be done after the catch block instead of inside the
            // catch block.
        }

        // Refresh session and try again. This time, if the call fails, just let the exception bubble up.
        LOG.info("Bridge server session expired. Refreshing session...");
        bridgeClientManager.getClient(AuthenticationApi.class).signIn(bridgeCredentials).execute();

        return callable.call();
    }

    // Functional interface used to make lambdas for the session helper.
    @FunctionalInterface
    interface BridgeCallable<T> {
        T call() throws IOException;
    }
}
