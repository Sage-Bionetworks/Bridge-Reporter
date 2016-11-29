package org.sagebionetworks.bridge.reporter.helper;

import org.joda.time.DateTime;
import org.sagebionetworks.bridge.sdk.ClientProvider;
import org.sagebionetworks.bridge.sdk.Session;
import org.sagebionetworks.bridge.sdk.exceptions.NotAuthenticatedException;
import org.sagebionetworks.bridge.sdk.models.ResourceList;
import org.sagebionetworks.bridge.sdk.models.accounts.SignInCredentials;
import org.sagebionetworks.bridge.sdk.models.reports.ReportData;
import org.sagebionetworks.bridge.sdk.models.studies.StudySummary;
import org.sagebionetworks.bridge.sdk.models.upload.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Helper to call Bridge Server to get information such as schemas. Also wraps some of the calls to provide caching.
 */
@Component("ReporterHelper")
public class BridgeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeHelper.class);

    private SignInCredentials credentials;
    private Session session = null;

    /** Bridge credentials for the Exporter account. This needs to be saved in memory so we can refresh the session. */
    @Autowired
    final void setCredentials(SignInCredentials credentials) {
        this.credentials = credentials;
    }

    /*
     * Helper method to get all studies summary as list from sdk
     */
    public ResourceList<StudySummary> getAllStudiesSummary() {
        return sessionHelper(() -> session.getStudyClient().getAllStudiesSummary());
    }

    /*
     * Helper method to get all uploads for specified study and date range
     */
    public ResourceList<Upload> getUploadsForStudy(String studyId, DateTime startDateTime, DateTime endDateTime) {
        return sessionHelper(() -> session.getWorkerClient().getUploadsForStudy(studyId, startDateTime, endDateTime));
    }

    /**
     * Helper method to save report for specified study with report id and report data
     * @param studyId
     * @param reportId
     * @param reportData
     */
    public void saveReportForStudy(String studyId, String reportId, ReportData reportData) {
        sessionHelper(() -> {
            session.getWorkerClient().saveReportForStudy(studyId, reportId, reportData);
            return null;
        });
    }

    // Helper method, which wraps a Bridge Server call with logic for initializing and refreshing a session.
    private <T> T sessionHelper(BridgeCallable<T> callable) {
        // Init session if necessary.
        if (session == null) {
            session = signIn();
        }

        // First attempt. This should be enough for most cases.
        try {
            return callable.call();
        } catch (NotAuthenticatedException ex) {
            // Code readability reasons, the error handling will be done after the catch block instead of inside the
            // catch block.
        }

        // Refresh session and try again. This time, if the call fails, just let the exception bubble up.
        LOG.info("Bridge server session expired. Refreshing session...");
        session = signIn();
        return callable.call();
    }

    // Helper method to sign in to Bridge Server and get a session. This needs to be wrapped because the sign-in call
    // is static and therefore not mockable.
    // Package-scoped to facilitate unit tests.
    Session signIn() {
        return ClientProvider.signIn(credentials);
    }

    // Functional interface used to make lambdas for the session helper.
    @FunctionalInterface
    interface BridgeCallable<T> {
        T call();
    }
}
