package org.sagebionetworks.bridge.reporter.helper;

import java.io.IOException;
import java.util.List;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.Upload;

/**
 * Helper to call Bridge Server to get information such as schemas. Also wraps some of the calls to provide caching.
 */
@Component("ReporterHelper")
public class BridgeHelper {
    private ClientManager bridgeClientManager;

    /** Bridge Client Manager, with credentials for Exporter account. This is used to refresh the session. */
    @Autowired
    public final void setBridgeClientManager(ClientManager bridgeClientManager) {
        this.bridgeClientManager = bridgeClientManager;
    }

    /*
     * Helper method to get all studies summary as list from sdk
     */
    public List<Study> getAllStudiesSummary() throws IOException {
        return bridgeClientManager.getClient(StudiesApi.class).getStudies(true).execute().body().getItems();
    }

    /*
     * Helper method to get all uploads for specified study and date range
     */
    public List<Upload> getUploadsForStudy(String studyId, DateTime startDateTime, DateTime endDateTime)
            throws IOException {
        return bridgeClientManager.getClient(ForWorkersApi.class).getUploadsInStudy(studyId, startDateTime,
                endDateTime).execute().body().getItems();
    }

    /**
     * Helper method to save report for specified study with report id and report data
     */
    public void saveReportForStudy(String studyId, String reportId, ReportData reportData) throws IOException {
        bridgeClientManager.getClient(ForWorkersApi.class).saveReportForStudy(studyId, reportId, reportData).execute();
    }
}
