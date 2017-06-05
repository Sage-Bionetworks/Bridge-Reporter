package org.sagebionetworks.bridge.reporter.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.AccountStatus;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * Generate a report of signups by account statuses.
 * @author alxdark
 *
 */
public class SignUpsReportGenerator implements ReportGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SignUpsReportGenerator.class);
    
    @Override
    public void generate(BridgeReporterRequest request, BridgeHelper bridgeHelper) throws IOException {
        LOG.info("Processing request as a sign up report.");
        
        DateTime startDate = request.getStartDateTime();
        DateTime endDate = request.getEndDateTime();
        String scheduler = request.getScheduler();
        ReportType scheduleType = request.getScheduleType();

        String reportId = scheduler + scheduleType.getSuffix();
        List<Study> studies = bridgeHelper.getAllStudiesSummary();

        for (Study study : studies) {
            LOG.info("Processing study '"+study.getIdentifier()+"'");
            String studyId = study.getIdentifier();

            Multiset<AccountStatus> statuses = HashMultiset.create();
            Multiset<SharingScope> sharingScopes = HashMultiset.create();

            List<StudyParticipant> participants = bridgeHelper.getParticipantsForStudy(studyId, startDate, endDate);
            for (StudyParticipant participant : participants) {
                statuses.add(participant.getStatus());
                // Accounts that aren't enabled do not have interesting sharing statuses. We'd like to count these
                // for consented accounts, but this isn't easy to do.
                if (participant.getStatus() == AccountStatus.ENABLED) {
                    sharingScopes.add(participant.getSharingScope());    
                }
            }
            
            Map<String, Integer> reportData = new HashMap<>();
            for (AccountStatus status : statuses) {
                reportData.put(status.name().toLowerCase(), statuses.count(status));
            }
            for (SharingScope scope : sharingScopes) {
                reportData.put(scope.name().toLowerCase(), sharingScopes.count(scope));
            }
            
            ReportData report = new ReportData().date(startDate.toLocalDate()).data(reportData);
            bridgeHelper.saveReportForStudy(studyId, reportId, report);

            LOG.info("Save signups report for hash[studyId]=" + studyId + ", scheduleType=" + scheduleType
                    + ", startDate=" + startDate + ",endDate=" + endDate + ", reportId=" + reportId
                    + ", reportData=" + reportData.toString());
        }        
    }
}
