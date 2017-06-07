package org.sagebionetworks.bridge.reporter.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.AccountStatus;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * Generate a report of signups by account statuses.
 *
 */
public class SignUpsReportGenerator implements ReportGenerator {
    
    @Override
    public Report generate(BridgeReporterRequest request, Study study, BridgeHelper bridgeHelper) throws IOException {
        DateTime startDate = request.getStartDateTime();
        DateTime endDate = request.getEndDateTime();
        String scheduler = request.getScheduler();
        ReportType scheduleType = request.getScheduleType();

        String reportId = scheduler + scheduleType.getSuffix();
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
        
        Map<String, Integer> statusData = new HashMap<>();
        for (AccountStatus status : statuses) {
            statusData.put(status.name().toLowerCase(), statuses.count(status));
        }
        Map<String, Integer> sharingData = new HashMap<>();
        for (SharingScope scope : sharingScopes) {
            sharingData.put(scope.name().toLowerCase(), sharingScopes.count(scope));
        }
        Map<String, Map<String,Integer>> reportData = new HashMap<>();
        reportData.put("bySharing", sharingData);
        reportData.put("byStatus", statusData);

        return new Report.Builder().withStudyId(studyId).withReportId(reportId).withDate(startDate.toLocalDate())
                .withReportData(reportData).build();
    }
}
