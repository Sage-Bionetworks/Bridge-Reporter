package org.sagebionetworks.bridge.reporter.worker;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportScheduleName;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

/**
 * SQS callback. Called by the PollSqsWorker. This handles a reporting request.
 */
@Component
public class BridgeReporterProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeReporterProcessor.class);
    
    private static final ReportGenerator UPLOADS_GENERATOR = new UploadsReportGenerator();
    private static final Map<ReportScheduleName, ReportGenerator> GENERATORS = new ImmutableMap.Builder<ReportScheduleName, ReportGenerator>()
            .put(ReportScheduleName.DAILY, UPLOADS_GENERATOR)
            .put(ReportScheduleName.WEEKLY, UPLOADS_GENERATOR)
            .put(ReportScheduleName.DAILY_SIGNUPS, new SignUpsReportGenerator())
            .build();

    private BridgeHelper bridgeHelper;
    
    @Autowired
    @Qualifier("ReporterHelper")
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Process the passed sqs msg as JsonNode. */
    public void process(JsonNode body) throws IOException, PollSqsWorkerBadRequestException, InterruptedException {
        BridgeReporterRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(body, BridgeReporterRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }

        DateTime startDateTime = request.getStartDateTime();
        DateTime endDateTime = request.getEndDateTime();
        String scheduler = request.getScheduler();
        ReportScheduleName scheduleType = request.getScheduleType();

        LOG.info("Received request for hash[scheduler]=" + scheduler + ", scheduleType=" + scheduleType + ", startDate=" +
                startDateTime + ",endDate=" + endDateTime);

        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            
            ReportGenerator generator = GENERATORS.get(scheduleType);
            generator.generate(request, bridgeHelper);
            
        } finally {
            LOG.info("Request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                    " seconds for hash[scheduler]=" + scheduler + ", scheduleType=" + scheduleType + ", startDate=" +
                    startDateTime + ",endDate=" + endDateTime);
        }
    }
}
