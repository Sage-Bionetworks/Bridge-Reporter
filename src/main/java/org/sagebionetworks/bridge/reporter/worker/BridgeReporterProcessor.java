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
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

/**
 * SQS callback. Called by the PollSqsWorker. This handles a reporting request.
 */
@Component
public class BridgeReporterProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeReporterProcessor.class);
    
    private static final ReportGenerator UPLOADS_GENERATOR = new UploadsReportGenerator();

    private static final Map<ReportType, ReportGenerator> GENERATORS = new ImmutableMap.Builder<ReportType, ReportGenerator>()
            .put(ReportType.DAILY, UPLOADS_GENERATOR)
            .put(ReportType.WEEKLY, UPLOADS_GENERATOR)
            .put(ReportType.DAILY_SIGNUPS, new SignUpsReportGenerator())
            .build();

    private BridgeHelper bridgeHelper;
    
    @Autowired
    @Qualifier("ReporterHelper")
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Process the passed sqs msg as JsonNode. */
    public void process(JsonNode body) throws IOException, PollSqsWorkerBadRequestException, InterruptedException {
        BridgeReporterRequest request = deserializeRequest(body);

        DateTime startDateTime = request.getStartDateTime();
        DateTime endDateTime = request.getEndDateTime();
        String scheduler = request.getScheduler();
        ReportType scheduleType = request.getScheduleType();

        LOG.info(String.format("Received request for hash[scheduler]=%s, scheduleType=%s, startDate=%s, endDate=%s", 
                scheduler, scheduleType, startDateTime, endDateTime));

        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            
            ReportGenerator generator = GENERATORS.get(scheduleType);
            generator.generate(request, bridgeHelper);
            
        } finally {
            LOG.info(String.format("Request took %s seconds for hash[scheduler]=%s, scheduleType=%s, startDate=%s, endDate=%s", 
                    requestStopwatch.elapsed(TimeUnit.SECONDS), scheduler, scheduleType, startDateTime, endDateTime));
        }
    }

    private BridgeReporterRequest deserializeRequest(JsonNode body) throws PollSqsWorkerBadRequestException {
        try {
            return DefaultObjectMapper.INSTANCE.treeToValue(body, BridgeReporterRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }
    }
}
