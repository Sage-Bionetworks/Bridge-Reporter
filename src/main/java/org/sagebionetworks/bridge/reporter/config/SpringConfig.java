package org.sagebionetworks.bridge.reporter.config;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.heartbeat.HeartbeatLogger;
import org.sagebionetworks.bridge.sdk.ClientInfo;
import org.sagebionetworks.bridge.sdk.ClientProvider;
import org.sagebionetworks.bridge.sdk.models.accounts.SignInCredentials;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

// These configs get credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
// For EC2 instances, this happens transparently.
// See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
// http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more info.
@ComponentScan("org.sagebionetworks.bridge.reporter")
@Configuration("reporterConfig")
public class SpringConfig {
    private static final String CONFIG_FILE = "BridgeReporter.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    // ClientProvider needs to be statically configured.
    static {
        // set client info
        ClientInfo clientInfo = new ClientInfo.Builder().withAppName("BridgeReporter").withAppVersion(1).build();
        ClientProvider.setClientInfo(clientInfo);
    }

    @Bean(name = "reporterConfigProperties")
    public Config bridgeConfig() throws IOException {
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        try {
            if (Files.exists(localConfigPath)) {
                return new PropertiesConfig(DEFAULT_CONFIG_FILE, localConfigPath);
            } else {
                return new PropertiesConfig(DEFAULT_CONFIG_FILE);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Bean
    public SignInCredentials bridgeWorkerCredentials() throws IOException {
        String study = bridgeConfig().get("bridge.worker.study");
        String email = bridgeConfig().get("bridge.worker.email");
        String password = bridgeConfig().get("bridge.worker.password");
        return new SignInCredentials(study, email, password);
    }

    @Bean
    public HeartbeatLogger heartbeatLogger() throws IOException {
        HeartbeatLogger heartbeatLogger = new HeartbeatLogger();
        heartbeatLogger.setIntervalMinutes(bridgeConfig().getInt("heartbeat.interval.minutes"));
        return heartbeatLogger;
    }
}
