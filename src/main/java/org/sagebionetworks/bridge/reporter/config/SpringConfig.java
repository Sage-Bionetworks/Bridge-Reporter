package org.sagebionetworks.bridge.reporter.config;

import org.sagebionetworks.bridge.heartbeat.HeartbeatLogger;
import org.sagebionetworks.bridge.sdk.ClientInfo;
import org.sagebionetworks.bridge.sdk.ClientProvider;
import org.sagebionetworks.bridge.sdk.models.accounts.SignInCredentials;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

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

    private Properties envConfig;

    @PostConstruct
    public void bridgeConfig() {
        // setup conf file to load attributes
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        Resource resource = new ClassPathResource(DEFAULT_CONFIG_FILE);

        try {
            envConfig = PropertiesLoaderUtils.loadProperties(resource);;

            if (Files.exists(localConfigPath)) {
                Properties localProps = new Properties();
                localProps.load(Files.newBufferedReader(localConfigPath, StandardCharsets.UTF_8));
                envConfig = localProps;
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Bean
    public SignInCredentials bridgeWorkerCredentials() {
        String study = envConfig.getProperty("bridge.worker.study");
        String email = envConfig.getProperty("bridge.worker.email");
        String password = envConfig.getProperty("bridge.worker.password");
        return new SignInCredentials(study, email, password);
    }

    @Bean
    public HeartbeatLogger heartbeatLogger() {
        HeartbeatLogger heartbeatLogger = new HeartbeatLogger();
        heartbeatLogger.setIntervalMinutes(Integer.parseInt(envConfig.getProperty("heartbeat.interval.minutes")));
        return heartbeatLogger;
    }
}
