package com.agent.demo.infra;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties(AgentLimitsProperties.class)
public class AgentInfraConfig {

    @Bean
    public ConcurrencyLimiter concurrencyLimiter(AgentLimitsProperties props) {
        return new ConcurrencyLimiter(
                props.getGlobalMaxConcurrent(),
                props.getPerStoreMaxConcurrent(),
                props.getMaxStores(),
                Duration.ofMinutes(props.getStoreIdleTtlMinutes())
        );
    }
}
