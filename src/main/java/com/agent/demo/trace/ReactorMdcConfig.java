package com.agent.demo.trace;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ReactorMdcConfig {

    @PostConstruct
    public void init() {
        ReactorMdcHook.install();
    }

    @PreDestroy
    public void destroy() {
        ReactorMdcHook.reset();
    }
}
