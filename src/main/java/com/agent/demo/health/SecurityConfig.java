package com.agent.demo.health;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * 实现访问 Health 限制:
 * <p>
 * GET /actuator/health/**：匿名可访问
 * /actuator/info、/actuator/prometheus：需要 OPS 角色
 */
@Configuration
public class SecurityConfig {
    @Bean
    public SecurityWebFilterChain springSecurityFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(ex -> ex
                        // Actuator - probes open
                        .pathMatchers("/actuator/health/**").permitAll()
                        // Actuator - protected
                        .pathMatchers("/actuator/info", "/actuator/prometheus").hasRole("OPS")
                        // Everything else (你的业务端口 8080 + SSE)
                        .anyExchange().permitAll()
                )
                .httpBasic(Customizer.withDefaults())
                .build();
    }

    @Bean
    public MapReactiveUserDetailsService userDetailsService(
            @Value("${ops.user}") String user,
            @Value("${ops.password}") String password,
            PasswordEncoder encoder
    ) {
        UserDetails ops = User.builder()
                .username(user)
                .password(encoder.encode(password))
                .roles("OPS")
                .build();
        return new MapReactiveUserDetailsService(ops);
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}
