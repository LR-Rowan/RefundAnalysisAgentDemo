package com.agent.demo.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * 基础 DTO
 */
public record RunRequest (
    @NotBlank String storeId,
    @NotBlank String query
){}
