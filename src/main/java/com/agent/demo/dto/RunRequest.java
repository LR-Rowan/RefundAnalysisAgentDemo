package com.agent.demo.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * 基础 DTO
 */
public record RunRequest (
    @NotBlank String storeId,
    @NotBlank String query,
    @Min(1) @Max(366) Integer windowDays       // 可选：不传则使用 Controller 中默认值
){}
