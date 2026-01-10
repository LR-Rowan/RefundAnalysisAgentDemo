package com.agent.demo.agent.result;

public record ActionItem(
        String title,
        String owner,
        String priority,    // "P0" | "P1" | "P2"
        String eta,
        String howToVerify
) {}
