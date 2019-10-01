package org.corfudb.common.metrics;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.Optional;

@AllArgsConstructor
public enum MetricsLevel {
    DISABLED(0),
    MONITOR(1),
    BENCHMARK(2),
    VERBOSE(3);

    @Getter
    private final int val;
    public static final String METRICS_LEVEL_PARAM = "--metrics-level";

    public static MetricsLevel parse(Map<String, Object> opts) {
        String config = Optional.ofNullable(opts.get(METRICS_LEVEL_PARAM))
                .map(c -> (String)c)
                .orElse("DISABLED");
        if (config.equalsIgnoreCase("MONITOR")) {
            return MONITOR;
        } else if (config.equalsIgnoreCase("BENCHMARK")) {
            return BENCHMARK;
        } else if (config.equalsIgnoreCase("VERBOSE")) {
            return VERBOSE;
        } else {
            return DISABLED;
        }
    }
}
