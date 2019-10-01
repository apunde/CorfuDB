package org.corfudb.common.metrics.servers;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PrometheusMetricsServerTest {
    private final static int METRICS_TEST_PORT = 9999;
    @Test
    void testConfigParseEmptyOpts() {
        Map<String, Object> map = new HashMap<>();
        PrometheusMetricsServer.Config config = PrometheusMetricsServer.Config.parse(map);
        assertFalse(config.isEnabled());
    }

    @Test
    void testConfigParseEnabled() {
        Map<String, Object> map = new HashMap<>();
        map.put(PrometheusMetricsServer.Config.METRICS_PORT_PARAM, METRICS_TEST_PORT);
        PrometheusMetricsServer.Config config = PrometheusMetricsServer.Config.parse(map);
        assertTrue(config.isEnabled());
    }
}
