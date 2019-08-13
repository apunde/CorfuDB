package org.corfudb.universe.scenario;

import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.junit.Test;

import java.time.Duration;

import static org.corfudb.universe.scenario.ScenarioUtils.waitUninterruptibly;

public class AddNodeLargeTransferIT extends GenericIntegrationTest {

    @Test(timeout = 30000)
    public void addNodeLargeTransfer() {
        getScenario().describe((fixture, testcase) -> {
            // CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());
            waitUninterruptibly(Duration.ofSeconds(3000));
        });
    }
}
