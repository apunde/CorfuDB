package org.corfudb.universe.scenario;

import org.corfudb.runtime.RebootUtil;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.GenericPersistentIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.LocalCorfuClient;
import org.junit.Test;

import java.time.Duration;

import static org.corfudb.universe.scenario.ScenarioUtils.waitUninterruptibly;

public class AddNodeLargeTransferIT extends GenericPersistentIntegrationTest {

    @Test(timeout = 3000000)
    public void addNodeLargeTransfer() {
        getScenario().describe((fixture, testcase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());
            LocalCorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            String firstServerEndpoint = corfuCluster.getFirstServer().getEndpoint();
            // corfuClient.generateDataForLogUnitIfNeeded(firstServerEndpoint, 10000);
            waitUninterruptibly(Duration.ofSeconds(300000));
        });
    }
}
