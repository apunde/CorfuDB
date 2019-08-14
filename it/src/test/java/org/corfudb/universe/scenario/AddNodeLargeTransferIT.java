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

    @Test(timeout = 300000)
    public void addNodeLargeTransfer() {
        getScenario().describe((fixture, testcase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());
            LocalCorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            String firstServerEndpoint = corfuCluster.getFirstServer().getEndpoint();

            RebootUtil.reset(firstServerEndpoint, corfuClient.getRuntime().getParameters(), 3, Duration.ofSeconds(3));
            try {
                corfuClient.waitUntilLayoutNoLongerBootstrapped(firstServerEndpoint);
            } catch (RuntimeException e) {
                e.printStackTrace();
            }
            System.out.println("Ok good");

            waitUninterruptibly(Duration.ofSeconds(3000));
        });
    }
}
