package org.corfudb.universe.scenario;


import org.corfudb.universe.GenericPersistentIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.docker.DockerCorfuCluster;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.client.LocalCorfuClient;

import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.docker.DockerCorfuServer;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.corfudb.universe.scenario.ScenarioUtils.waitUninterruptibly;

public class AddNodeLargeTransferIT extends GenericPersistentIntegrationTest {

    @Test(timeout = 3000000)
    public void addNodeLargeTransfer() {
        getScenario().describe((fixture, testcase) -> {
            DockerCorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());
            LocalCorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            String firstServerEndpoint = corfuCluster.getFirstServer().getEndpoint();
            // corfuClient.generateDataForLogUnitIfNeeded(firstServerEndpoint, 1024L * 1024L * 5000, 1);
            System.out.println("Tail: " + corfuClient.getTail(firstServerEndpoint));
//            CorfuServerParams secondServerParams = CorfuServerParams
//                    .serverParamsBuilder()
//                    .clusterName(corfuCluster.getParams().getName())
//                    .port(9001)
//                    .build();
//
//            CorfuServer node = corfuCluster.addNodeWithCustomParams(secondServerParams);
//            System.out.println(node.getParams().getName() + " " + "deployed");
//
//            String secondServerEndpoint = node.getEndpoint();
//
//            corfuClient.getRuntime().getManagementView()
//                    .addNode(secondServerEndpoint, 100, Duration.ofMinutes(20), Duration.ofMinutes(10));
//
//            System.out.println(node.getParams().getName() + " " + "added");
            waitUninterruptibly(Duration.ofSeconds(30000000));
        });
    }
}
