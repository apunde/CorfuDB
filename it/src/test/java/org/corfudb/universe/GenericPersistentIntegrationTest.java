package org.corfudb.universe;

import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.server.docker.DockerParams;
import org.corfudb.universe.scenario.Scenario;
import org.corfudb.universe.scenario.fixture.Fixtures;
import java.util.stream.IntStream;

public abstract class GenericPersistentIntegrationTest extends GenericIntegrationTest {

    @Override
    public DockerParams getDockerParams() {
        return DockerParams.builder().volumeMapping("/tmp").build();
    }

    private Integer [] getPortList(int seed, int numNodes){
        return IntStream.range(seed, seed + numNodes).boxed().toArray(Integer[]::new);
    }

    @Override
    public Scenario getDockerScenario(int numNodes){
        String persistentClusterName = "abc";
        int seed = 9000;
        Integer [] ports = getPortList(seed, numNodes);

        Fixtures.PersistentUniverseFixture universeFixture = new Fixtures.PersistentUniverseFixture();
        universeFixture.setNumNodes(numNodes);
        universeFixture.setPorts(ports);
        universeFixture.setCorfuCluster(CorfuClusterParams.builder().name(persistentClusterName).build());
        universe = UNIVERSE_FACTORY.buildDockerUniverse(universeFixture.data(), docker,
                getDockerParams(), getDockerLoggingParams());
        universe.deploy();

        return Scenario.with(universeFixture);
    }

}
