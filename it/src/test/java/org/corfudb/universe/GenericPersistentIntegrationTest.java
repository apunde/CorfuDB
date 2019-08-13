package org.corfudb.universe;

import org.corfudb.universe.node.server.docker.DockerParams;

public abstract class GenericPersistentIntegrationTest extends GenericIntegrationTest {

    @Override
    public DockerParams getDockerParams() {
        return DockerParams.builder().volumeMapping("/tmp").build();
    }
}
