package org.corfudb.universe.group.cluster.docker;

import com.google.common.collect.ImmutableSortedSet;
import com.spotify.docker.client.DockerClient;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.universe.group.cluster.AbstractCorfuCluster;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.docker.DockerCorfuServer;
import org.corfudb.universe.node.server.docker.DockerParams;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.util.DockerManager;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides Docker implementation of {@link CorfuCluster}.
 */
@Slf4j
public class DockerCorfuCluster extends AbstractCorfuCluster<CorfuClusterParams, UniverseParams> {
    @NonNull
    private final DockerClient docker;
    @NonNull
    private final LoggingParams loggingParams;

    private final DockerParams dockerParams;
    private final DockerManager dockerManager;

    @Builder
    public DockerCorfuCluster(DockerClient docker, CorfuClusterParams params, DockerParams dockerParams, UniverseParams universeParams,
                              LoggingParams loggingParams) {
        super(params, universeParams);
        this.docker = docker;
        this.dockerParams = dockerParams;
        this.loggingParams = loggingParams;
        this.dockerManager = DockerManager.builder().docker(docker).build();
    }

    @Override
    protected CorfuServer buildCorfuServer(CorfuServerParams nodeParams) {

        return DockerCorfuServer.builder()
                .universeParams(universeParams)
                .clusterParams(params)
                .params(nodeParams)
                .loggingParams(loggingParams)
                .docker(docker)
                .dockerParams(dockerParams)
                .dockerManager(dockerManager)
                .build();
    }

    @Override
    protected ImmutableSortedSet<String> getClusterLayoutServers() {
        List<String> servers = nodes
                .values()
                .stream()
                .map(CorfuServer::getEndpoint)
                .collect(Collectors.toList());

        return ImmutableSortedSet.copyOf(servers);
    }

    @Override
    public void bootstrap() {

        Layout layout = getLayout();
        try{
            BootstrapUtil.bootstrap(layout, params.getBootStrapRetries(), params.getRetryDuration());
        }
        catch(RuntimeException e){
            log.warn("Servers already bootstrapped. Proceeding.");
        }
    }

    private Layout getLayout() {
        long epoch = 0;
        UUID clusterId = UUID.randomUUID();
        List<String> servers = getClusterLayoutServers().asList();

        LayoutSegment segment = new LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,
                0L,
                -1L,
                Collections.singletonList(new Layout.LayoutStripe(servers))
        );
        return new Layout(servers, servers, Collections.singletonList(segment), epoch, clusterId);
    }
}
