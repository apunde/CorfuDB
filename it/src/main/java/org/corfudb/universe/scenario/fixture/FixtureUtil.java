package org.corfudb.universe.scenario.fixture;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.CorfuServerParams.ContainerResources;
import org.corfudb.universe.node.server.CorfuServerParams.CorfuServerParamsBuilder;
import org.corfudb.universe.node.server.ServerUtil;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmCorfuServerParamsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Builder
public class FixtureUtil {

    @Default
    private final Optional<Integer> initialPort = Optional.empty();
    private Optional<Integer> currPort;

    ImmutableList<CorfuServerParams> buildServers(
            CorfuClusterParams cluster, CorfuServerParamsBuilder serverBuilder) {

        currPort = initialPort;

        List<CorfuServerParams> serversParams = IntStream
                .rangeClosed(1, cluster.getNumNodes())
                .map(i -> getPort())
                .boxed()
                .sorted()
                .map(port -> serverBuilder
                        .port(port)
                        .clusterName(cluster.getName())
                        .containerResources(Optional.of(ContainerResources.builder().build()))
                        .serverVersion(cluster.getServerVersion())
                        .build()
                )
                .collect(Collectors.toList());

        return ImmutableList.copyOf(serversParams);
    }

    ImmutableList<CorfuServerParams> buildVmServers(
            CorfuClusterParams cluster, VmCorfuServerParamsBuilder serverParamsBuilder,
            String vmNamePrefix) {

        currPort = initialPort;

        List<CorfuServerParams> serversParams = new ArrayList<>();

        for (int i = 0; i < cluster.getNumNodes(); i++) {
            int port = getPort();

            VmCorfuServerParams serverParam = serverParamsBuilder
                    .clusterName(cluster.getName())
                    .vmName(vmNamePrefix + (i + 1))
                    .port(port)
                    .serverVersion(cluster.getServerVersion())
                    .build();

            serversParams.add(serverParam);
        }

        return ImmutableList.copyOf(serversParams);
    }

    private int getPort() {
        currPort = currPort.map(oldPort -> oldPort + 1);
        return currPort.orElseGet(ServerUtil::getRandomOpenPort);
    }
}
