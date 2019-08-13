package org.corfudb.universe.node.server.docker;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Builder.Default;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicInteger;


@Builder
@ToString
public class DockerParams {

    @Getter
    private final String[] dockerPorts;

    @Getter
    private final boolean privileged = true;

    @Getter
    private final String cmdLine;

    @Getter
    private final String imageName;

    @Getter
    private final String hostName;

    @Getter
    private final String volumeMapping;

    @Getter
    private AtomicInteger serverOrder = new AtomicInteger(0);

}
