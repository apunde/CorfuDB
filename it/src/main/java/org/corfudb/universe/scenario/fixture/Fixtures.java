package org.corfudb.universe.scenario.fixture;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.group.cluster.CorfuClusterParams.CorfuClusterParamsBuilder;
import org.corfudb.universe.group.cluster.SupportClusterParams;
import org.corfudb.universe.group.cluster.SupportClusterParams.SupportClusterParamsBuilder;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.ClientParams.ClientParamsBuilder;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.CorfuServerParams.CorfuServerParamsBuilder;
import org.corfudb.universe.node.server.SupportServerParams;
import org.corfudb.universe.node.server.SupportServerParams.SupportServerParamsBuilder;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmCorfuServerParamsBuilder;
import org.corfudb.universe.scenario.fixture.FixtureUtil.FixtureUtilBuilder;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.universe.UniverseParams.UniverseParamsBuilder;
import org.corfudb.universe.universe.vm.VmConfigFileUtil;
import org.corfudb.universe.universe.vm.VmUniverseParams;
import org.corfudb.universe.universe.vm.VmUniverseParams.VmUniverseParamsBuilder;
import org.slf4j.event.Level;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Fixture factory provides predefined fixtures
 */
public interface Fixtures {

    /**
     * Common constants used for test
     */
    class TestFixtureConst {

        private TestFixtureConst() {
            // prevent instantiation of this class
        }

        // Default name of the CorfuTable created by CorfuClient
        public static final String DEFAULT_STREAM_NAME = "stream";

        // Default number of values written into CorfuTable
        public static final int DEFAULT_TABLE_ITER = 100;

        // Default number of times to poll layout
        public static final int DEFAULT_WAIT_POLL_ITER = 300;

        // Default time to wait before next layout poll: 1 second
        public static final int DEFAULT_WAIT_TIME = 1;
    }

    @Getter
    class UniverseFixture implements Fixture<UniverseParams> {

        private final UniverseParamsBuilder universe = UniverseParams.universeBuilder();

        private final CorfuClusterParamsBuilder cluster = CorfuClusterParams.builder();

        private final CorfuServerParamsBuilder server = CorfuServerParams.serverParamsBuilder();

        private final SupportServerParamsBuilder supportServer = SupportServerParams.builder();

        private final SupportClusterParamsBuilder monitoringCluster = SupportClusterParams
                .builder();

        private final ClientParamsBuilder client = ClientParams.builder();

        private final FixtureUtilBuilder fixtureUtilBuilder = FixtureUtil.builder();

        private final LoggingParams.LoggingParamsBuilder logging = LoggingParams.builder()
                .enabled(false);

        private Optional<UniverseParams> data = Optional.empty();

        public UniverseParams data() {

            if (data.isPresent()) {
                return data.get();
            }

            UniverseParams universeParams = universe.build();
            CorfuClusterParams clusterParams = cluster.build();

            SupportClusterParams monitoringClusterParams = monitoringCluster.build();
            SupportServerParams monitoringServerParams = supportServer
                    .clusterName(monitoringClusterParams.getName())
                    .build();

            FixtureUtil fixtureUtil = fixtureUtilBuilder.build();
            List<CorfuServerParams> serversParams = fixtureUtil.buildServers(
                    clusterParams, server
            );

            serversParams.forEach(clusterParams::add);
            universeParams.add(clusterParams);

            if (monitoringServerParams.isEnabled()) {
                monitoringClusterParams.add(monitoringServerParams);
                universeParams.add(monitoringClusterParams);
            }

            data = Optional.of(universeParams);
            return universeParams;
        }
    }

    @Getter
    class VmUniverseFixture implements Fixture<VmUniverseParams> {
        private static final String DEFAULT_VM_PREFIX = "corfu-vm-";

        private final VmUniverseParamsBuilder universe;

        private final CorfuClusterParamsBuilder cluster = CorfuClusterParams.builder();

        private final VmCorfuServerParamsBuilder servers = VmCorfuServerParams.builder();

        private final ClientParamsBuilder client = ClientParams.builder();

        @Setter
        private String vmPrefix = DEFAULT_VM_PREFIX;

        private Optional<VmUniverseParams> data = Optional.empty();

        private final FixtureUtilBuilder fixtureUtilBuilder = FixtureUtil.builder();

        public VmUniverseFixture() {
            Properties credentials = VmConfigFileUtil.loadVmCredentialsProperties();
            Properties vmProperties = VmConfigFileUtil.loadVmProperties();

            servers
                    .logLevel(Level.INFO)
                    .mode(CorfuServer.Mode.CLUSTER)
                    .persistence(CorfuServer.Persistence.DISK)
                    .stopTimeout(Duration.ofSeconds(1))
                    .serverJarDirectory(Paths.get("target"))
                    .dockerImage(CorfuServerParams.DOCKER_IMAGE_NAME);

            universe = VmUniverseParams.builder()
                    .vSphereUrl(vmProperties.getProperty("vsphere.url"))
                    .networkName(vmProperties.getProperty("vm.network"))
                    .templateVMName("corfu-server")

                    .vmUserName(credentials.getProperty("vm.username"))
                    .vmPassword(credentials.getProperty("vm.password"))
                    .vSphereUsername(credentials.getProperty("vsphere.username"))
                    .vSpherePassword(credentials.getProperty("vsphere.password"))

                    .cleanUpEnabled(true);
        }

        @Override
        public VmUniverseParams data() {
            if (data.isPresent()) {
                return data.get();
            }

            CorfuClusterParams clusterParams = cluster.build();

            FixtureUtil fixtureUtil = fixtureUtilBuilder.build();
            ImmutableList<CorfuServerParams> serversParams = fixtureUtil.buildVmServers(
                    clusterParams, servers, vmPrefix
            );
            serversParams.forEach(clusterParams::add);

            ConcurrentMap<String, String> vmIpAddresses = new ConcurrentHashMap<>();
            for (int i = 0; i < clusterParams.getNumNodes(); i++) {
                vmIpAddresses.put(vmPrefix + (i + 1), "0.0.0.0");
            }

            VmUniverseParams universeParams = universe
                    .vmIpAddresses(vmIpAddresses)
                    .build();

            universeParams.add(clusterParams);

            data = Optional.of(universeParams);

            return universeParams;
        }
    }
}
