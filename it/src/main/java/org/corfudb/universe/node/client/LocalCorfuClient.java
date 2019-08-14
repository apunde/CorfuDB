package org.corfudb.universe.node.client;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.reflect.TypeToken;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.LongevityApp;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ManagementView;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.universe.node.stress.Stress;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.corfudb.runtime.CorfuRuntime.fromParameters;

/**
 * Provides Corfu client (utility class) used in the local machine
 * (in current process) which is basically a wrapper of CorfuRuntime.
 */
@Slf4j
public class LocalCorfuClient implements CorfuClient {
    private final CorfuRuntime runtime;
    @Getter
    private final ClientParams params;
    @Getter
    private final ImmutableSortedSet<String> serverEndpoints;
    @Getter
    private final int resetNodeTimeOutSeconds = 30;
    @Getter
    private final int getLayoutTimeOutSeconds = 30;
    private IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException, Boolean, ExponentialBackoffRetry> x;

    @Builder
    public LocalCorfuClient(ClientParams params, ImmutableSortedSet<String> serverEndpoints) {
        this.params = params;
        this.serverEndpoints = serverEndpoints;

        List<NodeLocator> layoutServers = serverEndpoints.stream()
                .sorted()
                .map(NodeLocator::parseString)
                .collect(Collectors.toList());

        CorfuRuntimeParameters runtimeParams = CorfuRuntimeParameters
                .builder()
                .layoutServers(layoutServers)
                .systemDownHandler(this::systemDownHandler)
                .build();

        this.runtime = fromParameters(runtimeParams);
    }

    /**
     * Connect corfu runtime to the server
     *
     * @return
     */
    @Override
    public LocalCorfuClient deploy() {
        connect();
        return this;
    }

    /**
     * Shutdown corfu runtime
     *
     * @param timeout a limit within which the method attempts to gracefully stop the client (not used for a client).
     */
    @Override
    public void stop(Duration timeout) {
        runtime.shutdown();
    }

    /**
     * Shutdown corfu runtime
     */
    @Override
    public void kill() {
        runtime.shutdown();
    }

    /**
     * Shutdown corfu runtime
     */
    @Override
    public void destroy() {
        runtime.shutdown();
    }

    @Override
    public Stress getStress() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public <K, V> CorfuTable<K, V> createDefaultCorfuTable(String streamName) {
        return runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<K, V>>() {
                })
                .setStreamName(streamName)
                .open();
    }

    @Override
    public void connect() {
        runtime.connect();
    }

    @Override
    public CorfuRuntime getRuntime() {
        return runtime;
    }

    @Override
    public Layout getLayout() {
        return runtime.getLayoutView().getLayout();
    }

    public boolean resetNode(String endpoint) {
        try {
            return runtime.getLayoutView().getRuntimeLayout().getBaseClient(endpoint).reset().get(resetNodeTimeOutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Future interrupted: ", e);
        } catch (ExecutionException e) {
            log.error("Execution exception: ", e);
        } catch (TimeoutException e) {
            log.error("Timeout exception: ", e);
        }
        return false;
    }

    public void generateDataForLogUnitIfNeeded(String node, long sizeCapInBytes) {
        long currentLogUnitSize = CFUtils.getUninterruptibly(getRuntime()
                .getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(node).getLogSize());

        if (currentLogUnitSize < sizeCapInBytes) {
            log.info("Size cap is already reached.");
        } else {
            LongevityApp stressTester = new LongevityApp(30000L, 10, node, false);
            stressTester.runLongevityTestsForOneWorkloadForever(0);
            while (currentLogUnitSize <= sizeCapInBytes) {
                Sleep.sleepUninterruptibly(Duration.ofSeconds(5));
                currentLogUnitSize = CFUtils.getUninterruptibly(getRuntime()
                        .getLayoutView()
                        .getRuntimeLayout()
                        .getLogUnitClient(node).getLogSize());
            }
            stressTester.waitForAppToFinish();
            log.info("Created needed data of {} bytes", sizeCapInBytes);
        }
    }

    @Override
    public ObjectsView getObjectsView() {
        return runtime.getObjectsView();
    }

    @Override
    public ManagementView getManagementView() {
        return runtime.getManagementView();
    }

    @Override
    public void invalidateLayout() {
        runtime.invalidateLayout();
    }

    @Override
    public void shutdown() {
        runtime.shutdown();
    }
}
