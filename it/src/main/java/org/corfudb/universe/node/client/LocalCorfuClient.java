package org.corfudb.universe.node.client;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.corfudb.generator.LongevityApp;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.TokenResponse;
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
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    public long getCurrentLogUnitSize(String node) {
        try {
            return IRetry.build(IntervalRetry.class, RetryExhaustedException.class, () -> {
                try {
                    return CFUtils.getUninterruptibly(getRuntime()
                            .getLayoutView()
                            .getRuntimeLayout()
                            .getLogUnitClient(node).getLogSize(), NetworkException.class, TimeoutException.class);
                } catch (NetworkException | TimeoutException e) {
                    log.info("Router not ready yet, retrying...", e);
                    throw new RetryNeededException();
                } catch (Exception e) {
                    log.error("Some other exception", e);
                }
                return 0L;

            }).setOptions(policy -> policy.setRetryInterval(2000)).run();
        } catch (Exception e) {
            log.info("Interrupted");
        }
        log.info("Failed, returning 0");
        return 0L;
    }

    public long getTail(String node){
        try {
            return IRetry.build(IntervalRetry.class, RetryExhaustedException.class, () -> {
                try {
                    TailsResponse tailresp = CFUtils.getUninterruptibly(getRuntime()
                            .getLayoutView()
                            .getRuntimeLayout()
                            .getLogUnitClient(node).getLogTail(), NetworkException.class, TimeoutException.class);
                    return tailresp.getLogTail();
                } catch (NetworkException | TimeoutException e) {
                    log.info("Router not ready yet, retrying...", e);
                    throw new RetryNeededException();
                } catch (Exception e) {
                    log.error("Some other exception", e);
                }
                return 0L;

            }).setOptions(policy -> policy.setRetryInterval(2000)).run();
        } catch (Exception e) {
            log.info("Interrupted");
        }
        log.info("Failed, returning 0");
        return 0L;
    }

    // 1 mb string
    private String generateLargeString(Random rand) {
        int size = 1024 * 1024 * 2;
        char[] chars = new char[size];
        Arrays.fill(chars, (char) (rand.nextInt(26) + 'a'));
        return new String(chars);
    }

    // 100 byte string
    private String generateSmallString(Random rand) {
        int size = 100;
        char[] chars = new char[size];
        Arrays.fill(chars, (char) (rand.nextInt(26) + 'a'));
        return new String(chars);
    }

    private LogData generatePayload(ByteBuf buffer, UUID streamId, UUID clientId) {
        final LogData ld = new LogData(DataType.DATA, buffer);
        TokenResponse tokenResponse = runtime.getSequencerView()
                .next(streamId);

        ld.setId(clientId);
        ld.useToken(tokenResponse);
        return ld;
    }

    public void generateDataForLogUnitIfNeeded(String node, long sizeCapInBytes, int reqsPerSecond) {


        long currentLogUnitSize = getCurrentLogUnitSize(node);
        Random rand = new Random();
        ExecutorService ec = Executors.newSingleThreadExecutor();

        UUID streamId = CorfuRuntime.getStreamID("stream");
        UUID clientId = UUID.randomUUID();
        if (currentLogUnitSize >= sizeCapInBytes) {
            log.info("Current num/size of records reached.");
        } else {
            // RL not to kill a setup prematurely
            RateLimiter rateLimiter = RateLimiter.create(reqsPerSecond);

            ec.execute(() -> {
                while (true) {
                    rateLimiter.acquire(reqsPerSecond);
                    final LogData ld = generatePayload(Unpooled.wrappedBuffer(generateLargeString(rand).getBytes()), streamId, clientId);

                    getRuntime()
                            .getLayoutView()
                            .getRuntimeLayout()
                            .getLogUnitClient(node).write(ld);
                }
            });
            while (currentLogUnitSize <= sizeCapInBytes) {

                log.info("Current size is {} but {} is needed. Waiting.", currentLogUnitSize, sizeCapInBytes);
                Sleep.sleepUninterruptibly(Duration.ofMillis(3000));
                currentLogUnitSize = getCurrentLogUnitSize(node);
            }

            log.info("Created needed data of {} bytes for {}. Shutting down the executor.", sizeCapInBytes, node);
            ec.shutdownNow();
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
