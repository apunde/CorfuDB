package org.corfudb.infrastructure.log;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import jdk.internal.util.xml.impl.Pair;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class LogStateTransfer {

    // prevent from creating instances
    private LogStateTransfer() {

    }

    /** Creates a map from servers to the address batches they are responsible for.
     *
     * @param addresses     The addresses of garbage or data entries.
     * @param bulkSize      The size of a batch, small enough to safely transfer within one rpc call.
     * @param runtimeLayout The current runtime layout to extract a server information.
     * @return A map from servers to list of address batches.
     */
    private static Map<String, List<List<Long>>> mapServersToBatches(List<Long> addresses,
                                                                     int bulkSize,
                                                                     RuntimeLayout runtimeLayout) {

        Map<String, List<Long>> serverToAddresses = addresses.stream().map(address -> {
            List<String> servers = runtimeLayout
                    .getLayout()
                    .getStripe(address)
                    .getLogServers();
            String logServer = servers.get(servers.size() - 1);
            return new SimpleEntry<>(logServer, address);
        }).collect(Collectors.groupingBy(SimpleEntry::getKey,
                Collectors.mapping(SimpleEntry::getValue, Collectors.toList())));

        return serverToAddresses
                .entrySet()
                .stream()
                .map(entry -> new SimpleEntry<>(entry.getKey(),
                        Lists.partition(entry.getValue(), bulkSize)))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    /**
     * Reads garbage from other log units in parallel.
     *
     * @param addresses     The addresses corresponding to the garbage entries.
     * @param bulkReadSize  The number of records to read within one RPC call.
     * @param runtimeLayout A runtime layout to use for connections.
     * @return A completable future of a list of garbage responses.
     */
    private static synchronized CompletableFuture<List<ReadResponse>> readGarbage(List<Long> addresses,
                                                                                  int bulkReadSize,
                                                                                  RuntimeLayout
                                                                                          runtimeLayout) {
        log.trace("Reading garbage for addresses: {}", addresses);
        Map<String, List<List<Long>>> serverToGarbageAddresses =
                mapServersToBatches(addresses, bulkReadSize, runtimeLayout);

        List<CompletableFuture<ReadResponse>> garbageResponses = serverToGarbageAddresses
                .entrySet()
                .stream()
                .flatMap(entry -> entry.getValue()
                        .stream()
                        .map(batch ->
                                runtimeLayout
                                        .getLogUnitClient(entry.getKey())
                                        .readGarbageEntries(batch)))
                .collect(Collectors.toList());
        return CFUtils.sequence(garbageResponses);
    }

    /**
     * Reads data entries by utilizing the replication protocol.
     *
     * @param addresses        The list of addresses.
     * @param bulkReadSize     The number of records to read within one RPC call.
     * @param runtimeLayout    A runtime layout to use for connections.
     * @param addressSpaceView A view of the address space to call a read via a protocol.
     * @param readOptions      The read options to use for the address space view.
     * @return A map from address to the LogData entry.
     */
    private static synchronized Map<Long, ILogData> readRecords(List<Long> addresses,
                                                                int bulkReadSize,
                                                                RuntimeLayout runtimeLayout,
                                                                AddressSpaceView addressSpaceView,
                                                                ReadOptions readOptions) {
        log.trace("Reading data entries: {}", addresses);
        Map<String, List<List<Long>>> serverToAddresses =
                mapServersToBatches(addresses, bulkReadSize, runtimeLayout);

        return serverToAddresses
                .values()
                .stream()
                .flatMap(batches ->
                        batches.stream().flatMap
                                (batch -> addressSpaceView
                                        .read(batch, readOptions)
                                        .entrySet()
                                        .stream()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * Write records to the current stream log.
     *
     * @param dataEntries The list of entries (data or garbage).
     * @param streamLog   The instance of the underlying stream log.
     */
    private static synchronized void writeRecords(List<LogData> dataEntries, StreamLog streamLog) {
        log.trace("Writing data entries: {}", dataEntries);
        streamLog.append(dataEntries);
    }
}
