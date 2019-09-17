package org.corfudb.infrastructure.log;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;

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

    /**
     * Reads garbage from other logunits in parallel.
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
        Map<String, List<Long>> serverToGarbageAddresses = new HashMap<>();

        List<List<Long>> batches = Lists.partition(addresses,
                bulkReadSize);

        for (List<Long> batch : batches) {
            for (long address : batch) {
                List<String> servers = runtimeLayout
                        .getLayout()
                        .getStripe(address)
                        .getLogServers();
                String logServer = servers.get(servers.size() - 1);
                List<Long> addressesPerServer =
                        serverToGarbageAddresses.computeIfAbsent(logServer, s -> new ArrayList<>());
                addressesPerServer.add(address);
            }
        }

        List<CompletableFuture<ReadResponse>> garbageResponses = serverToGarbageAddresses
                .entrySet()
                .stream()
                .map(entry -> runtimeLayout
                        .getLogUnitClient(entry.getKey())
                        .readGarbageEntries(entry.getValue()))
                .collect(Collectors.toList());
        return CFUtils.sequence(garbageResponses);
    }

    /**
     * Writes garbage entries to the current logunit.
     *
     * @param garbageEntries  The list of garbage entries.
     * @param bulkWriteSize   The number of records to write within one RPC call.
     * @param runtimeLayout   A runtime layout to use for connections.
     * @param currentEndpoint The current endpoint of the node.
     * @return A completable future of a list of write successes.
     */
    private static synchronized CompletableFuture<List<Boolean>> writeGarbage(List<LogData> garbageEntries,
                                                                              int bulkWriteSize,
                                                                              RuntimeLayout runtimeLayout,
                                                                              String currentEndpoint) {
        log.trace("Writing garbage entries: {}", garbageEntries);
        List<List<LogData>> batches = Lists.partition(garbageEntries, bulkWriteSize);
        List<CompletableFuture<Boolean>> writeResponses = batches.stream().map(batch -> runtimeLayout
                .getLogUnitClient(currentEndpoint)
                .writeGarbageEntries(batch))
                .collect(Collectors.toList());

        return CFUtils.sequence(writeResponses);
    }

    /**
     * Reads data entries through the protocol.
     * @param addresses The list of addresses.
     * @param bulkReadSize The number of records to read within one RPC call.
     * @param runtimeLayout A runtime layout to use for connections.
     * @param addressSpaceView A view of the address space to call a read via a protocol.
     * @param readOptions The read options to use for the address space view.
     * @return A map from address to the LogData entry.
     */
    private static synchronized Map<Long, ILogData> readRecords(List<Long> addresses,
                                                 int bulkReadSize,
                                                 RuntimeLayout runtimeLayout,
                                                 AddressSpaceView addressSpaceView,
                                                 ReadOptions readOptions) {
        log.trace("Reading data entries: {}", addresses);
        Map<String, List<Long>> serverToAddresses = new HashMap<>();
        List<List<Long>> batches = Lists.partition(addresses,
                bulkReadSize);

        for (List<Long> batch : batches) {
            for (long address : batch) {
                List<String> servers = runtimeLayout
                        .getLayout()
                        .getStripe(address)
                        .getLogServers();
                String logServer = servers.get(servers.size() - 1);
                List<Long> addressesPerServer =
                        serverToAddresses.computeIfAbsent(logServer, s -> new ArrayList<>());
                addressesPerServer.add(address);
            }
        }

        return serverToAddresses
                .values()
                .stream()
                .flatMap(addressBatch ->
                        addressSpaceView
                                .read(addressBatch, readOptions)
                                .entrySet()
                                .stream())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * Write records to the current logunit.
     *
     * @param addresses The list of data entries.
     * @param bulkWriteSize The number of records to write within one RPC call.
     * @param runtimeLayout A runtime layout to use for connections.
     * @param currentEndpoint The current endpoint of the node.
     * @return A completable future of a list of write successes.
     */
    private static synchronized CompletableFuture<List<Boolean>> writeRecords(List<LogData> addresses,
                                                                              int bulkWriteSize,
                                                                              RuntimeLayout runtimeLayout,
                                                                              String currentEndpoint){
        log.trace("Wring data entries: {}", addresses);
        List<List<LogData>> batches = Lists.partition(addresses, bulkWriteSize);
        List<CompletableFuture<Boolean>> writeResponses = batches.stream().map(batch -> runtimeLayout
                .getLogUnitClient(currentEndpoint)
                .writeRange(batch))
                .collect(Collectors.toList());
        return CFUtils.sequence(writeResponses);
    }
}
