package org.corfudb.runtime.view.stream;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;


/** The abstract queued stream view implements a stream backed by a read queue.
 *
 * <p>A read queue is a priority queue where addresses can be inserted, and are
 * dequeued in ascending order. Subclasses implement the fillReadQueue()
 * function, which defines how the read queue should be filled, and the
 * read() function, which reads an entry and updates the pointers for the
 * stream view.
 *
 * <p>The addresses in the read queue must be global addresses.
 *
 * <p>This implementation does not handle bulk reads and depends on IStreamView's
 * implementation of remainingUpTo(), which simply calls nextUpTo() under a lock
 * until it returns null.
 *
 * <p>Created by mwei on 1/6/17.
 */
@Slf4j
public abstract class AbstractQueuedStreamView implements IStreamView, AutoCloseable {

    /**
     * The ID of the stream.
     */
    @Getter
    private final UUID id;

    /**
     * The runtime the stream view was created with.
     */
    @Getter
    private final CorfuRuntime runtime;

    // Context of this stream
    @Getter
    private final StreamContext context;

    final StreamOptions options;

    /** Create a new queued stream view.
     *
     * @param streamId  The ID of the stream
     * @param runtime   The runtime used to create this view.
     */
    public AbstractQueuedStreamView(final CorfuRuntime runtime, final UUID streamId,
                                    StreamOptions options) {
        this.runtime = runtime;
        this.id = streamId;
        this.context = new StreamContext(id, Address.MAX);
        this.options = options;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        context.reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(long globalAddress) {
        // now request a seek on the context
        this.context.seek(globalAddress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ILogData nextUpTo(final long maxGlobal) {
        // Don't do anything if we've already exceeded the global pointer.
        if (context.getGlobalPointer() > maxGlobal) {
            return null;
        }

        // Get the next entry from the underlying implementation.
        final ILogData entry = getNextEntry(getContext(), maxGlobal);

        if (entry != null) {
            // Update the pointer.
            updatePointer(entry);
        }

        // Return the entry.
        return entry;
    }

    /** {@inheritDoc}
     */
    @Override
    public final List<ILogData> remainingUpTo(long maxGlobal) {
        final List<ILogData> entries = getNextEntries(getContext(), maxGlobal);

        // Nothing read, nothing to process.
        if (entries.size() == 0) {
            // We've resolved up to maxGlobal, so remember it. (if it wasn't max)
            if (maxGlobal != Address.MAX) {
                // Set Global Pointer and check that it is not pointing to an address in the trimmed space.
                getContext().setGlobalPointerCheckGCTrimMark(maxGlobal);
            }
            return entries;
        }

        // Otherwise update the pointer
        if (maxGlobal != Address.MAX) {
            // Set Global Pointer and check that it is not pointing to an address in the trimmed space.
            getContext().setGlobalPointerCheckGCTrimMark(maxGlobal);
        } else {
            // Update pointer from log data and then validate final position of the pointer against GC trim mark.
            updatePointer(entries.get(entries.size() - 1));
            getContext().validateGlobalPointerPosition(getCurrentGlobalPosition());
        }

        // And return the entries.
        return entries;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return getHasNext(getContext());
    }


    /** Retrieve the next entry in the stream, given the context.
     *
     * @param context       The context to retrieve the next entry from.
     * @param maxGlobal     The maximum global address to read to.
     * @return              Next ILogData for this context
     */


    /** Retrieve the next entries in the stream, given the context.
     *
     * <p>This function is designed to implement a bulk read. In a bulk read,
     * one of the entries may cause the context to change - the implementation
     * should check if the entry changes the context and stop reading
     * if this occurs, returning the entry that caused contextCheckFn to return
     * true.
     *
     * <p>The default implementation simply calls getNextEntry.
     *
     * @param context           The context to retrieve the next entry from.
     * @param maxGlobal         The maximum global address to read to.
     * @return                  A list of the next entries for this context
     */
    protected List<ILogData> getNextEntries(StreamContext context, long maxGlobal) {
        final List<ILogData> dataList = new ArrayList<>();
        ILogData thisData;

        while ((thisData = getNextEntry(context, maxGlobal)) != null) {
            // Add this read to the list of reads to return.
            dataList.add(thisData);

            // Update the pointer, because the underlying implementation
            // will expect it to be updated when we call getNextEntry() again.
            updatePointer(thisData);
        }

        return dataList;
    }

    /** Update the global pointer, given an entry.
     *
     * @param data  The entry to use to update the pointer.
     */
    private void updatePointer(final ILogData data) {
        // Update the global pointer, if it is non-checkpoint data.
        if (data.getType() == DataType.DATA && !data.hasCheckpointMetadata()) {
            // Note: here we only set the global pointer and do not validate its position with respect to the trim mark,
            // as the pointer is expected to be moving step by step (for instance when syncing a stream up to maxGlobal)
            // The validation is deferred to these methods which call it in advance based on the expected final position
            // of the pointer.
            getContext().setGlobalPointer(data.getGlobalAddress());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {}

    /** Add the given address to the resolved queue of the
     * given context.
     * @param context           The context to add the address to
     * @param globalAddress     The resolved global address.
     */
    protected void addToResolvedQueue(StreamContext context,
                                      long globalAddress,
                                      ILogData ld) {
        context.resolvedQueue.add(globalAddress);

        if (context.maxResolution < globalAddress) {
            context.maxResolution = globalAddress;
        }
    }

     /** Retrieve the next entry in the stream, given the context.
     *
     * @param context       The context to retrieve the next entry from.
     * @param maxGlobal     The maximum global address to read to.
     * @return              Next ILogData for this context
     */
    private ILogData getNextEntry(StreamContext context,
                                    long maxGlobal) {
        // If we have no entries to read, fill the read queue.
        // Return if the queue is still empty.
        if (context.readQueue.isEmpty() && context.readCpQueue.isEmpty()
                && !fillReadQueue(maxGlobal, context)) {
            return null;
        }

        // If maxGlobal is before the checkpoint position, throw a
        // trimmed exception
        if (maxGlobal < context.checkpoint.startAddress) {
            throw new TrimmedException();
        }

        // If checkpoint data is available, get from readCpQueue first
        NavigableSet<Long> getFrom;
        if (context.readCpQueue.size() > 0) {
            getFrom = context.readCpQueue;
            // Note: this is a checkpoint, we do not need to verify it is before the trim mark, it actually should be
            // cause this is the last address of the trimmed range.
            context.setGlobalPointer(context.checkpoint.startAddress);
        } else {
            getFrom = context.readQueue;
        }

        // If the lowest DATA element is greater than maxGlobal, there's nothing
        // to return.
        if (context.readCpQueue.isEmpty() && context.readQueue.first() > maxGlobal) {
            return null;
        }

        return removeFromQueue(getFrom);
    }

    /**
     * Remove next entry from the queue.
     *
     * @param queue queue of entries.
     * @return next available entry. or null if there are no more entries
     *         or remaining entries are not part of this stream.
     */
    protected abstract ILogData removeFromQueue(NavigableSet<Long> queue);

    @Override
    public void gc(long trimMark) {
        // GC stream only if the pointer is ahead from the trim mark (last untrimmed address),
        // this guarantees that the data to be discarded is already applied to the stream and data will not be lost.
        // Note: if the pointer is behind, discarding the data immediately will incur in data
        // loss as checkpoints are only loaded on resets. We don't want to trigger resets as this slows
        // the runtime.
        if (getContext().getGlobalPointer() >= getContext().getGcTrimMark()) {
            log.debug("gc[{}]: start GC on stream {} for trim mark {}", this, this.getId(),
                    getContext().getGcTrimMark());
            // Remove all the entries that are strictly less than
            // the trim mark
            getContext().readCpQueue.headSet(getContext().getGcTrimMark()).clear();
            getContext().readQueue.headSet(getContext().getGcTrimMark()).clear();
            getContext().resolvedQueue.headSet(getContext().getGcTrimMark()).clear();

            if (!getContext().resolvedQueue.isEmpty()) {
                getContext().minResolution = getContext()
                        .resolvedQueue.first();
            }
        } else {
            log.debug("gc[{}]: GC not performed on stream {} for this cycle. Global pointer {} is below trim mark {}",
                    this, this.getId(), getContext().getGlobalPointer(), getContext().getGcTrimMark());
        }

        // Set the trim mark for next GC Cycle
        getContext().setGcTrimMark(trimMark);
    }

    /**
     * {@inheritDoc}
     *
     * <p>We loop forever trying to
     * write, and automatically retrying if we get overwritten (hole filled).
     */
    @Override
    public long append(Object object,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        final LogData ld = new LogData(DataType.DATA, object);
        // Validate if the  size of the log data is under max write size.
        ld.checkMaxWriteSize(runtime.getParameters().getMaxWriteSize());

        // First, we get a token from the sequencer.
        TokenResponse tokenResponse = runtime.getSequencerView()
                .next(id);

        // We loop forever until we are interrupted, since we may have to
        // acquire an address several times until we are successful.
        for (int x = 0; x < runtime.getParameters().getWriteRetry(); x++) {
            // Next, we call the acquisitionCallback, if present, informing
            // the client of the token that we acquired.
            if (acquisitionCallback != null) {
                if (!acquisitionCallback.apply(tokenResponse)) {
                    // The client did not like our token, so we end here.
                    // We'll leave the hole to be filled by the client or
                    // someone else.
                    log.debug("Acquisition rejected token={}", tokenResponse);
                    return -1L;
                }
            }

            // Now, we do the actual write. We could get an overwrite
            // exception here - any other exception we should pass up
            // to the client.
            try {
                runtime.getAddressSpaceView().write(tokenResponse, ld);
                // The write completed successfully, so we return this
                // address to the client.
                return tokenResponse.getToken().getSequence();
            } catch (OverwriteException oe) {
                log.trace("Overwrite occurred at {}", tokenResponse);
                // We got overwritten, so we call the deacquisition callback
                // to inform the client we didn't get the address.
                if (deacquisitionCallback != null) {
                    if (!deacquisitionCallback.apply(tokenResponse)) {
                        log.debug("Deacquisition requested abort");
                        return -1L;
                    }
                }
                // Request a new token, informing the sequencer we were
                // overwritten.
                tokenResponse = runtime.getSequencerView().next(id);
            } catch (StaleTokenException te) {
                log.trace("Token grew stale occurred at {}", tokenResponse);
                if (deacquisitionCallback != null && !deacquisitionCallback.apply(tokenResponse)) {
                    log.debug("Deacquisition requested abort");
                    return -1L;
                }
                // Request a new token, informing the sequencer we were
                // overwritten.
                tokenResponse = runtime.getSequencerView().next(id);
            }
        }

        log.error("append[{}]: failed after {} retries, write size {} bytes",
                tokenResponse.getSequence(),
                runtime.getParameters().getWriteRetry(),
                ILogData.getSerializedSize(object));
        throw new AppendException();
    }

    /**
     * Reads data from an address in the address space.
     *
     * It will give the writer a chance to complete based on the time
     * when the reads of which this individual read is a step started.
     * If the reads have been going on for longer than the grace period
     * given for a writer to complete a write, the subsequent individual
     * read calls will immediately fill the hole on absence of data at
     * the given address.
     *
     * @param address       address to read.
     * @param readStartTime start time of the range of reads.
     * @return log data at the address.
     */
    protected ILogData read(final long address, long readStartTime) {
        try {
            if (System.currentTimeMillis() - readStartTime <
                    runtime.getParameters().getHoleFillTimeout().toMillis()) {
                return runtime.getAddressSpaceView().read(address);
            }
            return runtime.getAddressSpaceView()
                    .read(Collections.singleton(address), false, false)
                    .get(address);
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    @Nonnull
    protected List<ILogData> readAll(@Nonnull List<Long> addresses) {
        try {
            Map<Long, ILogData> dataMap =
                    runtime.getAddressSpaceView().read(addresses, options.ignoreTrimmed);
            // If trimmed exceptions are ignored, the data retrieved by the read API might not correspond
            // to all requested addresses, for this reason we must filter out data entries not included (null).
            // Also, we need to preserve ordering for checkpoint logic.
            return  addresses.stream().map(dataMap::get)
                    .filter(data -> data != null)
                    .collect(Collectors.toList());
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    private void processTrimmedException(TrimmedException te) {
        if (TransactionalContext.getCurrentContext() != null
                && TransactionalContext.getCurrentContext().getSnapshotTimestamp().getSequence()
                < getContext().checkpoint.snapshot) {
            te.setRetriable(false);
        }
    }

    /**
     * Fill the read queue for the current context. This method is called
     * whenever a client requests a read, but there are no addresses left in
     * the read queue.
     *
     * <p>This method returns true if entries were added to the read queue,
     * false otherwise.
     *
     * @param maxGlobal     The maximum global address to read to.
     * @param context       The current stream context.
     *
     * @return              True, if entries were added to the read queue,
     *                      False, otherwise.
     *
     * {@inheritDoc}
     */
    protected boolean fillReadQueue(final long maxGlobal,
                                    final StreamContext context) {
        log.trace("Fill_Read_Queue[{}] Max: {}, Current: {}, Resolved: {} - {}", this,
                maxGlobal, context.getGlobalPointer(), context.maxResolution, context.minResolution);
        log.trace("Fill_Read_Queue[{}]: addresses in this stream Resolved queue {}" +
                        " - ReadQueue {} - CP Queue {}", this,
                context.resolvedQueue, context.readQueue, context.readCpQueue);

        // If the stream has just been reset and we don't have
        // any checkpoint entries, we should consult
        // a checkpoint first.
        if (context.getGlobalPointer() == Address.NEVER_READ &&
                context.checkpoint.id == null) {
            // The checkpoint stream ID is the UUID appended with CP
            final UUID checkpointId = CorfuRuntime
                    .getCheckpointStreamIdFromId(context.id);
            // Find the checkpoint, if present
            try {
                if (discoverAddressSpace(checkpointId, context.readCpQueue,
                        runtime.getSequencerView()
                                .query(checkpointId).getToken().getSequence(),
                        Address.NEVER_READ, d -> scanCheckpointStream(context, d, maxGlobal),
                        true, maxGlobal)) {
                    log.trace("Fill_Read_Queue[{}] Get Stream Address Map using checkpoint with {} entries",
                            this, context.readCpQueue.size());

                    return true;
                }
            } catch (TrimmedException te) {
                log.warn("Fill_Read_Queue[{}] Trim encountered.", this, te);
                throw te;
            }
        }

        // The maximum address we will fill to.
        final long maxAddress =
                Long.min(maxGlobal, context.maxGlobalAddress);

        // If we already reached maxAddress ,
        // we return since there is nothing left to do.
        if (context.getGlobalPointer() >= maxAddress) {
            return false;
        }

        // If everything is available in the resolved
        // queue, use it
        if (context.maxResolution >= maxAddress
                && context.minResolution < context.getGlobalPointer()) {
            return fillFromResolved(maxGlobal, context);
        }

        Long latestTokenValue = null;

        // If the max has been resolved, use it.
        if (maxGlobal != Address.MAX) {
            latestTokenValue = context.resolvedQueue.ceiling(maxGlobal);
        }

        // If we don't have a larger token in resolved, or the request was for
        // a linearized read, fetch the token from the sequencer.
        if (latestTokenValue == null || maxGlobal == Address.MAX) {
            // The stream tail might be ahead of maxGlobal (our max timestamp to resolve up to)
            // We could limit it to the min between these two (maxGlobal and tail), but that could
            // lead to reading an address (maxGlobal) that does not belong to our stream and attempt
            // to deserialize it, or furthermore abort due to a trim on an address that does not even
            // belong to our stream.
            // For these reasons, we will keep this as the high boundary and prune
            // our discovered space of addresses up to maxGlobal.
            latestTokenValue = runtime.getSequencerView().query(context.id)
                    .getToken().getSequence();
            log.trace("Fill_Read_Queue[{}] Fetched tail {} from sequencer", this, latestTokenValue);
        }

        // If there is no information on the tail of the stream, return,
        // there is nothing to do
        if (Address.nonAddress(latestTokenValue)) {
            // sanity check:
            // currently, the only possible non-address return value for a token-query
            // is Address.NON_EXIST
            if (latestTokenValue != Address.NON_EXIST) {
                log.warn("TOKEN[{}] unexpected return value", latestTokenValue);
            }
            return false;
        }

        // If everything is available in the resolved
        // queue, use it
        if (context.maxResolution >= latestTokenValue
                && context.minResolution < context.getGlobalPointer()) {
            return fillFromResolved(latestTokenValue, context);
        }

        long stopAddress = Long.max(context.globalPointer, context.checkpoint.snapshot);

        // We check if we can fill partially from the resolved queue
        // This is a requirement for the getStreamAddressMaps as it considers the content of the resolved queue
        // to decide if needs to fetch the address maps or not.
        if (context.globalPointer < context.maxResolution) {
            if (fillFromResolved(context.maxResolution, context)) {
                stopAddress = context.maxResolution;
                log.trace("fillReadQueue[{}]: current pointer: {}, resolved up to: {}, readQueue: {}, " +
                                "new stop address: {}", this, context.globalPointer,
                        context.maxResolution, context.readQueue, stopAddress);
            }
        }

        // Now we fetch the address map for this stream from the sequencer in a single call, i.e.,
        // addresses of this stream in the range (stopAddress, startAddress==latestTokenValue]
        discoverAddressSpace(context.id, context.readQueue,
                latestTokenValue,
                stopAddress,
                d -> true, false, maxGlobal);

        return !context.readCpQueue.isEmpty() || !context.readQueue.isEmpty();
    }

    /**
     * Defines the strategy to discover addresses belonging to this stream.
     *
     * We currently support two mechanisms:
     *      - Following backpointers (@see org.corfudb.runtime.view.stream.BackpointerStreamView)
     *      - Requesting the sequencer for the complete address map of a stream.
     *      (@see org.corfudb.runtime.view.stream.AddressMapStreamView)
     *
     * @param streamId stream unique identifier.
     * @param queue queue to fill up.
     * @param startAddress read start address (inclusive)
     * @param stopAddress read stop address (exclusive)
     * @param filter filter to apply to data
     * @param checkpoint true if checkpoint discovery, false otherwise.
     * @param maxGlobal max address to resolve discovery.
     *
     * @return true if addresses were discovered, false, otherwise.
     */
    protected abstract boolean discoverAddressSpace(final UUID streamId,
                                                    final NavigableSet<Long> queue,
                                                    final long startAddress,
                                                    final long stopAddress,
                                                    final Function<ILogData, Boolean> filter,
                                                    final boolean checkpoint,
                                                    final long maxGlobal);

    protected boolean fillFromResolved(final long maxGlobal,
                                       final StreamContext context) {
        // There's nothing to read if we're already past maxGlobal.
        if (maxGlobal < context.getGlobalPointer()) {
            return false;
        }
        // Get the subset of the resolved queue, which starts at
        // globalPointer and ends at maxAddress inclusive.
        NavigableSet<Long> resolvedSet =
                context.resolvedQueue.subSet(context.getGlobalPointer(),
                        false, maxGlobal, true);

        // Put those elements in the read queue
        context.readQueue.addAll(resolvedSet);

        return !context.readQueue.isEmpty();
    }

    /**
     *  {@inheritDoc}
     *
     **/
    protected ILogData read(final long address) {
        try {
            return runtime.getAddressSpaceView().read(address);
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    /**
     *  This method reads a batch of addresses if 'nextRead' is not found in the cache.
     *  In the case of a cache miss, it piggybacks on the read for 'nextRead'.
     *
     *  If 'nextRead' is present in the cache, it directly returns this data.
     *
     * @param nextRead current address of interest
     * @param addresses batch of addresses to read (bring into the cache) in case there is a cache miss (includes
     *                  nextRead)
     * @return data for current 'address' of interest.
     */
    protected @Nonnull ILogData read(long nextRead, @Nonnull final NavigableSet<Long> addresses) {
        try {
            return runtime.getAddressSpaceView().read(nextRead, addresses, false);
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    /** Return whether calling getNextEntry() may return more
     * entries, given the context.
     * @param context       The context to retrieve the next entry from.
     * @return              True, if getNextEntry() may return an entry.
     *                      False otherwise.
     *
     * <p> We indicate we may have entries available
     * if the read queue contains entries to read -or-
     * if the next token is greater than our log pointer.
     */
    private boolean getHasNext(StreamContext context) {
        return  !context.readQueue.isEmpty()
                || runtime.getSequencerView().query(context.id).getToken().getSequence()
                > context.getGlobalPointer();
    }

    // Keeps the latest valid checkpoint (based on the snapshot it covers)
    private StreamCheckpoint latestValidCheckpoint = new StreamCheckpoint();

    /**
     * Resolve all potential checkpoints for the given max global.
     *
     * Note that, the position of checkpoint entries in the log does not correspond
     * to logical checkpoint ordering. For this reason, we must traverse all valid
     * checkpoints and only after scanning all, pick the checkpoint with the highest coverage.
     *
     * For instance, the fact that CP2.entriesGlobalAddress > CP1.entriesGlobalAddress, does not imply
     * that CP2.logicalCheckpointedSpace > CP1.logicalCheckpointedSpace.
     * Consider the case where, CP2 snapshot was taken before CP1, however, CP1 read/write is faster
     * and commits its entries to the log first.
     *
     * +------------------------------------------------+
     * | CP1 (snapshot 15) |  |  |  | CP2 (snapshot 10) |
     * +------------------------------------------------+
     *
     * @param context this stream's current context
     * @param data checkpoint log data entry
     * @param maxGlobal maximum global address to resolve this stream up to.
     *
     * @return true, if the checkpoint was completely resolved (from end to start markers of a checkpoint)
     *         false, otherwise.
     */
    protected boolean scanCheckpointStream(final StreamContext context, ILogData data,
                                           long maxGlobal) {
        if (data.hasCheckpointMetadata()) {
            CheckpointEntry cpEntry = (CheckpointEntry) data.getPayload(runtime);

            // Consider only checkpoints that are less than maxGlobal
            // Because we are traversing in reverse order END marker of a checkpoint should be found first.
            if (context.checkpoint.id == null &&
                    cpEntry.getCpType() == CheckpointEntry.CheckpointEntryType.END
                    && Long.decode(cpEntry.getDict()
                    .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)) <= maxGlobal) {
                log.trace("Checkpoint[{}] END found at address {} type {} id {} author {}",
                        this, data.getGlobalAddress(), cpEntry.getCpType(),
                        Utils.toReadableId(cpEntry.getCheckpointId()), cpEntry.getCheckpointAuthorId());

                // Because checkpoint ordering is not guaranteed, i.e., a checkpoint for a lower snapshot
                // could appear in the log after a checkpoint for a higher snapshot (case of multiple
                // checkpointers running in parallel). We need to inspect all checkpoints and keep the one
                // with the highest VLO version.
                UUID checkpointId = cpEntry.getCheckpointId();
                long checkpointVLOVersion = Long.decode(cpEntry.getDict()
                        .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS));
                boolean isCheckpointForHighestVLOVersion = latestValidCheckpoint.validateHigher(checkpointId,
                        checkpointVLOVersion);

                // If the entry being inspected represents a checkpoint for a higher VLO
                // take this as our latest valid checkpoint, and accumulate relevant info
                // (addresses, numEntries).
                if (isCheckpointForHighestVLOVersion) {
                    // We found a highest checkpoint, set this as our valid checkpoint.
                    latestValidCheckpoint = new StreamCheckpoint(checkpointId);
                    latestValidCheckpoint.setStartAddress(data.getCheckpointedStreamStartLogAddress());
                    latestValidCheckpoint.setNumEntries(1);
                    latestValidCheckpoint.setTotalBytes((long) data.getSizeEstimate());
                    latestValidCheckpoint.addAddress(data.getGlobalAddress());

                    if (cpEntry.getDict().get(CheckpointEntry.CheckpointDictKey
                            .SNAPSHOT_ADDRESS) != null) {
                        latestValidCheckpoint.setSnapshot(Long.decode(cpEntry.getDict()
                                .get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS)));
                    }
                }
            } else if (latestValidCheckpoint.getId() != null &&
                    latestValidCheckpoint.getId().equals(cpEntry.getCheckpointId())) {
                // Case: all other markers other than END of a checkpoint.

                // Add checkpoint entry data to the summarized state of the checkpoint, which will be used
                // when the definite checkpoint is selected.
                latestValidCheckpoint.addBytes((long) data.getSizeEstimate());
                latestValidCheckpoint.addNumEntries(1);
                latestValidCheckpoint.addAddress(data.getGlobalAddress());

                if (cpEntry.getCpType().equals(CheckpointEntry.CheckpointEntryType.START)) {
                    // Only for the case of START markers add some extra information.
                    log.trace("Checkpoint[{}] START found at address {} type {} id {} author {}",
                            this, data.getGlobalAddress(), cpEntry.getCpType(),
                            Utils.toReadableId(cpEntry.getCheckpointId()),
                            cpEntry.getCheckpointAuthorId());
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Resolves the valid checkpoint for the current view of the stream and returns
     * addresses belonging to this checkpoint.
     *
     * @param context current stream context.
     *
     * @return addresses for the valid checkpoint.
     */
    public List<Long> resolveCheckpoint(final StreamContext context) {

        List<Long> checkpointAddresses = new ArrayList<>();

        if (latestValidCheckpoint != null && latestValidCheckpoint.getId() != null) {
            // Select checkpoint with the highest start address
            log.trace("resolveCheckpoint[{}]: selecting checkpoint {} with start address {}", this,
                    latestValidCheckpoint.getId(), latestValidCheckpoint.getStartAddress());
            context.checkpoint = latestValidCheckpoint;
            checkpointAddresses.addAll(latestValidCheckpoint.getCheckpointAddresses());
        }

        // Checkpoint has been resolved, reset latest valid checkpoint.
        latestValidCheckpoint = new StreamCheckpoint();
        return checkpointAddresses;
    }

    /**
     * {@inheritDoc}
     * */
    @Override
    public ILogData previous() {
        final StreamContext context = getContext();
        final long oldPointer = context.getGlobalPointer();

        log.trace("previous[{}]: max={} min={}", this,
                context.maxResolution,
                context.minResolution);

        // If never read, there would be no pointer to the previous entry.
        if (context.getGlobalPointer() == Address.NEVER_READ) {
            return null;
        }

        // If we're attempt to go prior to most recent checkpoint, we
        // throw a TrimmedException.
        if (context.getGlobalPointer() - 1 < context.checkpoint.startAddress) {
            throw new TrimmedException();
        }

        // Otherwise, the previous entry should be resolved, so get
        // one less than the current.
        Long prevAddress = context
                .resolvedQueue.lower(context.getGlobalPointer());
        // If the pointer is before our min resolution, we need to resolve
        // to get the correct previous entry.
        if (prevAddress == null && Address.isAddress(context.minResolution)
                || prevAddress != null && prevAddress <= context.minResolution) {
            context.setGlobalPointerCheckGCTrimMark(prevAddress == null ? Address.NEVER_READ :
                    prevAddress - 1L);

            remainingUpTo(context.minResolution);
            context.minResolution = Address.NON_ADDRESS;
            context.setGlobalPointerCheckGCTrimMark(oldPointer);
            prevAddress = context
                    .resolvedQueue.lower(context.getGlobalPointer());
            log.trace("previous[{}]: updated resolved queue {}", this, context.resolvedQueue);
        }

        // Clear the read queue, it may no longer be valid
        context.readQueue.clear();

        if (prevAddress != null) {
            log.trace("previous[{}]: updated read queue {}", this, context.readQueue);
            context.setGlobalPointerCheckGCTrimMark(prevAddress);
            return read(prevAddress);
        }

        if (context.checkpoint.id == null) {
            // The stream hasn't been checkpointed and we need to
            // move the stream pointer to an address before the first
            // entry
            log.trace("previous[{}]: reached the beginning of the stream resetting" +
                    " the stream pointer to {}", this, Address.NON_ADDRESS);
            context.setGlobalPointerCheckGCTrimMark(Address.NON_ADDRESS);
            return null;
        }

        if (context.resolvedQueue.first() == context.getGlobalPointer()) {
            log.trace("previous[{}]: reached the beginning of the stream resetting" +
                    " the stream pointer to checkpoint version {}", this, context.checkpoint.startAddress);
            // Note: this is a checkpoint, we do not need to verify it is before the trim mark, it actually should be
            // cause this is the last address of the trimmed range.
            context.setGlobalPointer(context.checkpoint.startAddress);
            return null;
        }

        throw new IllegalStateException("The stream pointer seems to be corrupted!");
    }


    /**
    * {@inheritDoc}
    * */
    @Override
    public ILogData current() {
        final StreamContext context = getContext();

        if (Address.nonAddress(context.getGlobalPointer())) {
            return null;
        }
        return read(context.getGlobalPointer());
    }

    /**
     * {@inheritDoc}
     * */
    @Override
    public long getCurrentGlobalPosition() {
        return getContext().getGlobalPointer();
    }

    @Override
    public String toString() {
        return Utils.toReadableId(context.id) + "@" + getContext().getGlobalPointer();
    }

    /**
     * Represents a contained form of a stream's checkpoint.
     * Only with relevant information for the stream view.
     */
    @Data
    static class StreamCheckpoint {

        UUID id = null;
        // Represents the VLO version at the time of checkpoint, i.e., last update observed
        // on the checkpointed stream at the snapshot this checkpoint was taken.
        long startAddress = Address.NEVER_READ;
        // Total number of entries in this checkpoint
        long numEntries = 0L;
        // Total number of Bytes in this checkpoint
        long totalBytes = 0L;

        /** The address the current checkpoint snapshot was taken at.
         *  The checkpoint guarantees for this stream there are no entries
         *  between startAddress and snapshot.
         */
        long snapshot = Address.NEVER_READ;
        // List of addresses belonging to this checkpoint
        List<Long> checkpointAddresses = new ArrayList<>();

        /**
         * Create a new stream checkpoint to contain basic checkpoint information.
         */
        public StreamCheckpoint(UUID id) {
            this.id = id;
        }

        public StreamCheckpoint() {
        }

        public void addAddress(long address) {
            checkpointAddresses.add(address);
        }

        /**
         * Validates current checkpoint against the proposed and keeps the higher,
         * i.e., the latest checkpoint based on the snapshot it covers.
         *
         * @param id checkpoint id
         * @param startAddress checkpoint VLO version (last update observed for this stream at the time of checkpoint)
         * @return true, if this checkpoint if higher.
         *         false, otherwise.
         */
        public boolean validateHigher(UUID id, long startAddress) {
            if (Address.isAddress(startAddress) && startAddress > this.startAddress) {
                log.trace("validateHigher[{}]: valid checkpoint {} with start address {}", this, id, startAddress);
                return true;
            }

            return false;
        }

        public void reset() {
            id = null;
            startAddress = Address.NEVER_READ;
            snapshot = Address.NEVER_READ;
            numEntries = 0;
            totalBytes = 0;
            checkpointAddresses = new ArrayList<>();
        }

        public void addBytes(long bytes) {
            totalBytes += bytes;
        }

        public void addNumEntries(int entries) {
            numEntries += entries;
        }
    }
}
