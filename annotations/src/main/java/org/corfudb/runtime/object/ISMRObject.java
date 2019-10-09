package org.corfudb.runtime.object;


/**
 * All objects that need to be understood by Corfu have to implement this
 * interface, whose purpose is to facilitate a better integration between
 * different Corfu components and the underlying object.
 *
 * @param <T> The underlying object type
 */
public interface ISMRObject<T> extends ICorfuExecutionContext<T> {
}
