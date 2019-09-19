package org.corfudb.protocols.wireprotocol.statetransfer;

/**
 * An interface that should be implemented by all the state transfer requests.
 */
public interface Request {

    /**
     * Returns the type of the request.
     *
     * @return type of request
     */
    StateTransferRequestType getRequestType();

    /**
     * Serialize this request into a byte array
     *
     * @return serialized bytes of the request
     */
    byte[] getSerialized();
}
