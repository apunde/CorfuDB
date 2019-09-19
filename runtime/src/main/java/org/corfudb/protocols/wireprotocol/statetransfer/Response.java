package org.corfudb.protocols.wireprotocol.statetransfer;

/**
 * An interface that should be implemented by all the state transfer responses.
 */
public interface Response {

    /**
     * Returns the type of the response.
     *
     * @return type of the response
     */
    StateTransferResponseType getResponseType();

    /**
     * Serialize this response into a byte array.
     *
     * @return serialized bytes of the response
     */
    byte[] getSerialized();
}
