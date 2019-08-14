package org.corfudb.universe.node.server;

import org.corfudb.universe.node.NodeException;

import java.io.IOException;
import java.net.ServerSocket;

public class ServerUtil {

    private ServerUtil() {
        //prevent creating class instances
    }

    public static int getRandomOpenPort() {
        return getPortIfOpen(0);
    }

    public static int getPortIfOpen(int portNum) {
        try (ServerSocket socket = new ServerSocket(portNum)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new NodeException("Can't get any open port", e);
        }
    }
}
