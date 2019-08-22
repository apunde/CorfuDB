package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Objects;

@Slf4j
public final class FileReceiver {
    private final int port;
    private final FileWriter fileWriter;

    FileReceiver(final int port, final FileWriter fileWriter) {
        this.port = port;
        this.fileWriter = fileWriter;
    }

    void receive() throws IOException {
        SocketChannel channel = null;
        ServerSocketChannel serverSocketChannel = null;
        try {
            serverSocketChannel = ServerSocketChannel.open();
            init(serverSocketChannel);

            log.info("Waiting to accept sockets...");
            channel = serverSocketChannel.accept();
            log.info("Accepted: " + channel.getRemoteAddress());
            doTransfer(channel);
        } finally {
            if (!Objects.isNull(channel)) {
                channel.close();
            }
            log.info("Closing writer and a socket");
            this.fileWriter.close();
            if(serverSocketChannel != null){
                serverSocketChannel.close();
            }
        }
    }

    private void doTransfer(final SocketChannel channel) throws IOException {
        assert !Objects.isNull(channel);

        this.fileWriter.transfer(channel);
    }

    private void init(final ServerSocketChannel serverSocketChannel) throws IOException {
        assert !Objects.isNull(serverSocketChannel);
        log.info("Opening a port on: " + this.port);
        serverSocketChannel.bind(new InetSocketAddress(this.port));
    }
}
