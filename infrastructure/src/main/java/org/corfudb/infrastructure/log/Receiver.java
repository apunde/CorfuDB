package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class Receiver {
    public static void main(String[] args) {

        try {
            FileWriter fileWriter = new FileWriter("/tmp/1/log/0.log");
            FileReceiver fileReceiver = new FileReceiver(8888, fileWriter);
            fileReceiver.receive();
        } catch (IOException e) {
            log.error("Open socket error: ", e);
        }
    }
}
