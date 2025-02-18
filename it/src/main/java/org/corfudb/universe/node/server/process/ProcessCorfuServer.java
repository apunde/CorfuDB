package org.corfudb.universe.node.server.process;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.node.server.AbstractCorfuServer;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.universe.UniverseParams;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Implements a {@link CorfuServer} instance that is running on a host machine.
 */
@Slf4j
public class ProcessCorfuServer extends AbstractCorfuServer<CorfuServerParams, UniverseParams> {
    private static final String LOCALHOST = "127.0.0.1";

    @NonNull
    private final String ipAddress;

    @NonNull
    private final CorfuProcessManager processManager;

    private final ExecutionHelper commandHelper = ExecutionHelper.getInstance();

    @Builder
    public ProcessCorfuServer(
            @NonNull CorfuServerParams params, @NonNull UniverseParams universeParams) {
        super(params, universeParams);
        this.ipAddress = getIpAddress();

        Path corfuDir = Paths.get(System.getProperty("user.home"), "corfu");
        this.processManager = new CorfuProcessManager(corfuDir, params, getNetworkInterface());
    }

    /**
     * Deploys a Corfu server on the target directory as specified, including the following steps:
     * a) Copy the corfu jar file under the working directory to the target directory
     * b) Run that jar file using java on the local machine
     */
    @Override
    public CorfuServer deploy() {
        executeCommand(Optional.empty(), processManager.createServerDirCommand());
        executeCommand(Optional.empty(), processManager.createStreamLogDirCommand());

        commandHelper.copyFile(
                params.getInfrastructureJar(),
                processManager.getServerJar()
        );
        start();
        return this;
    }

    /**
     * Symmetrically disconnect the server from the cluster,
     * which creates a complete partition.
     */
    @Override
    public void disconnect() {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Symmetrically disconnect a server from a list of other servers,
     * which creates a partial partition.
     *
     * @param servers List of servers to disconnect from
     */
    @Override
    public void disconnect(List<CorfuServer> servers) {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Pause the {@link CorfuServer} process on the localhost
     */
    @Override
    public void pause() {
        log.info("Pausing the Corfu server: {}", params.getName());

        executeCommand(Optional.empty(), processManager.pauseCommand());
    }

    /**
     * Start a {@link CorfuServer} process on the localhost
     */
    @Override
    public void start() {
        executeCommand(
                Optional.of(processManager.getCorfuDir()),
                processManager.startCommand(getCommandLineParams())
        );
    }

    /**
     * Restart the {@link CorfuServer} process on the localhost
     */
    @Override
    public void restart() {
        stop(params.getStopTimeout());
        start();
    }

    /**
     * Reconnect a server to the cluster
     */
    @Override
    public void reconnect() {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Reconnect a server to a list of servers.
     */
    @Override
    public void reconnect(List<CorfuServer> servers) {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Resume a {@link CorfuServer}
     */
    @Override
    public void resume() {
        log.info("Resuming the corfu server: {}", params.getName());

        executeCommand(Optional.empty(), processManager.resumeCommand());
    }

    /**
     * Executes a certain command on the local machine.
     */
    private void executeCommand(Optional<Path> workDir, String cmdLine) {
        try {
            commandHelper.executeCommand(workDir, cmdLine);
        } catch (IOException e) {
            throw new NodeException("Execution error. Cmd: " + cmdLine, e);
        }
    }

    /**
     * @return the IpAddress of this local machine.
     */
    @Override
    public String getIpAddress() {
        return LOCALHOST;
    }

    /**
     * @param timeout a limit within which the method attempts to gracefully stop the {@link CorfuServer}.
     */
    @Override
    public void stop(Duration timeout) {
        log.info("Stop corfu server. Params: {}", params);

        try {
            executeCommand(Optional.empty(), processManager.stopCommand());
        } catch (Exception e) {
            String err = String.format("Can't STOP corfu: %s. Process not found", params.getName());
            throw new NodeException(err, e);
        }
    }

    /**
     * Kill the {@link CorfuServer} process on the local machine directly.
     */
    @Override
    public void kill() {
        log.info("Kill the corfu server. Params: {}", params);
        try {
            executeCommand(Optional.empty(), processManager.killCommand());
        } catch (Exception e) {
            String err = String.format("Can't KILL corfu: %s. Process not found, ip: %s",
                    params.getName(), ipAddress
            );
            throw new NodeException(err, e);
        }
    }

    /**
     * Destroy the {@link CorfuServer} by killing the process and removing the files
     *
     * @throws NodeException this exception will be thrown if the server can not be destroyed.
     */
    @Override
    public void destroy() {
        log.info("Destroy node: {}", params.getName());
        kill();
        try {
            removeAppDir();
        } catch (Exception e) {
            throw new NodeException("Can't clean corfu directories", e);
        }
    }

    /**
     * Remove corfu server application dir.
     * AppDir is a directory that contains corfu-infrastructure jar file and could have log files,
     * stream-log files and so on, whatever used by the application.
     */
    private void removeAppDir() {
        executeCommand(Optional.empty(), processManager.removeServerDirCommand());
    }

    @Override
    public String getNetworkInterface() {
        return ipAddress;
    }
}
