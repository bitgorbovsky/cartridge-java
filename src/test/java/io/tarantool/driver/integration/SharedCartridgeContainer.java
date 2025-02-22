package io.tarantool.driver.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

public abstract class SharedCartridgeContainer {

    private static final Logger logger = LoggerFactory.getLogger(SharedCartridgeContainer.class);

    protected static final TarantoolCartridgeContainer container =
        new TarantoolCartridgeContainer(
            "cartridge/instances.yml",
            "cartridge/topology.lua")
            .withDirectoryBinding("cartridge")
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .waitingFor(Wait.forLogMessage(".*Listening HTTP on.*", 4))
            .withStartupTimeout(Duration.ofMinutes(2));

    protected static void startCluster() {
        if (!container.isRunning()) {
            container.start();
        }
    }

    protected static void startCartridge() throws IOException, InterruptedException {
        container.execInContainer("cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "-d");
    }

    protected static void stopCartridge() throws IOException, InterruptedException {
        container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "--data-dir=/tmp/data");
    }

    protected static void startInstances(List<String> instancesName) throws IOException, InterruptedException {
        for (String instanceName : instancesName) {
            startInstance(instanceName);
        }
    }

    protected static void stopInstances(List<String> instancesName) throws IOException, InterruptedException {
        for (String instanceName : instancesName) {
            stopInstance(instanceName);
        }
    }

    protected static void startInstance(String instanceName) throws IOException, InterruptedException {
        container.execInContainer(
            "cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "-d", instanceName);
    }

    protected static void stopInstance(String instanceName) throws IOException, InterruptedException {
        container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "--data-dir=/tmp/data", instanceName);
    }
}
