package io.tarantool.driver.core.connection;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.tarantool.driver.TarantoolVersionHolder;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.connection.TarantoolConnection;
import io.tarantool.driver.api.connection.TarantoolConnectionListener;
import io.tarantool.driver.api.connection.TarantoolConnectionListeners;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.core.TarantoolChannelInitializer;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manages instantiation and connection procedures for Tarantool server connections
 *
 * @author Alexey Kuzin
 */
public class TarantoolConnectionFactory {

    protected final TarantoolClientConfig config;
    protected final Bootstrap bootstrap;
    protected final ScheduledExecutorService timeoutScheduler;
    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    /**
     * Basic constructor.
     *
     * @param config           Tarantool client config
     * @param bootstrap        prepared Netty's bootstrap
     * @param timeoutScheduler scheduled executor for limiting the connection tasks by timeout
     */
    public TarantoolConnectionFactory(
        TarantoolClientConfig config,
        Bootstrap bootstrap,
        ScheduledExecutorService timeoutScheduler) {
        this.config = config;
        this.bootstrap = bootstrap;
        this.timeoutScheduler = timeoutScheduler;
    }

    /**
     * Create single connection and return connection future
     *
     * @param serverAddress       Tarantool server address to connect
     * @param connectionListeners listeners for the event of establishing the connection
     * @return connection future
     */
    public CompletableFuture<TarantoolConnection> singleConnection(
        InetSocketAddress serverAddress,
        TarantoolConnectionListeners connectionListeners) {
        CompletableFuture<Channel> connectionFuture = new CompletableFuture<>();
        RequestFutureManager requestManager = new RequestFutureManager(config, timeoutScheduler);
        TarantoolVersionHolder versionHolder = new TarantoolVersionHolder();
        TarantoolChannelInitializer handler = new TarantoolChannelInitializer(
            config, requestManager, versionHolder, connectionFuture);

        ChannelFuture future = bootstrap.clone()
            .handler(handler)
            .remoteAddress(serverAddress)
            .connect();

        future.addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                connectionFuture.completeExceptionally(new TarantoolClientException(
                    String.format("Failed to connect to the Tarantool server at %s", serverAddress), f.cause()));
            }
        });

        timeoutScheduler.schedule(() -> {
            if (!connectionFuture.isDone()) {
                connectionFuture.completeExceptionally(new TimeoutException(
                    String.format("Failed to connect to the Tarantool server at %s within %d ms",
                        serverAddress, config.getConnectTimeout())));
            }
        }, config.getConnectTimeout(), TimeUnit.MILLISECONDS);

        CompletableFuture<TarantoolConnection> result = connectionFuture
            .thenApply(ch -> new TarantoolConnectionImpl(requestManager, versionHolder, ch));

        for (TarantoolConnectionListener listener : connectionListeners.all()) {
            result = result.thenCompose(listener::onConnection);
        }

        return result.handle((connection, ex) -> {
            if (ex != null) {
                logger.warn("Connection failed: {}", ex.getMessage());
            }
            return connection;
        });
    }

    /**
     * Create several connections and return their futures
     *
     * @param serverAddress       Tarantool server address to connect
     * @param connections         number of connections to create
     * @param connectionListeners listeners for the event of establishing the connection
     * @return a collection with specified number of connection futures
     */
    public Collection<CompletableFuture<TarantoolConnection>> multiConnection(
        InetSocketAddress serverAddress,
        int connections,
        TarantoolConnectionListeners connectionListeners) {
        return Stream.generate(() -> serverAddress)
            .map(address -> singleConnection(address, connectionListeners))
            .limit(connections)
            .collect(Collectors.toList());
    }
}
