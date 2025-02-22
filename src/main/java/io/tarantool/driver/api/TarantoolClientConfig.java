package io.tarantool.driver.api;

import io.netty.handler.ssl.SslContext;
import io.tarantool.driver.api.connection.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.api.connection.TarantoolConnectionSelectionStrategies;
import io.tarantool.driver.api.retry.RequestRetryPolicy;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.driver.utils.Assert;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class-container for {@link TarantoolClient} configuration.
 * <p>
 * It is recommended to use the {@link TarantoolClientConfig.Builder} for constructing the configuration
 *
 * @author Alexey Kuzin
 */
public class TarantoolClientConfig {

    private static final int DEFAULT_CONNECT_TIMEOUT = 1000; // milliseconds
    private static final int DEFAULT_READ_TIMEOUT = 1000; // milliseconds
    private static final int DEFAULT_REQUEST_TIMEOUT = 2000; // milliseconds
    private static final int DEFAULT_CONNECTIONS = 1;
    private static final int DEFAULT_CURSOR_BATCH_SIZE = 100;
    private static final int DEFAULT_EVENT_LOOP_THREADS_NUMBER = 0;

    private TarantoolCredentials credentials;
    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int readTimeout = DEFAULT_READ_TIMEOUT;
    private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private int connections = DEFAULT_CONNECTIONS;
    private int eventLoopThreadsNumber = DEFAULT_EVENT_LOOP_THREADS_NUMBER;
    private MessagePackMapper messagePackMapper =
        DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    private ConnectionSelectionStrategyFactory connectionSelectionStrategyFactory =
        TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory.INSTANCE;
    private final AtomicBoolean isSecure = new AtomicBoolean(false);
    private SslContext sslContext;

    /**
     * Basic constructor.
     */
    public TarantoolClientConfig() {
    }

    /**
     * Copy constructor.
     *
     * @param config whose internal fields will be copied
     */
    public TarantoolClientConfig(TarantoolClientConfig config) {
        this.connectionSelectionStrategyFactory = config.getConnectionSelectionStrategyFactory();
        this.messagePackMapper = config.getMessagePackMapper();
        this.connectTimeout = config.getConnectTimeout();
        this.requestTimeout = config.getRequestTimeout();
        this.credentials = config.getCredentials();
        this.readTimeout = config.getReadTimeout();
        this.connections = config.getConnections();
        this.isSecure.set(config.isSecure.get());
        this.sslContext = config.getSslContext();
        this.eventLoopThreadsNumber = config.getEventLoopThreadsNumber();
    }

    /**
     * Get Tarantool credentials
     *
     * @return configured Tarantool user credentials
     * @see TarantoolCredentials
     */
    public TarantoolCredentials getCredentials() {
        return credentials;
    }

    /**
     * Set Tarantool credentials store
     *
     * @param credentials Tarantool user credentials
     * @see TarantoolCredentials
     */
    public void setCredentials(TarantoolCredentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Get TCP connection timeout, in milliseconds
     *
     * @return a number
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Set TCP connection timeout, in milliseconds
     *
     * @param connectTimeout timeout for establishing a connection to Tarantool server
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Get request completion timeout, in milliseconds
     *
     * @return a number
     */
    public int getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Set request completion timeout, in milliseconds
     *
     * @param requestTimeout timeout for receiving the response for a request to Tarantool server
     */
    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    /**
     * Get socket read timeout, in milliseconds
     *
     * @return a number
     */
    public int getReadTimeout() {
        return readTimeout;
    }

    /**
     * Set socket read timeout, in milliseconds
     *
     * @param readTimeout timeout for reading data from a socket, in milliseconds
     */
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * Get number of connections to be established with the target server. Default value is 1
     *
     * @return number of server connections
     */
    public int getConnections() {
        return connections;
    }

    /**
     * Set number of connections to be established with the target server
     *
     * @param connections number of server connections
     */
    public void setConnections(int connections) {
        this.connections = connections;
    }

    /**
     * Set {@link SslContext} for establishing SSL/TLS connection
     *
     * @param sslContext {@link SslContext} instance
     */
    public void setSslContext(SslContext sslContext) {
        this.sslContext = sslContext;
        this.isSecure.set(true);
    }

    /**
     * Get settings for establishing SSL/TLS connection
     *
     * @return a {@link SslContext} instance
     */
    public SslContext getSslContext() {
        return this.sslContext;
    }

    /**
     * Gets a flag that determines client uses encryption for binary connections or not
     *
     * @return boolean flag
     */
    public boolean isSecure() {
        return this.isSecure.get();
    }

    /**
     * Turn on secure connection or turn off secure connection
     *
     * @param isSecure boolean flag
     */
    public void setSecure(boolean isSecure) {
        this.isSecure.set(isSecure);
    }

    /**
     * Get mapper between Java objects and MessagePack entities
     *
     * @return a {@link MessagePackMapper} instance
     */
    public MessagePackMapper getMessagePackMapper() {
        return messagePackMapper;
    }

    /**
     * Set mapper between Java objects and MessagePack entities
     *
     * @param messagePackMapper {@link MessagePackMapper} instance
     */
    public void setMessagePackMapper(MessagePackMapper messagePackMapper) {
        this.messagePackMapper = messagePackMapper;
    }

    /**
     * Get factory implementation for collection selection strategy instances
     *
     * @return connection selection strategy factory instance
     */
    public ConnectionSelectionStrategyFactory getConnectionSelectionStrategyFactory() {
        return connectionSelectionStrategyFactory;
    }

    /**
     * Set factory implementation for collection selection strategy instances, for example, an instance of
     * {@link TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory}
     *
     * @param connectionSelectionStrategyFactory connection selection strategy factory instance
     */
    public void setConnectionSelectionStrategyFactory(
        ConnectionSelectionStrategyFactory connectionSelectionStrategyFactory) {
        this.connectionSelectionStrategyFactory = connectionSelectionStrategyFactory;
    }

    /**
     * How many items will be fetched from server per cursor request.
     *
     * @return default size of a batch for a cursor.
     */
    public int getCursorBatchSize() {
        return DEFAULT_CURSOR_BATCH_SIZE;
    }

    /**
     * Create a builder instance.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get number of set up internal event loop threads.
     *
     * @return a positive integer value
     */
    public int getEventLoopThreadsNumber() {
        return eventLoopThreadsNumber;
    }

    /**
     * Set number of internal event loop threads.
     *
     * @param eventLoopThreadsNumber number of threads
     */
    public void setEventLoopThreadsNumber(int eventLoopThreadsNumber) {
        this.eventLoopThreadsNumber = eventLoopThreadsNumber;
    }

    /**
     * A builder for {@link TarantoolClientConfig}
     */
    public static final class Builder {

        private TarantoolClientConfig config;

        /**
         * Basic constructor.
         */
        public Builder() {
            config = new TarantoolClientConfig();
        }

        /**
         * Basic constructor.
         *
         * @param config client configuration
         */
        public Builder(TarantoolClientConfig config) {
            this.config = config;
        }

        /**
         * Specify user credentials
         *
         * @param credentials the Tarantool user credentials
         * @return builder
         * @see TarantoolClientConfig#setCredentials(TarantoolCredentials)
         */
        public Builder withCredentials(TarantoolCredentials credentials) {
            Assert.notNull(credentials, "Tarantool server credentials should not be null");
            config.setCredentials(credentials);
            return this;
        }

        /**
         * Specify response reading timeout. Default is 1000 milliseconds
         *
         * @param readTimeout the timeout for reading the responses from Tarantool server, in milliseconds
         * @return builder
         * @see TarantoolClientConfig#setReadTimeout(int)
         */
        public Builder withReadTimeout(int readTimeout) {
            Assert.state(readTimeout > 0, "Response reading timeout should be greater than 0");
            config.setReadTimeout(readTimeout);
            return this;
        }

        /**
         * Specify connection timeout. Default is 1000 milliseconds
         *
         * @param connectTimeout the timeout for connecting to the Tarantool server, in milliseconds
         * @return builder
         * @see TarantoolClientConfig#setConnectTimeout(int)
         */
        public Builder withConnectTimeout(int connectTimeout) {
            Assert.state(connectTimeout > 0, "Connection timeout should be greater than 0");
            config.setConnectTimeout(connectTimeout);
            return this;
        }

        /**
         * Specify request timeout. Default is 2000 milliseconds
         *
         * @param requestTimeout the timeout for receiving a response from the Tarantool server, in milliseconds
         * @return builder
         * @see TarantoolClientConfig#setRequestTimeout(int)
         */
        public Builder withRequestTimeout(int requestTimeout) {
            Assert.state(requestTimeout > 0, "Request timeout should be greater than 0");
            config.setRequestTimeout(requestTimeout);
            return this;
        }

        /**
         * Specify mapper between Java objects and MessagePack entities
         *
         * @param mapper configured {@link MessagePackMapper} instance
         * @return builder
         * @see TarantoolClientConfig#setMessagePackMapper(MessagePackMapper)
         */
        public Builder withMessagePackMapper(MessagePackMapper mapper) {
            Assert.notNull(mapper, "MessagePack mapper should not be null");
            config.setMessagePackMapper(mapper);
            return this;
        }

        /**
         * Specify the number of connections used for sending requests to the server. The default value is 1.
         * More connections may help if a request can stuck on the server side or if the request payloads are big.
         *
         * @param connections the number of connections
         * @return builder
         */
        public Builder withConnections(int connections) {
            Assert.state(connections > 0, "The number of server connections must be greater than 0");
            config.setConnections(connections);
            return this;
        }

        /**
         * Specify SslContext with settings for establishing SSL/TLS connection between Tarantool
         *
         * @param sslContext {@link SslContext} instance
         * @return builder
         */
        public Builder withSslContext(SslContext sslContext) {
            Assert.notNull(sslContext, "SslContext must not be null");
            config.setSslContext(sslContext);
            return this;
        }

        /**
         * Turn on secure connection or turn off secure connection
         * Works only for new connections
         *
         * @param isSecure boolean flag
         * @return builder
         */
        public Builder withSecure(boolean isSecure) {
            Assert.notNull(config.getSslContext(), "SslContext must not be null");
            config.setSecure(isSecure);
            return this;
        }

        /**
         * Set the implementation of a factory which instantiates a strategy instance providing the algorithm of
         * selecting the next connection from a connection pool for performing the next request
         *
         * @param factory connection selection strategy factory instance
         * @return builder
         */
        public Builder withConnectionSelectionStrategyFactory(ConnectionSelectionStrategyFactory factory) {
            Assert.notNull(factory, "Connection selection strategy factory must not be null");
            config.setConnectionSelectionStrategyFactory(factory);
            return this;
        }

        /**
         * Specify netty threads number. Default is 0, real value will set in netty background
         *
         * @param eventLoopThreadsNumber number of threads
         * @return builder
         */
        public Builder withEventLoopThreadsNumber(int eventLoopThreadsNumber) {
            Assert.state(eventLoopThreadsNumber > 0, "EventLoopThreadsNumber should be equals or greater than 0");
            config.setEventLoopThreadsNumber(eventLoopThreadsNumber);
            return this;
        }

        /**
         * Build a {@link TarantoolClientConfig} instance
         *
         * @return configured instance
         */
        public TarantoolClientConfig build() {
            if (config.getCredentials() == null) {
                config.setCredentials(new SimpleTarantoolCredentials());
            }

            return new TarantoolClientConfig(config);
        }

        /**
         * Prepare the builder for new configuration process
         *
         * @return the empty builder
         */
        public Builder clear() {
            config = new TarantoolClientConfig();
            return this;
        }
    }
}
