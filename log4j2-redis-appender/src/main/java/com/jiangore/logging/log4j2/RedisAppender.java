package com.jiangore.logging.log4j2;

import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.DefaultErrorHandler;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.*;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.net.Protocol;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.logging.log4j.util.Strings;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * <pre>
 *      applog-to-redis
 * </pre>
 *
 * @author tao
 * @version v1.0.0
 * @since 2022/5/26 16:07
 */
@Plugin(name = "RedisAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE,
        printObject = true)
public class RedisAppender implements Appender {

    private static final StatusLogger LOGGER = StatusLogger.getLogger();

    private final Configuration config;

    private final String name;

    private final String logPrefix;

    private final Layout<? extends Serializable> layout;

    private final String key;

    private final byte[] keyBytes;

    private final String host;

    private final int port;

    private final String password;

    private volatile State state;

    private volatile ErrorHandler errorHandler = new DefaultErrorHandler(this);

    private RedisAppender(Builder builder) {
        this.config = builder.config;
        this.name = builder.name;
        this.logPrefix = String.format("[RedisAppender{%s}]", builder.name);
        this.layout = builder.layout;
        this.key = builder.key;
        this.keyBytes = builder.key.getBytes(builder.charset);
        this.host = builder.host;
        this.port = builder.port;
        this.password = builder.password;
    }

    public Configuration getConfig() {
        return config;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Layout<? extends Serializable> getLayout() {
        return layout;
    }

    @Override
    public boolean ignoreExceptions() {
        return false;
    }

    @Override
    public ErrorHandler getHandler() {
        return errorHandler;
    }

    @Override
    public void setHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public void append(LogEvent event) {
        if (State.STARTED.equals(state)) {
            LOGGER.debug("{} appending: {}", logPrefix, event.getMessage().getFormattedMessage());
            byte[] eventBytes = layout.toByteArray(event);
            //throttler.push(eventBytes);
            // 添加到redistribution
        }
    }

    @Override
    public void initialize() {

    }

    @Override
    public void start() {
        LOGGER.info("{} starting", logPrefix);
        ensureInitialized();
    }

    private synchronized void ensureInitialized() {
        if (state == null) {
            initialize();
        }
    }

    @Override
    public synchronized void stop() {

    }

    @Override
    public boolean isStarted() {
        return state == State.STARTED;
    }

    @Override
    public boolean isStopped() {
        return state == State.STOPPED;
    }

    @Override
    public String toString() {
        return "RedisAppender{state=" + state +
                ", name='" + name + '\'' +
                ", layout='" + layout + '\'' +
                ", key='" + key + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

    @PluginBuilderFactory
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements org.apache.logging.log4j.core.util.Builder<RedisAppender> {

        @PluginConfiguration
        private Configuration config;

        @PluginBuilderAttribute
        @Required(message = "missing name")
        private String name;

        @PluginBuilderAttribute
        private Charset charset = StandardCharsets.UTF_8;

        @PluginElement("Layout")
        private Layout<? extends Serializable> layout = PatternLayout.newBuilder().withCharset(charset).build();

        @PluginBuilderAttribute
        @Required(message = "missing key")
        private String key;

        @PluginBuilderAttribute
        private String host = "localhost";

        @PluginBuilderAttribute
        private String password = null;

        @PluginBuilderAttribute
        private int port = 6379;


        private Builder() {
            // Do nothing.
        }

        public Configuration getConfig() {
            return config;
        }

        public Builder setConfig(Configuration config) {
            this.config = config;
            return this;
        }

        public String getName() {
            return name;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Charset getCharset() {
            return charset;
        }

        public Builder setCharset(Charset charset) {
            this.charset = charset;
            return this;
        }

        public Layout<? extends Serializable> getLayout() {
            return layout;
        }

        public Builder setLayout(Layout<LogEvent> layout) {
            this.layout = layout;
            return this;
        }

        public String getKey() {
            return key;
        }

        public Builder setKey(String key) {
            this.key = key;
            return this;
        }

        public String getHost() {
            return host;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public int getPort() {
            return port;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public String getPassword() {
            return password;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }


        @Override
        public RedisAppender build() {
            return new RedisAppender(this);
        }



        @Override
        public String toString() {
            return "Builder{name='" + name + '\'' +
                    ", charset=" + charset +
                    ", layout='" + layout + '\'' +
                    ", key='" + key + '\'' +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    '}';
        }

    }
}
