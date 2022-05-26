package com.jiangore.logging.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Administrator
 */
public class RedisAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

	RedisClient client;
	GenericObjectPool<StatefulRedisConnection<String, String>> pool;

	/**
	 * 日志布局
	 */
	JSONEventLayout jsonLayout;

	Layout<ILoggingEvent> layout;

	/**
	 * default config
	 */
	String host = "localhost";
	int port = 6379;
	String password = null;
	int database = 0;
	long timeout = 3000;
	String key = null;

	int timeBetweenEvictionRuns = 30000;
	int minEvictableIdleTime = 60000;

	public RedisAppender() {
		jsonLayout = new JSONEventLayout();
	}

	@Override
	public void start() {
		super.start();
		RedisURI uri = RedisURI.Builder
				.redis(host, port)
				.withDatabase(database)
				.withTimeout(Duration.ofMillis(timeout))
				.build();
		if (password != null && !"".equals(password)) {
			uri.setPassword(password.toCharArray());
		}
		client = RedisClient.create(uri);

		GenericObjectPoolConfig<StatefulRedisConnection<String, String>> config = new GenericObjectPoolConfig<>();
		//最大提供的连接数
		config.setMaxTotal(100);
		//最大空闲连接数(即初始化提供了10有效的连接数)
		config.setMaxIdle(10);
		//最小保证的提供的（空闲）连接数
		config.setMinIdle(2);
		config.setTestOnBorrow(true);
		config.setMinEvictableIdleTime(Duration.ofMillis(minEvictableIdleTime));
		config.setTimeBetweenEvictionRuns(Duration.ofMillis(timeBetweenEvictionRuns));
		pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, config);
	}

	@Override
	protected void append(ILoggingEvent event) {
		StatefulRedisConnection<String, String> connection = null;
		try {
			connection = pool.borrowObject();
			RedisCommands<String, String> syncCommands = connection.sync();
			String json = layout == null ? jsonLayout.doLayout(event) : layout.doLayout(event);
			syncCommands.rpush(key, json);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	@Override
	public void stop() {
		super.stop();
		if (pool != null) {
			pool.close();
		}
		if (client != null) {
			client.shutdown();
		}
	}

	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}

	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}

	public int getDatabase() {
		return database;
	}
	public void setDatabase(int database) {
		this.database = database;
	}

	public long getTimeout() {
		return timeout;
	}
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public int getTimeBetweenEvictionRuns() {
		return timeBetweenEvictionRuns;
	}
	public void setTimeBetweenEvictionRuns(int timeBetweenEvictionRuns) {
		this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
	}

	public int getMinEvictableIdleTime() {
		return minEvictableIdleTime;
	}
	public void setMinEvictableIdleTime(int minEvictableIdleTime) {
		this.minEvictableIdleTime = minEvictableIdleTime;
	}

	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}

	public Layout<ILoggingEvent> getLayout() {
		return layout;
	}
	public void setLayout(Layout<ILoggingEvent> layout) {
		this.layout = layout;
	}


	@Deprecated
	public String getSource() {
		return jsonLayout.getSource();
	}
	@Deprecated
	public void setSource(String source) {
		jsonLayout.setSource(source);
	}
	@Deprecated
	public String getSourceHost() {
		return jsonLayout.getSourceHost();
	}
	@Deprecated
	public void setSourceHost(String sourceHost) {
		jsonLayout.setSourceHost(sourceHost);
	}
	@Deprecated
	public String getSourcePath() {
		return jsonLayout.getSourcePath();
	}
	@Deprecated
	public void setSourcePath(String sourcePath) {
		jsonLayout.setSourcePath(sourcePath);
	}
	@Deprecated
	public String getTags() {
		if (jsonLayout.getTags() != null) {
			Iterator<String> i = jsonLayout.getTags().iterator();
			StringBuilder sb = new StringBuilder();
			while (i.hasNext()) {
				sb.append(i.next());
				if (i.hasNext()) {
					sb.append(',');
				}
			}
			return sb.toString();
		}
		return null;
	}
	@Deprecated
	public void setTags(String tags) {
		if (tags != null) {
			String[] atags = tags.split(",");
			jsonLayout.setTags(Arrays.asList(atags));
		}
	}
	@Deprecated
	public String getType() {
		return jsonLayout.getType();
	}
	@Deprecated
	public void setType(String type) {
		jsonLayout.setType(type);
	}

	@Deprecated
	public void setMdc(boolean flag) {
		jsonLayout.setProperties(flag);
	}

	@Deprecated
	public boolean getMdc() {
		return jsonLayout.getProperties();
	}

	@Deprecated
	public void setLocation(boolean flag) {
		jsonLayout.setLocationInfo(flag);
	}

	@Deprecated
	public boolean getLocation() {
		return jsonLayout.getLocationInfo();
	}

	@Deprecated
	public void setCallerStackIndex(int index) {
		jsonLayout.setCallerStackIdx(index);
	}

	@Deprecated
	public int getCallerStackIndex() {
		return jsonLayout.getCallerStackIdx();
	}

	@Deprecated
	public void addAdditionalField(AdditionalField p) {
		jsonLayout.addAdditionalField(p);
	}

}
