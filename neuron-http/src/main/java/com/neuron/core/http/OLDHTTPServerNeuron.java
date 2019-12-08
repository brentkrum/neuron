package com.neuron.core.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.Config;
import com.neuron.core.DefaultNeuronInstanceBase;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.StatusSystem;
import com.neuron.core.MessagePipeSystem.IPipeWriterContext;
import com.neuron.core.NeuronRef.INeuronStateListenerRemoval;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.socket.InboundSocketNeuron;
import com.neuron.core.socket.InboundSocketNeuron.BindHandler;
import com.neuron.core.socket.InboundSocketNeuron.BindListener;
import com.neuron.core.socket.InboundSocketNeuron.Connection;
import com.neuron.core.socket.InboundSocketNeuron.MyChannelInitializer;
import com.neuron.core.socket.InboundSocketNeuron.ServerState;

import co.cask.http.internal.HttpDispatcher;
import co.cask.http.internal.RequestRouter;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;

public class OLDHTTPServerNeuron extends DefaultNeuronInstanceBase implements INeuronInitialization {
	private static final Logger LOG = LogManager.getLogger(HTTPServerNeuron.class);
	
	public static final String Config_ServerBacklog = "serverSocketBacklog";
	public static final String Config_ServerPort = "serverPort";
	public static final String Config_UseSSL = "useSSL";
	private static final int DEFAULT_SERVER_BACKLOG = Config.getFWInt("http.HTTPServerNeuron.defaultServerSocketBacklog", 32);
	
	private enum ServerState { None, Binding, Bound, Closing, Closed }
	
	private final ObjectConfig m_config;
	private final MyChannelInitializer m_initializer = new MyChannelInitializer();
	private final ChannelGroup m_allChannelsGroup = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
	private final ServerBootstrap m_serverBootstrap = new ServerBootstrap();
	private ServerState m_serverState = ServerState.None;
	
	private final int m_port;
	private final boolean m_sslEnabled;
	private final String m_statusHostAndPort;
	private boolean m_deinitializing;
	private Channel m_serverConnection;
	private IPipeWriterContext m_inPipeWriter;
	
	public OLDHTTPServerNeuron(NeuronRef instanceRef, ObjectConfig config) {
		super(instanceRef);
		m_config = config;
		m_port = config.getInteger(Config_ServerPort, 8080);
		m_sslEnabled = config.getBoolean(Config_UseSSL);
		m_statusHostAndPort = "InboundSocketNeuron:" + m_port;
	}
	
	@Override
	public ObjectConfig config() {
		return m_config;
	}


	@Override
	public void init(Promise<Void> initPromise) {
		m_serverBootstrap.group(NeuronApplication.getIOPool(), NeuronApplication.getTaskPool())
				.channel(NioServerSocketChannel.class)
				.childHandler(m_initializer)
				.handler(new BindHandler())
				.option(ChannelOption.SO_BACKLOG, m_config.getInteger(Config_ServerBacklog, DEFAULT_SERVER_BACKLOG))
				.childOption(ChannelOption.SO_KEEPALIVE, true);

		m_serverBootstrap.register().addListener((regFuture) -> {
			if (regFuture.isSuccess()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Server register successful");
				}
				initPromise.setSuccess((Void) null);
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Server register failed");
				}
				initPromise.setFailure(regFuture.cause());
			}
		});

		try (INeuronStateLock lock = ref().lockState()) {
			lock.addStateAsyncListener(NeuronState.SystemOnline, (success, neuronRef, completedPromise) -> {
				if (success) {
					synchronized (HTTPServerNeuron.this) {
						m_serverState = ServerState.Binding;
					}
					NeuronApplication.logInfo(LOG, "Binding to port {}", m_port);
					m_serverBootstrap.bind(m_port).addListener(new BindListener(completedPromise));
				} else {
					completedPromise.setSuccess((Void) null);
				}
			});
		}
	}

	@Override
	public void connectResources() {
		// TODO Auto-generated method stub
		super.connectResources();
	}

	@Override
	public void nowOnline() {
		// TODO Auto-generated method stub
		super.nowOnline();
	}

	private final class MyChannelInitializer extends ChannelInitializer<SocketChannel> {

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			if (LOG.isDebugEnabled()) {
				LOG.debug("initChannel({})", ch.remoteAddress().toString());
			}
			m_allChannelsGroup.add(ch);

			ChannelPipeline pipeline = ch.pipeline();
			if (sslHandlerFactory != null) {
				// Add SSLHandler if SSL is enabled
				pipeline.addLast("ssl", sslHandlerFactory.create(ch.alloc()));
			}
			pipeline.addLast("codec", new HttpServerCodec());
			pipeline.addLast("compressor", new HttpContentCompressor());
			pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
			pipeline.addLast("keepAlive", new HttpServerKeepAliveHandler());
			pipeline.addLast("router", new RequestRouter(resourceHandler, httpChunkLimit, sslHandlerFactory != null));
			if (eventExecutorGroup == null) {
				pipeline.addLast("dispatcher", new HttpDispatcher());
			} else {
				pipeline.addLast(eventExecutorGroup, "dispatcher", new HttpDispatcher());
			}

			if (pipelineModifier != null) {
				pipelineModifier.modify(pipeline);
			}
			
			
			try(INeuronStateLock lock = ref().lockState()) {
				// Only accept connections while we are SystemOnline or Online
				if (!lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Channel rejected due to Neuron state {}", lock.currentState());
					}
					ch.close();
					return;
				}

				final INeuronStateListenerRemoval listenerRemove = lock.addStateAsyncListener(NeuronState.Deinitializing, (success, neuronRef, completed) -> {
					if (LOG.isDebugEnabled()) {
						try(INeuronStateLock callbackLock = ref().lockState()) {
							LOG.debug("DeInit closing");
						}
					}
					ch.close().addListener((f) -> {
						if (LOG.isDebugEnabled()) {
							try(INeuronStateLock callbackLock = ref().lockState()) {
								LOG.debug("DeInit close completed");
							}
						}
						completed.setSuccess((Void)null);
					});
				});
				if (LOG.isTraceEnabled()) {
					// Need to only include this IF the log level is trace
					ch.pipeline().addLast(new LoggingHandler(HTTPServerNeuron.class, LogLevel.TRACE));
				}
				ch.pipeline().addLast(m_connectionDataHandler);
				
				final Connection c = new Connection(ch.remoteAddress().toString(), ch, listenerRemove);
				ch.attr(CONNECTION).set(c);
				NeuronApplication.logInfo(LOG, "{} connected", c.toString());
				StatusSystem.setInboundStatus(c.toString(), StatusSystem.StatusType.Up, "Connected");
				sendConnectEvent(c);
			}
			
		}
	}
}
