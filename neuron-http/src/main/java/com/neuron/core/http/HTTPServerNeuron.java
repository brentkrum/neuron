package com.neuron.core.http;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.Security;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.AddressableDuplexBusSystem;
import com.neuron.core.Config;
import com.neuron.core.DefaultNeuronInstanceBase;
import com.neuron.core.IMessagePipeReaderCallback;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.MessagePipeSystem;
import com.neuron.core.MessagePipeSystem.IPipeWriterContext;
import com.neuron.core.ObjectConfigBuilder;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.ObjectConfigBuilder.ObjectConfigObjectBuilder;
import com.neuron.core.StatusSystem;
import com.neuron.core.StatusSystem.StatusType;

import co.cask.http.internal.HttpDispatcher;
import co.cask.http.internal.RequestRouter;

import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.NeuronRef.INeuronStateListenerRemoval;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;

public class HTTPServerNeuron extends DefaultNeuronInstanceBase implements INeuronInitialization {
	private static final Logger LOG = LogManager.getLogger(HTTPServerNeuron.class);

	private enum ServerState {
		None, Binding, Bound, Closing, Closed
	}

	private static final int DEFAULT_SERVER_BACKLOG = Config.getFWInt("http.HTTPServerNeuron.defaultServerSocketBacklog", 32);
	private static final String DEFAULT_SSL_PROTOCOL = Config.getFWString("http.HTTPServerNeuron.defaultSSLProtocol", "TLSv1.2");
	private static final String DEFAULT_STATIC_CONTENT_PATH_ROOT = Config.getFWString("http.HTTPServerNeuron.defaultStaticContentPathRoot", "/www");

	private static final AttributeKey<Connection> CONNECTION = AttributeKey.newInstance("ConnectionClass");
	private final ServerBootstrap m_serverBootstrap = new ServerBootstrap();
	private final ServerCloseListener m_serverCloseListener = new ServerCloseListener();
	private final ConnectionInboundMsgHandler m_connectionMsgHandler = new ConnectionInboundMsgHandler();
	private final ConnectionInboundChunkHandler m_connectionChunkHandler = new ConnectionInboundMsgHandler();
	private final ConnectionInboundInitHandler m_connectionInitHandler = new ConnectionInboundInitHandler();
	private final int m_port;
	private final boolean m_sslEnabled;
	private final String m_statusHostAndPort;
	private final ObjectConfig m_config;
	private final String m_staticContentPathRoot;

	private boolean m_deinitializing;
	private Channel m_serverConnection;
	private ServerState m_serverState = ServerState.None;
	private SslContext m_sslContext;
	
	public HTTPServerNeuron(NeuronRef instanceRef, int port, ObjectConfig config) {
		super(instanceRef);
		m_port = port;
		m_config = config;
		m_sslEnabled = config.getBoolean(HTTPServerNeuronTemplate.Config_UseSSL);
		m_statusHostAndPort = "HTTPServerNeuron:" + m_port;
		m_staticContentPathRoot = config.getString(HTTPServerNeuronTemplate.Config_StaticContentPathRoot, DEFAULT_STATIC_CONTENT_PATH_ROOT);
	}

	@Override
	public void connectResources() {
	}

	@Override
	public void init(Promise<Void> initPromise) {
		if (m_sslEnabled) {
			m_sslContext = createSSLContext();
		}
		m_serverBootstrap.group(NeuronApplication.getIOPool(), NeuronApplication.getIOPool())
			.channel(NioServerSocketChannel.class)
			.handler(new BindHandler())
			.option(ChannelOption.ALLOCATOR, NeuronApplication.allocator())
			.option(ChannelOption.SO_BACKLOG, m_config.getInteger(HTTPServerNeuronTemplate.Config_ServerBacklog, DEFAULT_SERVER_BACKLOG))
			.childHandler(new MyChannelInitializer())
			.childOption(ChannelOption.ALLOCATOR, NeuronApplication.allocator())
			.childOption(ChannelOption.SO_KEEPALIVE, true);

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
	}

	@Override
	public void deinit(Promise<Void> promise) {
		synchronized (HTTPServerNeuron.this) {
			m_deinitializing = true;
			if (m_serverState == ServerState.Bound) {
				try (INeuronStateLock lock = ref().lockState()) {
					NeuronApplication.logInfo(LOG, "Closing listener");
				}
				m_serverCloseListener.setCompletePromise(promise);
				m_serverConnection.close();
				m_serverState = ServerState.Closing;

			} else if (m_serverState == ServerState.Binding) {
				try (INeuronStateLock lock = ref().lockState()) {
					NeuronApplication.logInfo(LOG, "Aborting current bind attempt in progress");
				}
				m_serverCloseListener.setCompletePromise(promise);
				m_serverConnection.close();
				m_serverState = ServerState.Closing;

			} else if (m_serverState == ServerState.Closing) {
				m_serverCloseListener.setCompletePromise(promise);

			} else {
				// m_serverState is None or Closed
				promise.setSuccess((Void) null);
			}
		}
	}

	public SslContext createSSLContext() {
		final String[] sslProtocols = m_config.getString(HTTPServerNeuronTemplate.Config_SSLProtocols, DEFAULT_SSL_PROTOCOL).split(",");
		final String keyManagerAlgorithm = m_config.getString(HTTPServerNeuronTemplate.Config_KeyManagerAlgorithm, KeyManagerFactory.getDefaultAlgorithm());
		final String keyStoreFilename = m_config.getString(HTTPServerNeuronTemplate.Config_KeyStoreFilename, null);
		final String keyStorePassword = m_config.getString(HTTPServerNeuronTemplate.Config_KeyStorePassword, "changeit");
		final String trustManagerAlgorithm = m_config.getString(HTTPServerNeuronTemplate.Config_TrustManagerAlgorithm, TrustManagerFactory.getDefaultAlgorithm());
		final String trustStoreFilename = m_config.getString(HTTPServerNeuronTemplate.Config_TrustStoreFilename, null);
		final String trustStorePassword = m_config.getString(HTTPServerNeuronTemplate.Config_TrustStorePassword, "changeit");
		
		if (keyStoreFilename == null) {
			NeuronApplication.logError("Missing required configuration parameter " + HTTPServerNeuronTemplate.Config_KeyStoreFilename);
			return null;
		}
		
		try {
			final KeyManagerFactory kmFactory = getKeyManagerFactory(keyStoreFilename, keyStorePassword.toCharArray(), keyManagerAlgorithm);
			final TrustManagerFactory tmFactory = getTrustManagerFactory(trustStoreFilename, trustStorePassword.toCharArray(), trustManagerAlgorithm);
	      SslContextBuilder builder = SslContextBuilder.forServer(kmFactory);
	      builder.protocols(sslProtocols);
	      if (tmFactory != null) {
	      	builder.trustManager(tmFactory);
		      builder.clientAuth(ClientAuth.OPTIONAL);
	      } else {
		      builder.clientAuth(ClientAuth.NONE);
	      }
	      
			return builder.build();

		} catch (Exception ex) {
			NeuronApplication.logError("Could not configure SSL context", ex);
		}
		return null;
	}

	/**
	 * KeyStores provide credentials, TrustStores verify credentials.
	 *
	 * Server KeyStores stores the server's private keys, and certificates for
	 * corresponding public keys. Used here for HTTPS connections over localhost.
	 *
	 * Client TrustStores store servers' certificates.
	 */
	private static KeyStore getStore(final String storeFileName, final char[] password) throws Exception {
		final KeyStore store = KeyStore.getInstance("jks");
		try (InputStream in = new FileInputStream(storeFileName)) {
			store.load(in, password);
		}

		return store;
	}
	
	/**
	 * Server KeyManagers use their private keys during the key exchange algorithm
	 * and send certificates corresponding to their public keys to the clients. The
	 * certificate comes from the KeyStore.
	 */
	private static KeyManagerFactory getKeyManagerFactory(String keyStoreFilename, final char[] keyStorePassword, String algorithm) throws Exception {
		final KeyStore serverKeyStore = getStore(keyStoreFilename, keyStorePassword);
		final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
		keyManagerFactory.init(serverKeyStore, keyStorePassword);
		return keyManagerFactory;
	}
	
	private static TrustManagerFactory getTrustManagerFactory(String trustStoreFilename, final char[] trustStorePassword, String trustManagerAlgorithm) throws Exception {
		if (trustStoreFilename == null) {
			NeuronApplication.logInfo("Not using a server trust manager (trusting all client hosts)");
			return null;
		}
		final KeyStore trustStore = getStore(trustStoreFilename, trustStorePassword);
		final TrustManagerFactory tmf = TrustManagerFactory.getInstance(trustManagerAlgorithm);
		tmf.init(trustStore);
		return tmf;
	}
	

	private void sendConnectEvent(final Connection c) {
		final InboundMessage pipeMsg = new InboundMessage(c, InboundMessage.MessageType.Connect, null);
		if (!m_inPipeWriter.offer(pipeMsg)) {
			pipeMsg.release();
			if (LOG.isDebugEnabled()) {
				LOG.debug("Connect message rejected by pipe");
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Sent connect message");
			}
		}
	}

	private class OutPipeReader implements IMessagePipeReaderCallback {

		@Override
		public void onData(ReferenceCounted pipeMsg) {
			if (!(pipeMsg instanceof OutboundMessage)) {
				try (INeuronStateLock lock = ref().lockState()) {
					LOG.error("Invalid message class of {} submitted to Out pipe", pipeMsg.getClass().getCanonicalName());
				}
				return;
			}
			final OutboundMessage msg = (OutboundMessage) pipeMsg;
			synchronized (HTTPServerNeuron.this) {
				if (m_serverState != ServerState.Bound) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("OutPipeReader.onData - server state {} is not Bound", m_serverState);
					}
					return;
				}
				try (INeuronStateLock lock = ref().lockState()) {
					// Only send data while we are SystemOnline, Online, or SystemOffline
					if (!lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("OutPipeReader.onData - neuron state {} is not SystemOnline/Online/GoingOffline",
									lock.currentState());
						}
						return;
					}
				}
				msg.data.retain();
				if (LOG.isTraceEnabled()) {
					LOG.trace("OutPipeReader.onData - writeAndFlush");
				}
				// What happens here if Channel is closed, closing, broken, etc.???
				msg.connection.m_channel.writeAndFlush(msg.data);
			}
		}

	}

	private class MyChannelInitializer extends ChannelInitializer<SocketChannel> {
		@Override
		public void initChannel(SocketChannel childChannel) throws Exception {
			final String connectionToString = childChannel.remoteAddress().toString();
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("[{}] initChannel() - start", connectionToString);
			}

			try (INeuronStateLock lock = ref().lockState()) {
				// Only accept connections while we are SystemOnline or Online
				if (!lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("[{}] Channel rejected due to Neuron state {}", connectionToString, lock.currentState());
					}
					childChannel.close();
					return;
				}

				final INeuronStateListenerRemoval listenerRemove = lock.addStateAsyncListener(NeuronState.Deinitializing, (success, neuronRef, completed) -> {
					if (LOG.isDebugEnabled()) {
						try (INeuronStateLock callbackLock = ref().lockState()) {
							LOG.debug("[{}] DeInit closing", connectionToString);
						}
					}
					childChannel.close().addListener((f) -> {
						if (LOG.isDebugEnabled()) {
							try (INeuronStateLock callbackLock = ref().lockState()) {
								LOG.debug("[{}] DeInit close completed", connectionToString);
							}
						}
						completed.setSuccess((Void) null);
					});
				});
				final ChannelPipeline pipeline = childChannel.pipeline();
				if (m_sslContext != null) {
					pipeline.addLast("sslEngine", m_sslContext.newHandler(childChannel.alloc()));
				}
				pipeline.addLast("codec", new HttpServerCodec(/*4096, 8192, 8192*/)); // TODO: make these configurable
				pipeline.addLast("compressor", new HttpContentCompressor());
				pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
				pipeline.addLast("keepAlive", new HttpServerKeepAliveHandler());
				pipeline.addLast(m_connectionInitHandler);
				final Connection c = new Connection(connectionToString, childChannel, listenerRemove);
				childChannel.attr(CONNECTION).set(c);
				sendConnectEvent(c);
			}
			finally {
				if (LOG.isDebugEnabled()) {
					LOG.debug("[{}] initChannel() - end", connectionToString);
				}
			}
		}
	}
/*
	private class StaticContentHandler extends ChannelInboundHandlerAdapter {

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
	      if (msg instanceof HttpRequest) {
	      	HttpRequest r = (HttpRequest)msg;
	      	
	      	if (r.method() == HttpMethod.GET && r.decoderResult()) {
	      		
	      	}
	      }
		}
		
	}
*/
	@Sharable
	private class ConnectionInboundInitHandler extends ChannelInboundHandlerAdapter {

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
      	final Connection conn = ctx.channel().attr(CONNECTION).get();
      	
	      if (msg instanceof HttpRequest) {
	      	final HttpRequest req = (HttpRequest)msg;
	      	
            if (HttpUtil.is100ContinueExpected(req)) {
            	ctx.write( new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE) );
            }
            if (LOG.isDebugEnabled()) {
            	final StringBuilder sb = new StringBuilder();
	            final HttpHeaders headers = req.headers();
	            if (!headers.isEmpty()) {
	            	sb.append("\r\n");
	                for (Map.Entry<String, String> h: headers) {
	                    CharSequence key = h.getKey();
	                    CharSequence value = h.getValue();
	                    sb.append('\t').append(key).append(" = ").append(value).append("\r\n");
	                }
	            }
            	LOG.info("{} {} {}{}", req.method(), req.protocolVersion(), req.uri(), sb);
            } else if (LOG.isInfoEnabled()) {
            	LOG.info("{} {} {}", req.method(), req.protocolVersion(), req.uri());
            }
            
            final QueryStringDecoder qsDecoder = new QueryStringDecoder(req.uri());
            
				final ChannelPipeline pipeline = ctx.pipeline();
				if (wantsStreaming) {
					pipeline.addLast(m_connectionChunkHandler);
					pipeline.remove(this);
					
				} else if (somoneListening) {
					pipeline.addLast(new HttpObjectAggregator(1024*1024));
					pipeline.addLast(m_connectionMsgHandler);
					pipeline.remove(this);
				
				} else if(method == GET,HEAD,etc.) {
					m_staticContentHandler.channelRead(ctx, msg);
					
				} else {
					// 404, bad method or something
				}
	      	
	      } else {
				LOG.error("[{}] ConnectionInboundInitHandler got an unexpected message of type {}", conn.m_connectionToString, msg.getClass().getCanonicalName());
	      	ctx.close();
	      }
		}
	}
	
	@Sharable
	private class ConnectionInboundMsgHandler extends ChannelInboundHandlerAdapter {

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			
			if (msg instanceof FullHttpRequest) {
	      	HttpRequest r = (HttpRequest)msg;
	      	
	      	if (r.method() == HttpMethod.GET) {
	      		final QueryStringDecoder qs = new QueryStringDecoder(r.uri());
	      		try {
		      		final Path p = Paths.get(m_staticContentPathRoot, qs.path());
		      		final File f = p.toFile();
		      		if (f.exists() && f.isFile()) {
		      			// This is a static file in the filesystem
		      		}
	      		} catch(Exception ex) {
	      			
	      		}
	      	}
				
			}
	      if (msg instanceof HttpRequest) {
	         // Start of HTTP
	      	// There can be multiple if this connection is re-used, but will only be one at a time
	      	// We can get multiple requests even before we start responding
	      	// Responses must be in-order
	      	
	      	// When we get a request, if we already have processed a previous request, call a "requestDone" method
	      	//		so we can handle bad requests that send in incomplete body data
	      	// How do we send "requestDone" when there are no subsequent requests on this connection?
	      	//		1. channelInactive
	      	//		2. timeout?
	      	//		3. channelReadComplete ?
	      	
	      	// Need to process GET requests for static assets here... just add file responders for files in the file-system and inputstream responders for classpath assets
	      	// Static assets are pulled from a config supplied directory
	      	//
	       } else if (msg instanceof HttpContent) {
	         // Optional Body
	      	// Send chunks
	       } else {
					LOG.error("ConnectionInboundDataHandler.channelRead got an object that was not an HttpRequest or HttpContent: {}", msg.getClass().getCanonicalName());
					ReferenceCountUtil.release(msg);
	       }
			
			
			final Connection c = ctx.channel().attr(CONNECTION).get();
			final InboundMessage pipeMsg = new InboundMessage(c, InboundMessage.MessageType.Data, (ByteBuf) msg);
			if (m_inPipeWriter.offer(pipeMsg)) {
				if (LOG.isTraceEnabled()) {
					LOG.trace("ConnectionInboundDataHandler.channelRead() - sent Data message");
				}
				return;
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("ConnectionInboundDataHandler.channelRead() - {} pipe full, releasing buffer",
						m_inPipeWriter.name());
			}
			pipeMsg.release();
		}

		@Override
		public void channelInactive(ChannelHandlerContext ctx) throws Exception {
			if (LOG.isDebugEnabled()) {
				LOG.debug("channelInactive({})", ctx.channel().remoteAddress().toString());
			}

			final Connection c = ctx.channel().attr(CONNECTION).getAndSet(null);
			if (c == null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("channelInactive() - CONNECTION attribute was missing");
				}
				return;
			}
			StatusSystem.setInboundStatus(c.toString(), StatusSystem.StatusType.Down, "Disconnected");
			if (c.m_listenerRemove != null) {
				c.m_listenerRemove.remove();
			}
			final InboundMessage pipeMsg = new InboundMessage(c, InboundMessage.MessageType.Disconnect, null);
			if (m_inPipeWriter.offer(pipeMsg)) {
				return;
			}
			pipeMsg.release();
			if (LOG.isDebugEnabled()) {
				LOG.debug("ConnectionInboundDataHandler.channelInactive() - {} pipe full, cannot send Disconnect message",
						m_inPipeWriter.name());
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("exceptionCaught({})", ctx.channel().remoteAddress().toString(), cause);
			}
			synchronized (HTTPServerNeuron.this) {
				if (m_deinitializing) {
					return;
				}
			}
			try (INeuronStateLock lock = ref().lockState()) {
				NeuronApplication.logInfo(LOG, "Exception with connection, closing", cause);
			}
			// close listener will deal with stuff
			ctx.close();
		}
	}

	@Sharable
	private class BindHandler extends ChannelInboundHandlerAdapter {

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			synchronized (HTTPServerNeuron.this) {
				if (m_deinitializing) {
					return;
				}
				if (m_serverState == ServerState.Bound) {
					m_serverState = ServerState.Closing;
					try (INeuronStateLock lock = ref().lockState()) {
						NeuronApplication.logInfo(LOG, "Exception with bound socket, closing", cause);
					}
					ctx.close();
				} else {
					LOG.info("Exception with socket (state={}), calling close", m_serverState, cause);
					ctx.close();
				}
			}
		}
	}

	private class BindListener implements ChannelFutureListener {
		private final Promise<Void> m_completedPromise;

		BindListener(Promise<Void> completedPromise) {
			m_completedPromise = completedPromise;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					synchronized (HTTPServerNeuron.this) {
						if (m_deinitializing) {
							return;
						}
						if (m_serverState != ServerState.Binding) {
							try (INeuronStateLock lock = ref().lockState()) {
								LOG.error("BindListener got an operationComplete(success) while serverState is {}", m_serverState.toString());
							}
							return;
						}
						m_serverState = ServerState.Bound;
						m_serverConnection = future.channel();
						try (INeuronStateLock lock = ref().lockState()) {
							NeuronApplication.logInfo(LOG, "Bound and listening");
							StatusSystem.setInboundStatus(m_statusHostAndPort, StatusType.Up, "Listening");
						}
						m_serverConnection.closeFuture().addListener(m_serverCloseListener);
					}

				} else {
					// TODO add a retry timer when a bind failure because port is in use.
					// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
					// it might just be a lazy cleanup of the socket from a previous run
					String statusText = future.cause().getMessage();
					Throwable cause = future.cause();
					synchronized (HTTPServerNeuron.this) {
						try (INeuronStateLock lock = ref().lockState()) {
							if (m_serverState != ServerState.Binding) {
								LOG.error("BindListener got an operationComplete(failed) while serverState is {}", m_serverState.toString());
								return;
							}
							m_serverState = ServerState.Closed;
							m_serverConnection = null;
							StatusSystem.setInboundStatus(m_statusHostAndPort, StatusType.Down, statusText);
							NeuronApplication.logError(LOG, "Bind to port {} failed, taking neuron offline", m_port, cause);
							lock.addStateListener(NeuronState.Online, (success) -> {
								try (INeuronStateLock onlineLock = ref().lockState()) {
									onlineLock.takeOffline();
								}
							});
						}
						// There is no close listener, so we only have to worry about ourselves
						future.channel().close();
					}
				}
			} finally {
				if (m_completedPromise != null) {
					m_completedPromise.setSuccess((Void) null);
				}
			}
		}

	}

	private class ServerCloseListener implements ChannelFutureListener {
		private boolean m_called;
		private Promise<Void> m_completePromise;

		synchronized void setCompletePromise(Promise<Void> promise) {
			if (m_called) {
				promise.setSuccess((Void) null);
			} else {
				m_completePromise = promise;
			}
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try (INeuronStateLock lock = ref().lockState()) {
				if (m_serverState != ServerState.Bound && m_serverState != ServerState.Closing) {
					LOG.error("ServerCloseListener got an operationComplete(failed) while serverState is {}", m_serverState.toString());
					return;
				}
				synchronized (HTTPServerNeuron.this) {
					// Channel has closed
					m_serverState = ServerState.Closed;
					m_serverConnection = null;

					// if we are shutting down / unloading
					if (m_deinitializing) {
						NeuronApplication.logInfo(LOG, "Listener closed");
						StatusSystem.setInboundStatus(m_statusHostAndPort, StatusType.Down, "Not listening, neuron deinitialized");
					} else {
						NeuronApplication.logInfo(LOG, "Listener closed unexpectedly, taking nuron offline");
						StatusSystem.setInboundStatus(m_statusHostAndPort, StatusType.Down, "Not listening, closed unexpectedly");
						// send a message in the pipe alerting to the disconnected state?
						lock.addStateListener(NeuronState.Online, (success) -> {
							try (INeuronStateLock onlineLock = ref().lockState()) {
								onlineLock.takeOffline();
							}
						});
					}
				}
			} finally {
				synchronized (this) {
					m_called = true;
					if (m_completePromise != null) {
						m_completePromise.setSuccess((Void) null);
					}
				}
			}
		}

	}

	public static final class Connection {
		private final String m_connectionToString;
		private final Channel m_channel;
		private final INeuronStateListenerRemoval m_listenerRemove;

		Connection(String connectionToString, Channel channel, INeuronStateListenerRemoval listenerRemove) {
			m_connectionToString = connectionToString;
			m_channel = channel;
			m_listenerRemove = listenerRemove;
		}

		@Override
		public String toString() {
			return m_connectionToString;
		}
	}

	public static final class OutboundMessage extends AbstractReferenceCounted {
		public final Connection connection;
		public final ByteBuf data;

		public OutboundMessage(Connection connection, ByteBuf data) {
			this.connection = connection;
			this.data = data;
		}

		@Override
		public ReferenceCounted touch(Object hint) {
			return this;
		}

		@Override
		protected void deallocate() {
			if (data != null) {
				data.release();
			}
		}

	}

	public static final class InboundMessage extends AbstractReferenceCounted {
		public enum MessageType {
			Connect, Data, Disconnect
		}; // TODO Add a ConnectionLsit event where I send all open connections. Do this on
			// a ReaderConnect event to the In pipe
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		public final MessageType messageType;
		public final Connection connection;
		public final ByteBuf data;
		
		private final String m_httpMethod;
		private final HttpHeaders m_headers;
		private final List<KeyValue> m_params;
		private final List<KeyValue> m_formData;
		
		InboundMessage(Connection connection, MessageType messageType, ByteBuf data) {
			this.messageType = messageType;
			this.connection = connection;
			this.data = data;
		}

		@Override
		public ReferenceCounted touch(Object hint) {
			return this;
		}

		@Override
		protected void deallocate() {
			if (data != null) {
				data.release();
			}
		}

	}
}
