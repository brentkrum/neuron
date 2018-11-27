package com.neuron.core.socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Promise;

public class InboundSocketNeuron extends DefaultNeuronInstanceBase implements INeuronInitialization
{
	private static final Logger LOG = LogManager.getLogger(InboundSocketNeuron.class);
	
	private enum ServerState { None, Binding, Bound, Closing, Closed }
	
	private static final AttributeKey<Connection> CONNECTION = AttributeKey.newInstance("ConnectionClass"); 
	private final ServerBootstrap m_serverBootstrap = new ServerBootstrap();
	private final ServerCloseListener m_serverCloseListener = new ServerCloseListener();
	private final ConnectionInboundDataHandler m_connectionDataHandler = new ConnectionInboundDataHandler();
	private final int m_port;
	private final String m_statusHostAndPort;
	private final ObjectConfig m_config;
	private boolean m_deinitializing;
	private Channel m_serverConnection;
	private IPipeWriterContext m_inPipeWriter;
	private ServerState m_serverState = ServerState.None;
	
	public InboundSocketNeuron(NeuronRef instanceRef, int port, ObjectConfig config)
	{
		super(instanceRef);
		m_port = port;
		m_statusHostAndPort = "InboundSocketNeuron:" + m_port;
		m_config = config;
	}

	@Override
	public void connectResources()
	{
		ObjectConfigObjectBuilder inPipeConfigBuilder = ObjectConfigBuilder.config();
		if (m_config.has(InboundSocketNeuronTemplate.Config_InPipeMaxPipeMsgCount)) {
			inPipeConfigBuilder.option(MessagePipeSystem.pipeBrokerConfig_MaxPipeMsgCount, m_config.getInteger(InboundSocketNeuronTemplate.Config_InPipeMaxPipeMsgCount, null));
		}
		MessagePipeSystem.configurePipeBroker("In", inPipeConfigBuilder.build());
		
		ObjectConfigObjectBuilder outPipeConfigBuilder = ObjectConfigBuilder.config();
		if (m_config.has(InboundSocketNeuronTemplate.Config_OutPipeMaxPipeMsgCount)) {
			outPipeConfigBuilder.option(MessagePipeSystem.pipeBrokerConfig_MaxPipeMsgCount, m_config.getInteger(InboundSocketNeuronTemplate.Config_OutPipeMaxPipeMsgCount, null));
		}
		MessagePipeSystem.configurePipeBroker("Out", outPipeConfigBuilder.build());
		
		MessagePipeSystem.readFromPipe("Out", ObjectConfigBuilder.config().build(), new OutPipeReader());
		
		m_inPipeWriter = MessagePipeSystem.writeToPipe("In", ObjectConfigBuilder.config().build(), (event, context) -> {
		});
	}

	@Override
	public void init(Promise<Void> initPromise)
	{
		m_serverBootstrap.group(NeuronApplication.getIOPool(), NeuronApplication.getIOPool())
			.channel(NioServerSocketChannel.class)
			.handler(new BindHandler())
			.option(ChannelOption.SO_BACKLOG, 32)
			.childHandler(new MyChannelInitializer())
			.childOption(ChannelOption.SO_KEEPALIVE, true);

		m_serverBootstrap.register().addListener((regFuture) -> {
			if (regFuture.isSuccess()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Server register successful");
				}
				initPromise.setSuccess((Void)null);
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Server register failed");
				}
				initPromise.setFailure(regFuture.cause());
			}
		});
		
		try(INeuronStateLock lock = ref().lockState()) {
			lock.addStateAsyncListener(NeuronState.SystemOnline, (success, neuronRef, completedPromise) -> {
				if (success) {
					synchronized(InboundSocketNeuron.this) {
						m_serverState = ServerState.Binding;
					}
					NeuronApplication.logInfo(LOG, "Binding to port {}", m_port);
					m_serverBootstrap.bind(m_port).addListener(new BindListener(completedPromise));
				} else {
					completedPromise.setSuccess((Void)null);
				}
			});
		}
	}
	
	@Override
	public void deinit(Promise<Void> promise) {
		synchronized(InboundSocketNeuron.this) {
			m_deinitializing = true;
			if (m_serverState == ServerState.Bound) {
				try(INeuronStateLock lock = ref().lockState()) {
					NeuronApplication.logInfo(LOG, "Closing listener");
				}
				m_serverCloseListener.setCompletePromise(promise);
				m_serverConnection.close();
				m_serverState = ServerState.Closing;
				
			} else if (m_serverState == ServerState.Binding) {
				try(INeuronStateLock lock = ref().lockState()) {
					NeuronApplication.logInfo(LOG, "Aborting current bind attempt in progress");
				}
				m_serverCloseListener.setCompletePromise(promise);
				m_serverConnection.close();
				m_serverState = ServerState.Closing;
				
			} else if (m_serverState == ServerState.Closing) {
				m_serverCloseListener.setCompletePromise(promise);
				
			} else {
				// m_serverState is None or Closed
				promise.setSuccess((Void)null);
			}
		}
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
				try(INeuronStateLock lock = ref().lockState()) {
					LOG.error("Invalid message class of {} submitted to Out pipe", pipeMsg.getClass().getCanonicalName());
				}
				return;
			}
			final OutboundMessage msg = (OutboundMessage)pipeMsg;
			synchronized(InboundSocketNeuron.this) {
				if (m_serverState != ServerState.Bound) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("OutPipeReader.onData - server state {} is not Bound", m_serverState);
					}
					return;
				}
				try(INeuronStateLock lock = ref().lockState()) {
					// Only send data while we are SystemOnline, Online, or SystemOffline
					if (!lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("OutPipeReader.onData - neuron state {} is not SystemOnline/Online/GoingOffline", lock.currentState());
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
		public void initChannel(SocketChannel ch) throws Exception {
			if (LOG.isDebugEnabled()) {
				LOG.debug("initChannel({})", ch.remoteAddress().toString());
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
					ch.pipeline().addLast(new LoggingHandler(InboundSocketNeuron.class, LogLevel.TRACE));
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

	@Sharable
	private class ConnectionInboundDataHandler extends ChannelInboundHandlerAdapter {

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg)
		{
			if (!(msg instanceof ByteBuf)) {
				ReferenceCountUtil.release(msg);
				LOG.error("channelRead got a non-ByteBuf: {}", msg.getClass().getCanonicalName());
				return;
			}
			final Connection c = ctx.channel().attr(CONNECTION).get();
			final InboundMessage pipeMsg = new InboundMessage(c, InboundMessage.MessageType.Data, (ByteBuf)msg);
			if (m_inPipeWriter.offer(pipeMsg)) {
				if (LOG.isTraceEnabled()) {
					LOG.trace("ConnectionInboundDataHandler.channelRead() - sent Data message");
				}
				return;
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("ConnectionInboundDataHandler.channelRead() - {} pipe full, releasing buffer", m_inPipeWriter.name());
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
				LOG.debug("ConnectionInboundDataHandler.channelInactive() - {} pipe full, cannot send Disconnect message", m_inPipeWriter.name());
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
		{
			if (LOG.isDebugEnabled()) {
				LOG.debug("exceptionCaught({})", ctx.channel().remoteAddress().toString(), cause);
			}
			synchronized(InboundSocketNeuron.this) {
				if (m_deinitializing) {
					return;
				}
			}
			try(INeuronStateLock lock = ref().lockState()) {
				NeuronApplication.logInfo(LOG, "Exception with connection, closing", cause);
			}
			// close listener will deal with stuff
			ctx.close();
		}
	}

	@Sharable
	private class BindHandler extends ChannelInboundHandlerAdapter {
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
		{
			synchronized(InboundSocketNeuron.this) {
				if (m_deinitializing) {
					return;
				}
				if (m_serverState == ServerState.Bound) {
					m_serverState = ServerState.Closing;
					try(INeuronStateLock lock = ref().lockState()) {
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
		public void operationComplete(ChannelFuture future) throws Exception
		{
			try {
				if (future.isSuccess()) {
					synchronized(InboundSocketNeuron.this) {
						if (m_deinitializing) {
							return;
						}
						if (m_serverState != ServerState.Binding) {
							try(INeuronStateLock lock = ref().lockState()) {
								LOG.error("BindListener got an operationComplete(success) while serverState is {}", m_serverState.toString());
							}
							return;
						}						
						m_serverState = ServerState.Bound;
						m_serverConnection = future.channel();
						try(INeuronStateLock lock = ref().lockState()) {
							NeuronApplication.logInfo(LOG, "Bound and listening");
							StatusSystem.setInboundStatus(m_statusHostAndPort, StatusType.Up, "Listening");
						}
						m_serverConnection.closeFuture().addListener(m_serverCloseListener);
					}
					
				} else {
					// TODO add a retry timer when a bind failure because port is in use. <<<<<<<<<<<<<<<<<<<<<<<<<<<<<< 
					// 	  it might just be a lazy cleanup of the socket from a previous run
					String statusText = future.cause().getMessage();
					Throwable cause = future.cause(); 
					synchronized(InboundSocketNeuron.this) {
						try(INeuronStateLock lock = ref().lockState()) {
							if (m_serverState != ServerState.Binding) {
								LOG.error("BindListener got an operationComplete(failed) while serverState is {}", m_serverState.toString());
								return;
							}
							m_serverState = ServerState.Closed;
							m_serverConnection = null;
							StatusSystem.setInboundStatus(m_statusHostAndPort, StatusType.Down, statusText);
							NeuronApplication.logError(LOG, "Bind to port {} failed, taking neuron offline", m_port, cause);
							lock.addStateListener(NeuronState.Online, (success) -> {
								try(INeuronStateLock onlineLock = ref().lockState()) {
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
					m_completedPromise.setSuccess((Void)null);
				}
			}
		}
		
	}
	
	private class ServerCloseListener implements ChannelFutureListener {
		private boolean m_called;
		private Promise<Void> m_completePromise;
		
		
		synchronized void setCompletePromise(Promise<Void> promise) {
			if (m_called) {
				promise.setSuccess((Void)null);
			} else {
				m_completePromise = promise;
			}
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception
		{
			synchronized(InboundSocketNeuron.this) {
				try(INeuronStateLock lock = ref().lockState()) {
					if (m_serverState != ServerState.Bound && m_serverState != ServerState.Closing) {
						LOG.error("ServerCloseListener got an operationComplete(failed) while serverState is {}", m_serverState.toString());
						return;
					}
					
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
							try(INeuronStateLock onlineLock = ref().lockState()) {
								onlineLock.takeOffline();
							}
						});
					}
				} finally {
					synchronized(this) {
						m_called = true;
						if (m_completePromise != null) {
							m_completePromise.setSuccess((Void)null);
						}
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
		public enum MessageType { Connect, Data, Disconnect }; // TODO Add a ConnectionLsit event where I send all open connections.  Do this on a ReaderConnect event to the In pipe <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		public final MessageType messageType;
		public final Connection connection;
		public final ByteBuf data;
		
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
