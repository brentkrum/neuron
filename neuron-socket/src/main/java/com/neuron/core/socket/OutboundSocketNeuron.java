package com.neuron.core.socket;

import java.net.BindException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.BytePipeSystem;
import com.neuron.core.DefaultNeuronInstanceBase;
import com.neuron.core.IBytePipeBufReaderCallback;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.ObjectConfigBuilder;
import com.neuron.core.StatusSystem;
import com.neuron.core.StatusSystem.StatusType;
import com.neuron.core.BytePipeSystem.IPipeWriterContext;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Promise;

public class OutboundSocketNeuron extends DefaultNeuronInstanceBase implements INeuronInitialization
{
	private static final Logger LOG = LogManager.getLogger(OutboundSocketNeuron.class);
	private enum ConnectionState { None, Connecting, Connected, Closing, Closed }
	private final Bootstrap m_channelBootstrap = new Bootstrap();
	private final CloseListener m_closeListener = new CloseListener();
	private final String m_inetHost;
	private final int m_port;
	private final String m_statusHostAndPort;
	private final long m_retryDelayInMS;
	
	private int m_repeatConnectError1 = 0;
	private boolean m_deinitializing;
	private Channel m_currentConnection;
	private IPipeWriterContext m_inPipeWriter;
	private ConnectionState m_connectionState = ConnectionState.None;
	
	public OutboundSocketNeuron(NeuronRef instanceRef, String inetHost, int port, long retryDelayInMS)
	{
		super(instanceRef);
		m_inetHost = inetHost;
		m_port = port;
		m_statusHostAndPort = m_inetHost + ":" + m_port;
		m_retryDelayInMS = retryDelayInMS;
	}

	@Override
	public void connectResources()
	{
		BytePipeSystem.configurePipeBroker("In", ObjectConfigBuilder.config().build());
		BytePipeSystem.configurePipeBroker("Out", ObjectConfigBuilder.config().build());
		
		BytePipeSystem.readFromPipeAsChunk("Out", ObjectConfigBuilder.config().build(), new OutPipeReader());
		
		m_inPipeWriter = BytePipeSystem.writeToPipe("In", ObjectConfigBuilder.config().build(), (event, context) -> {
			// No need to care about these events
		});
	}

	@Override
	public void init(Promise<Void> initPromise)
	{
		m_channelBootstrap.group(NeuronApplication.getIOPool())
			.channel(NioSocketChannel.class)
			.option(ChannelOption.SO_KEEPALIVE, true)
			.remoteAddress(m_inetHost, m_port);
	
		// Need to only include this IF the log level is trace
		if (LOG.isTraceEnabled()) {
			m_channelBootstrap.handler(new LoggingHandler(OutboundSocketNeuron.class, LogLevel.TRACE));
		}
		
		m_channelBootstrap.handler(new InboundDataHandler());
		
		m_channelBootstrap.register().addListener((regFuture) -> {
			if (regFuture.isSuccess()) {
				initPromise.setSuccess((Void)null);
			} else {
				initPromise.setFailure(regFuture.cause());
			}
		});
		
		try(INeuronStateLock lock = ref().lockState()) {
			lock.addStateAsyncListener(NeuronState.SystemOnline, (success, neuronRef, completedPromise) -> {
				if (success) {
					synchronized(OutboundSocketNeuron.this) {
						m_connectionState = ConnectionState.Connecting;
					}
					NeuronApplication.logInfo(LOG, "Connecting to {}:{}", m_inetHost, m_port);
					m_channelBootstrap.connect().addListener(new ConnectListener(completedPromise));
				} else {
					completedPromise.setSuccess((Void)null);
				}
			});
		}
	}
	
	@Override
	public void deinit(Promise<Void> promise) {
		synchronized(OutboundSocketNeuron.this) {
			m_deinitializing = true;
			if (m_connectionState == ConnectionState.Connected) {
				try(INeuronStateLock lock = ref().lockState()) {
					NeuronApplication.logInfo(LOG, "Closing connection");
				}
				m_closeListener.setCompletePromise(promise);
				m_currentConnection.close();
				m_connectionState = ConnectionState.Closing;
				return;
			} else if (m_connectionState == ConnectionState.Connecting) {
				try(INeuronStateLock lock = ref().lockState()) {
					NeuronApplication.logInfo(LOG, "Aborting current connection attempt in progress");
				}
				m_closeListener.setCompletePromise(promise);
				m_currentConnection.close();
				m_connectionState = ConnectionState.Closing;
				return;
			} else if (m_connectionState == ConnectionState.Closing) {
				m_closeListener.setCompletePromise(promise);
			}
		}
		promise.setSuccess((Void)null);
	}
	
	private class OutPipeReader implements IBytePipeBufReaderCallback {

		@Override
		public void onData(ByteBuf buf) {
			synchronized(OutboundSocketNeuron.this) {
				if (m_connectionState != ConnectionState.Connected) {
					if (LOG.isTraceEnabled()) {
						LOG.trace("{}:{} m_connectionState({}) != ConnectionState.Connected", m_inetHost, m_port, m_connectionState);
					}
					return;
				}
				buf.retain();
				if (LOG.isTraceEnabled()) {
					LOG.trace("{}:{} writeAndFlush", m_inetHost, m_port);
				}
				m_currentConnection.writeAndFlush(buf);
			}
		}
		
	}

	@Sharable
	private class InboundDataHandler extends ChannelInboundHandlerAdapter {

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg)
		{
			if (msg instanceof ByteBuf) {
				final IPipeWriterContext inPipeWriter = m_inPipeWriter;
				if (inPipeWriter != null && inPipeWriter.offerBuf((ByteBuf)msg)) {
					return;
				}
			}
			ReferenceCountUtil.release(msg);
		}
//
//		@Override
//		public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//			LOG.info("}}}}}}}} channelInactive");
//		}
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
		{
			synchronized(OutboundSocketNeuron.this) {
				if (m_deinitializing) {
					return;
				}
				if (m_connectionState == ConnectionState.Connected) {
					m_connectionState = ConnectionState.Closing;
					try(INeuronStateLock lock = ref().lockState()) {
						NeuronApplication.logInfo(LOG, "Exception with outbound socket, closing", cause);
					}
					// close listener will deal with retrying and stuff
					ctx.close();
				} else {
					try(INeuronStateLock lock = ref().lockState()) {
						NeuronApplication.logInfo(LOG, "Exception with outbound socket", cause);
					}
				}
			}
		}
	}
	
	private class ConnectListener implements ChannelFutureListener {
		private final Promise<Void> m_completedPromise;
		
		ConnectListener(Promise<Void> completedPromise) {
			m_completedPromise = completedPromise;
		}
		
		ConnectListener() {
			m_completedPromise = null;
		}
		
		@Override
		public void operationComplete(ChannelFuture future) throws Exception
		{
			try {
				if (future.isSuccess()) {
					synchronized(OutboundSocketNeuron.this) {
						m_repeatConnectError1 = 0;
						if (m_deinitializing) {
							return;
						}
						if (m_connectionState != ConnectionState.Connecting) {
							try(INeuronStateLock lock = ref().lockState()) {
								LOG.error("ConnectListener got an operationComplete(success) while connectionState is {}", m_connectionState.toString());
							}
							return;
						}						
						m_connectionState = ConnectionState.Connected;
						m_currentConnection = future.channel();
						try(INeuronStateLock lock = ref().lockState()) {
							NeuronApplication.logInfo(LOG, "Connected");
							StatusSystem.setHostStatus(m_statusHostAndPort, StatusType.Up, "Connected");
						}
						m_currentConnection.closeFuture().addListener(m_closeListener);
					}
					
				} else {
					String statusText = future.cause().getMessage();
					final boolean takeOffline;
					Throwable cause = future.cause(); 
					if (cause instanceof UnknownHostException) {
						takeOffline = true;
					} else if (cause instanceof SocketException && cause.getCause() instanceof BindException) {
						takeOffline = true;
						cause = cause.getCause();
					} else if (cause instanceof ConnectException && cause.getCause() instanceof ConnectException) {
						takeOffline = false;
						cause = cause.getCause();
					} else {
						takeOffline = false;
					}
					if (statusText == null) {
						statusText = cause.getClass().getSimpleName();
					}
					// There is no close listener, so we only have to worry about ourselves
					future.channel().close();
					synchronized(OutboundSocketNeuron.this) {
						if (m_deinitializing) {
							return;
						}
						try(INeuronStateLock lock = ref().lockState()) {
							if (m_connectionState != ConnectionState.Connecting) {
								LOG.error("ConnectListener got an operationComplete(failed) while connectionState is {}", m_connectionState.toString());
								return;
							}
							m_connectionState = ConnectionState.Closed;
							m_currentConnection = null;
							if (takeOffline) {
								NeuronApplication.logError(LOG, "Connection to {}:{} failed, taking neuron offline", m_inetHost, m_port, cause);
								lock.addStateListener(NeuronState.Online, (success) -> {
									try(INeuronStateLock onlineLock = ref().lockState()) {
										onlineLock.takeOffline();
									}
								});
								return;
							} else {
								if (m_repeatConnectError1 == 0) {
									NeuronApplication.logError(LOG, "Connection to {}:{} failed, but will continue to retry every {} ms", m_inetHost, m_port, m_retryDelayInMS, cause);
								}
								m_repeatConnectError1++;
							}
						}
						StatusSystem.setHostStatus(m_statusHostAndPort, StatusType.Down, statusText + ". Retrying every " + m_retryDelayInMS + " ms.");
						
						// send a message in the pipe alerting to the disconnected state?
						startRetryTimer();
					}
				}
			} finally {
				if (m_completedPromise != null) {
					m_completedPromise.setSuccess((Void)null);
				}
			}
		}
		
	}
	
	private class CloseListener implements ChannelFutureListener {
		private boolean m_called;
		private Promise<Void> m_completePromise;
		
		
		synchronized void setCompletePromise(Promise<Void> promise) {
			if (m_called) {
				promise.setSuccess((Void)null);
			} else {
				m_completePromise = promise;
			}
		}
		
		public synchronized void reset() {
			m_called = false;
			m_completePromise = null;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception
		{
			synchronized(OutboundSocketNeuron.this) {
				try(INeuronStateLock lock = ref().lockState()) {
					if (m_connectionState != ConnectionState.Connected && m_connectionState != ConnectionState.Closing) {
						LOG.error("CloseListener got an operationComplete(failed) while connectionState is {}", m_connectionState.toString());
						return;
					}
					NeuronApplication.logInfo(LOG, "Connection closed");
					
					// Channel has closed
					m_connectionState = ConnectionState.Closed;
					m_currentConnection = null;
					
					// if we are shutting down / unloading
					if (m_deinitializing) {
						StatusSystem.setHostStatus(m_statusHostAndPort, StatusType.Down, "Disconnected, neuron deinitialized");
					} else {
						StatusSystem.setHostStatus(m_statusHostAndPort, StatusType.Down, "Disconnected unexpectedly, will continue to retry");
						// send a message in the pipe alerting to the disconnected state?
						startRetryTimer();
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
	
	private void startRetryTimer() {
		NeuronApplication.getTaskPool().schedule(() -> {
			try {
				synchronized(OutboundSocketNeuron.this) {
					if (m_connectionState != ConnectionState.Closed) {
						LOG.warn("RetryTimer got called-back while connectionState is {}.  Expected Closed.  RetryTimer is shutting down.", m_connectionState.toString());
						return;
					}
					try(INeuronStateLock lock = ref().lockState()) {
						if (!lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online)) {
							LOG.info("Neuron state is {}, RetryTimer is shutting down.", lock.currentState());
							return;
						}
					}
					
					m_closeListener.reset();
					m_connectionState = ConnectionState.Connecting;
				}
//				LOG.info("Connecting to {}:{}", m_inetHost, m_port);
				m_channelBootstrap.connect().addListener(new ConnectListener());
			} catch(Exception ex) {
				LOG.error("Unrecoverable exception in retry", ex);
			}
		}, m_retryDelayInMS, TimeUnit.MILLISECONDS); 
	}
}
