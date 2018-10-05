package com.neuron.core.socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.BytePipeSystem;
import com.neuron.core.DefaultNeuronInstanceBase;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.ObjectConfigBuilder;
import com.neuron.core.BytePipeSystem.IPipeWriterContext;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.NeuronRef.INeuronStateLock;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
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
	private final Bootstrap m_channelBootstrap = new Bootstrap();
	private final ConnectListener m_connectListener = new ConnectListener();
	private final CloseListener m_closeListener = new CloseListener();
	private String m_inetHost;
	private int m_port;
	
	private Channel m_currentConnection;
	private IPipeWriterContext m_inPipeWriter;
	
	public OutboundSocketNeuron(NeuronRef instanceRef, String inetHost, int port)
	{
		super(instanceRef);
		m_inetHost = inetHost;
		m_port = port;
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
		
		initPromise.setSuccess((Void)null);
	}

	@Override
	public void connectResources()
	{
		BytePipeSystem.configurePipeBroker("In", ObjectConfigBuilder.config());
		BytePipeSystem.configurePipeBroker("Out", ObjectConfigBuilder.config());
		
		BytePipeSystem.readFromPipeAsChunk("Out", ObjectConfigBuilder.config(), (buf) -> {
			try(INeuronStateLock lock = ref().lockState()) {
				synchronized(OutboundSocketNeuron.this) {
					if (m_currentConnection != null) {
						m_currentConnection.writeAndFlush(buf);
					}
				}
			} catch(Exception ex) {
				NeuronApplication.logError(LOG, "Unexpected exception", ex);
			}
		});
		
		m_inPipeWriter = BytePipeSystem.writeToPipe("In", ObjectConfigBuilder.config(), (event, context) -> {
			// No need to care about these events
		});
	}
	
	@Override
	public void deinit(Promise<Void> promise) {
		synchronized(OutboundSocketNeuron.this) {
			if (m_currentConnection != null) {
				m_currentConnection.close().addListener((f) -> {
					promise.setSuccess((Void)null);
				});
				return;
			}
		}
		promise.setSuccess((Void)null);
	}

	@Override
	public void nowOnline() {
		m_channelBootstrap.connect().addListener(m_connectListener);
	}

	@ChannelHandler.Sharable
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

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
		{
			try(INeuronStateLock lock = ref().lockState()) {
				NeuronApplication.logInfo(LOG, "Exception with outbound socket, closing", cause);
			}
			// send a message in the pipe alerting to the disconnected state?
			// report the connection/this neuron as down/disconnected -- we should have a place to report things that are "down"
			ctx.close();
			// close listener will deal with retrying and stuff
		}
	}
	
	private class ConnectListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception
		{
			if (future.isSuccess()) {
				m_currentConnection = future.channel();
				m_currentConnection.closeFuture().addListener(m_closeListener);
				// report the connection/this neuron as up/connected -- we should have a place to report things that are "up"
			} else {
				future.channel().close();
				// send a message in the pipe alerting to the disconnected state?
				// report the connection/this neuron as down/disconnected -- we should have a place to report things that are "down"
				// set timer to keep retrying
			}
		}
		
	}
	
	private class CloseListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception
		{
			m_currentConnection = null;
			// Channel has closed
			// if we are shutting down / unloading
			// 	report the connection/this neuron as down/disconnected due to neuron unload
			// else
			// 	send a message in the pipe alerting to the disconnected state?
			// 	report the connection/this neuron as down/disconnected -- we should have a place to report things that are "down"
			//		set a timer to keep retrying
		}
		
	}
}
