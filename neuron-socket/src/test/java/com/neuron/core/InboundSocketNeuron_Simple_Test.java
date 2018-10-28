package com.neuron.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.MessagePipeSystem.IPipeWriterContext;
import com.neuron.core.NeuronStateSystem.INeuronManagement;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.socket.InboundSocketNeuron;
import com.neuron.core.socket.InboundSocketNeuronTemplate;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.NeuronStateTestUtils;
import com.neuron.core.test.TemplateStateTestUtils;
import com.neuron.core.test.TestUtils;
import com.neuron.utility.CharSequenceTrie;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Promise;

public class InboundSocketNeuron_Simple_Test {
	private static Promise<Void> m_testFuture; 

	@BeforeAll
	public static void init() {
		System.setProperty("io.netty.leakDetection.level", "PARANOID");
		System.setProperty("io.netty.leakDetection.targetRecords", "1");
		
//		System.setProperty("logger.com.neuron.core.MessagePipeSystem", "DEBUG");
//		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		System.setProperty("logger.com.neuron.core.socket.InboundSocketNeuron", "DEBUG");

		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
		TemplateStateTestUtils.registerAndBringOnline("InboundSocketNeuronTemplate", InboundSocketNeuronTemplate.class).awaitUninterruptibly();
	}

	@AfterAll
	public static void deinit() {
		TestUtils.printSystemStatuses();
		NeuronApplication.shutdown();
	}

	@Test
	void simpleTest() {
		m_testFuture = NeuronApplication.getTaskPool().next().newPromise();

		
		LogManager.getLogger(InboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Create and bring OutboundSocket online");
		INeuronManagement nInboundSocket = NeuronStateSystem.registerNeuron("InboundSocketNeuronTemplate", "InboundSocketServer");
		assertTrue(nInboundSocket.bringOnline(ObjectConfigBuilder.config()
				.option(InboundSocketNeuronTemplate.Config_Port, 9999)
				.build()
			));
		assertTrue(NeuronStateTestUtils.createFutureForState(nInboundSocket.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));
		
		
		// Create server
		LogManager.getLogger(InboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Create client");
		Bootstrap b = new Bootstrap();
		b.group(NeuronApplication.getIOPool())
			.channel(NioSocketChannel.class)
			.option(ChannelOption.SO_KEEPALIVE, true)
			.remoteAddress("localhost", 9999)
			.handler(new MyChannelInboundHandlerAdapter());

		
		LogManager.getLogger(InboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Create and bring Test Neuron online");
		TemplateStateTestUtils.registerAndBringOnline("TestWriterTemplate", TestWriterTemplate.class).awaitUninterruptibly();
		INeuronManagement nTest = NeuronStateSystem.registerNeuron("TestWriterTemplate", "TestNeuron");
		assertTrue(nTest.bringOnline(ObjectConfigBuilder.config().build()));
		assertTrue(NeuronStateTestUtils.createFutureForState(nTest.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));

		
		LogManager.getLogger(InboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Connect client");
		ChannelFuture clientChannelF = b.connect().syncUninterruptibly();
		assertTrue(clientChannelF.isSuccess());
		
		
		// Wait for test to complete
		LogManager.getLogger(InboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Wait while test runs");
		assertTrue(m_testFuture.awaitUninterruptibly(15000));
		
		// Close server
		LogManager.getLogger(InboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Disconnect client");
		clientChannelF.channel().disconnect().syncUninterruptibly();
		LogManager.getLogger(InboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Close client");
		clientChannelF.channel().close().syncUninterruptibly();

		// Take down outbound socket
		LogManager.getLogger(InboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Take test writer offline");
		TemplateStateTestUtils.takeTemplateOffline("TestWriterTemplate");
		LogManager.getLogger(InboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Take outbound socket offline");
		TemplateStateTestUtils.takeTemplateOffline("InboundSocketNeuronTemplate");
		
//		assertTrue(createFutureForState(nOutboundSocket.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
//		assertTrue(createFutureForState(nOutboundSocket.currentRef(), NeuronState.Offline).isSuccess());
		// assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "Neuron config item Config_InetHost is either missing or invalid"));
	}
	
	@Sharable
	private static class MyChannelInboundHandlerAdapter extends ChannelInboundHandlerAdapter {
		private static final Logger LOG = LogManager.getLogger(MyChannelInboundHandlerAdapter.class);
		private final CompositeByteBuf m_localBuffer = NeuronApplication.allocateCompositeBuffer();
				
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			if (!(msg instanceof ByteBuf)) {
				return;
			}
			m_localBuffer.addComponent(true, (ByteBuf)msg);
			while(true) {
				if (m_localBuffer.readableBytes() < 8) {
					break;
				}
				long cur = m_localBuffer.readLong();
				final ByteBuf outBuf = ctx.alloc().ioBuffer();
				outBuf.writeLong(cur);
				ctx.writeAndFlush(outBuf);
			}
			m_localBuffer.discardReadBytes();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
			// Close the connection when an exception is raised.
			LOG.error("exceptionCaught()", cause);
			ctx.close();
		}
	}

	
	public static class TestWriterTemplate extends DefaultTestNeuronTemplateBase {
		public TestWriterTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new Neuron(ref);
		}
		
		private static class Neuron extends DefaultNeuronInstanceBase {
			private static final Logger LOG = LogManager.getLogger(Neuron.class);
			private final CharSequenceTrie<ClientConnection> m_conns = new CharSequenceTrie<>();
			private IPipeWriterContext m_writerContext;
			private long m_expected = -1;
			private int m_numRW = 0;
			
			public Neuron(NeuronRef instanceRef) {
				super(instanceRef);
			}


			@Override
			public void connectResources() {
				m_writerContext = MessagePipeSystem.writeToPipe("InboundSocketServer", "Out", ObjectConfigBuilder.config().build(), (event, context) -> {
					// event will either be PipeEmpty or PipeWriteable... write on either
					if (LOG.isTraceEnabled()) {
						LOG.trace("Got event: {}", event.toString());
					}
				});
				MessagePipeSystem.readFromPipe("InboundSocketServer", "In", ObjectConfigBuilder.config().build(), (pipeMsg) -> {
					InboundSocketNeuron.InboundMessage msg = (InboundSocketNeuron.InboundMessage)pipeMsg;
					if (msg.messageType == InboundSocketNeuron.InboundMessage.MessageType.Connect) {
						ClientConnection c;
						m_conns.addOrFetch(msg.connection.toString(), c = new ClientConnection(msg.connection));
						LOG.info("Connected: {}", msg.connection.toString());
						writeMessage(c.m_connection, System.currentTimeMillis());
						
					} else if (msg.messageType == InboundSocketNeuron.InboundMessage.MessageType.Disconnect) {
						m_conns.remove(msg.connection.toString());
						LOG.info("Disconnected: {}", msg.connection.toString());
							
					} else {
						if (LOG.isTraceEnabled()) {
							LOG.trace("~~~~~ readFromPipe-Data");
						}
						
						ClientConnection c = m_conns.get(msg.connection.toString());
						if (c == null) {
							LOG.info("Data message from disconnected client: {}", msg.connection.toString());
							return;
						}
						c.m_localBuffer.addComponent(true, msg.data.retain());
						
						ByteBuf buf = c.m_localBuffer;
						if (buf.readableBytes() >= 8) {
							long cur = buf.readLong();
							
							if (cur != m_expected) {
								LogManager.getLogger(Neuron.class).error(m_expected + " != " + cur);
								m_testFuture.tryFailure(new RuntimeException("NeuronB cur[" + cur + "] did not match expected[" + m_expected + "]"));
								return;
							}
							m_numRW++;
							if (m_numRW >= 25000) {
								m_testFuture.trySuccess((Void)null);
							} else {
								writeMessage(c.m_connection, cur+1);
							}
						}
						c.m_localBuffer.discardReadBytes();
					}
				});
			}

			private void writeMessage(InboundSocketNeuron.Connection connection, long num) {
				final ByteBuf outBuf = NeuronApplication.allocateIOBuffer();
				InboundSocketNeuron.OutboundMessage outMsg = new InboundSocketNeuron.OutboundMessage(connection, outBuf);
				m_expected = num;
				outBuf.writeLong(num);
				if (!m_writerContext.offer(outMsg)) {
					LOG.error("Failed to offer message to pipe");
					m_testFuture.tryFailure(new RuntimeException("Failed to offer message to pipe"));
					outMsg.release();
				}
			}
			
			private class ClientConnection {
				private final CompositeByteBuf m_localBuffer = NeuronApplication.allocateCompositeBuffer();
				private final InboundSocketNeuron.Connection m_connection;
				
				ClientConnection(InboundSocketNeuron.Connection connection) {
					m_connection = connection;
				}
			}
			
		}
	}

}
