package com.neuron.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.BytePipeSystem.IPipeWriterContext;
import com.neuron.core.NeuronStateManager.INeuronManagement;
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.socket.OutboundSocketNeuronTemplate;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.NeuronStateManagerTestUtils;
import com.neuron.core.test.TemplateStateManagerTestUtils;
import com.neuron.core.test.TestUtils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Promise;

public class OutboundSocketNeuron_Simple_Test {
	private static Promise<Void> m_testFuture; 

	@BeforeAll
	public static void init() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");

		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
		TemplateStateManagerTestUtils.registerAndBringOnline("OutboundSocketNeuronTemplate", OutboundSocketNeuronTemplate.class).awaitUninterruptibly();
	}

	@AfterAll
	public static void deinit() {
		TestUtils.printSystemStatuses();
		NeuronApplication.shutdown();
	}

	@Test
	void simpleTest() {
		m_testFuture = NeuronApplication.getTaskPool().next().newPromise();
		
		// Create server
		LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Create server");
		ServerBootstrap b = new ServerBootstrap();
		b.group(NeuronApplication.getIOPool(), NeuronApplication.getIOPool())
				.channel(NioServerSocketChannel.class) // (3)
				.childHandler(new ChannelInitializer<SocketChannel>() { // (4)
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info("]]]]]]]] Connected {}", ch.remoteAddress().toString());
						ch.pipeline().addLast(new MyChannelInboundHandlerAdapter());
						ch.closeFuture().addListener((f) -> {
							LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info("]]]]]]]] Closed");
						});
					}
				})
				.option(ChannelOption.SO_BACKLOG, 128) // (5)
				.childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

		ChannelFuture serverChannelF = b.bind(9999).syncUninterruptibly();
		assertTrue(serverChannelF.isSuccess());

		// Connect outbound socket neuron to server
		LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Create and bring OutboundSocket online");
		INeuronManagement nOutboundSocket = NeuronStateManager.registerNeuron("OutboundSocketNeuronTemplate", "OutboundSocket");
		assertTrue(nOutboundSocket.bringOnline(ObjectConfigBuilder.config()
				.option(OutboundSocketNeuronTemplate.Config_InetHost, "127.0.0.1")
				.option(OutboundSocketNeuronTemplate.Config_Port, 9999)
				.build()
			));
		assertTrue(NeuronStateManagerTestUtils.createFutureForState(nOutboundSocket.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));

		// Bring test neuron online
		LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Create and bring Test Neuron online");
		TemplateStateManagerTestUtils.registerAndBringOnline("TestWriterTemplate", TestWriterTemplate.class).awaitUninterruptibly();
		INeuronManagement nTest = NeuronStateManager.registerNeuron("TestWriterTemplate", "TestNeuron");
		assertTrue(nTest.bringOnline(ObjectConfigBuilder.config().build()));
		assertTrue(NeuronStateManagerTestUtils.createFutureForState(nTest.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));

		// Wait for test to complete
		LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Wait while test runs");
		assertTrue(m_testFuture.awaitUninterruptibly(15000));
		
		// Close server
		LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Disconnect server");
		serverChannelF.channel().disconnect().syncUninterruptibly();
		LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Close server");
		serverChannelF.channel().close().syncUninterruptibly();

		// Take down outbound socket
		LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Take test writer offline");
		TemplateStateManagerTestUtils.takeTemplateOffline("TestWriterTemplate");
		LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Take outbound socket offline");
		TemplateStateManagerTestUtils.takeTemplateOffline("OutboundSocketNeuronTemplate");
		
//		assertTrue(createFutureForState(nOutboundSocket.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
//		assertTrue(createFutureForState(nOutboundSocket.currentRef(), NeuronState.Offline).isSuccess());
		// assertTrue(NeuronStateManagerTestUtils.logContains(nMgt.currentRef(), "Neuron config item Config_InetHost is either missing or invalid"));
	}
	
	private static class MyChannelInboundHandlerAdapter extends ChannelInboundHandlerAdapter {
		private static final Logger LOG = LogManager.getLogger(MyChannelInboundHandlerAdapter.class);
		private final CompositeByteBuf m_localBuffer = NeuronApplication.allocateCompositeBuffer();
				
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
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
				outBuf.writeLong(cur+1);
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
			private IPipeWriterContext m_writerContext;
			private long m_expected = -1;
			private int m_numRW = 0;
			public Neuron(NeuronRef instanceRef) {
				super(instanceRef);
			}


			@Override
			public void connectResources() {
				m_writerContext = BytePipeSystem.writeToPipe("OutboundSocket", "Out", ObjectConfigBuilder.config().build(), (event, context) -> {
					// event will either be PipeEmpty or PipeWriteable... write on either
					LogManager.getLogger(Neuron.class).debug("Got event: {}", event.toString());
				});
				BytePipeSystem.readFromPipeAsAppendBuf("OutboundSocket", "In", ObjectConfigBuilder.config().build(), (buf) -> {
					while (true) {
						if (buf.readableBytes() < 8) {
							break;
						}
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
							try(BytePipeStreamWriter out = m_writerContext.openStreamWriter()) {
								final long writeThis = System.currentTimeMillis();
								m_expected = writeThis+1;
								out.writeLong(writeThis);
							} catch(Exception ex) {
								LogManager.getLogger(Neuron.class).error("", ex);
								m_testFuture.tryFailure(ex);
							}
						}
					}
				});
			}

			@Override
			public void nowOnline() {
				// This method is called after our system state becomes online, but before the caller to bringNeuronOnline
				// gets notified.
				
				// Start the test
				LogManager.getLogger(OutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Starting test");
				try(BytePipeStreamWriter out = m_writerContext.openStreamWriter()) {
					final long writeThis = System.currentTimeMillis();
					m_expected = writeThis+1;
					out.writeLong(writeThis);
				} catch(Exception ex) {
					LogManager.getLogger(Neuron.class).error("", ex);
					m_testFuture.tryFailure(ex);
				}
			}
		}
	}

}
