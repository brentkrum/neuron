package com.neuron.core;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.INeuronManagement;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.socket.InboundSocketNeuron;
import com.neuron.core.socket.InboundSocketNeuronTemplate;
import com.neuron.core.socket.OutboundSocketNeuronTemplate;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.NeuronStateTestUtils;
import com.neuron.core.test.TemplateStateTestUtils;
import com.neuron.core.test.TestUtils;
import com.neuron.utility.CharSequenceTrie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Promise;

public class InboundOutboundSocketNeuron_Simple_Test {
	private static Promise<Void> m_testOutboundSocketRead; 
	private static Promise<Void> m_testInboundSocketRead; 
	private static int m_outboundSocketReadCount = 0;
	private static int m_inboundSocketReadCount = 0;

	@BeforeAll
	public static void init() {
//		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
//		System.setProperty("logger.com.neuron.core.BytePipeSystem", "TRACE");
//		System.setProperty("logger.com.neuron.core.BytePipeBufReader", "TRACE");
//		System.setProperty("logger.com.neuron.core.socket.InboundSocketNeuron", "DEBUG");
		System.setProperty("io.netty.leakDetection.level", "PARANOID");
		System.setProperty("io.netty.leakDetection.targetRecords", "1");

		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
		TemplateStateTestUtils.registerAndBringOnline("OutboundSocketNeuronTemplate", OutboundSocketNeuronTemplate.class).awaitUninterruptibly();
		TemplateStateTestUtils.registerAndBringOnline("InboundSocketNeuronTemplate", InboundSocketNeuronTemplate.class).awaitUninterruptibly();
	}

	@AfterAll
	public static void deinit() {
		TestUtils.printSystemStatuses();
		NeuronApplication.shutdown();
	}

	@Test
	void simpleTest() {
		m_testOutboundSocketRead = NeuronApplication.getTaskPool().next().newPromise();
		m_testInboundSocketRead = NeuronApplication.getTaskPool().next().newPromise();
		
		// Create server
		LogManager.getLogger(InboundOutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Create and bring InboundSocket online");
		INeuronManagement nInboundSocket = NeuronStateSystem.registerNeuron("InboundSocketNeuronTemplate", "InboundSocket");
		assertTrue(nInboundSocket.bringOnline(ObjectConfigBuilder.config()
				.option(InboundSocketNeuronTemplate.Config_Port, 9999)
				.option(InboundSocketNeuronTemplate.Config_InPipeMaxPipeMsgCount, 1024)
				.build()
			));
		assertTrue(NeuronStateTestUtils.createFutureForState(nInboundSocket.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));

		// Connect outbound socket neuron to server
		LogManager.getLogger(InboundOutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Create and bring OutboundSocket online");
		INeuronManagement nOutboundSocket = NeuronStateSystem.registerNeuron("OutboundSocketNeuronTemplate", "OutboundSocket");
		assertTrue(nOutboundSocket.bringOnline(ObjectConfigBuilder.config()
				.option(OutboundSocketNeuronTemplate.Config_InetHost, "127.0.0.1")
				.option(OutboundSocketNeuronTemplate.Config_Port, 9999)
				.build()
			));
		assertTrue(NeuronStateTestUtils.createFutureForState(nOutboundSocket.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));

		// Bring test neuron online
		LogManager.getLogger(InboundOutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Create and bring Test Neuron online");
		TemplateStateTestUtils.registerAndBringOnline("TestWriterTemplate", TestWriterTemplate.class).awaitUninterruptibly();
		INeuronManagement nTest = NeuronStateSystem.registerNeuron("TestWriterTemplate", "TestNeuron");
		assertTrue(nTest.bringOnline(ObjectConfigBuilder.config().build()));
		assertTrue(NeuronStateTestUtils.createFutureForState(nTest.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));

		// Wait for test to complete
		LogManager.getLogger(InboundOutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Wait while test runs");
		m_testOutboundSocketRead.awaitUninterruptibly(5000);
		if (m_testOutboundSocketRead.cause() != null) {
			m_testOutboundSocketRead.cause().printStackTrace();
			fail("m_testOutboundSocketRead failed");
		}
		if (m_testInboundSocketRead.cause() != null) {
			m_testInboundSocketRead.cause().printStackTrace();
			fail("m_testInboundSocketRead failed");
		}
		
		LogManager.getLogger(InboundOutboundSocketNeuron_Simple_Test.class).info("outboundSocketReadCount={} inboundSocketReadCount={}", m_outboundSocketReadCount, m_inboundSocketReadCount);
		
		// Take down outbound socket
		LogManager.getLogger(InboundOutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Take test writer offline");
		TemplateStateTestUtils.takeTemplateOffline("TestWriterTemplate").syncUninterruptibly();
		LogManager.getLogger(InboundOutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Take outbound socket offline");
		TemplateStateTestUtils.takeTemplateOffline("OutboundSocketNeuronTemplate").syncUninterruptibly();
		LogManager.getLogger(InboundOutboundSocketNeuron_Simple_Test.class).info(">>>>>>>> Take inbound socket offline");
		TemplateStateTestUtils.takeTemplateOffline("InboundSocketNeuronTemplate").syncUninterruptibly();
		
//		assertTrue(createFutureForState(nOutboundSocket.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
//		assertTrue(createFutureForState(nOutboundSocket.currentRef(), NeuronState.Offline).isSuccess());
		// assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "Neuron config item Config_InetHost is either missing or invalid"));
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
//			private static final Logger LOG = LogManager.getLogger(Neuron.class);
			private BytePipeSystem.IPipeWriterContext m_outboundWriterContext;
			private MessagePipeSystem.IPipeWriterContext m_inboundWriterContext;
			
			public Neuron(NeuronRef instanceRef) {
				super(instanceRef);
			}


			@Override
			public void connectResources() {
				OutboundSocketReaderCallback outboundSocketReaderCallback = new OutboundSocketReaderCallback();
				BytePipeSystem.readFromPipeAsAppendBuf("OutboundSocket", "In", ObjectConfigBuilder.config().build(), outboundSocketReaderCallback);
				m_outboundWriterContext = BytePipeSystem.writeToPipe("OutboundSocket", "Out", ObjectConfigBuilder.config().build(), (event, context) -> {
					// event will either be PipeEmpty or PipeWriteable... write on either
//					if (LOG.isInfoEnabled()) {
//						LOG.info("Got event: {}", event.toString());
//					}
					outboundSocketReaderCallback.m_writer.requestMoreWork();
				});
				
				InboundSocketReaderCallback inboundSocketReaderCallback = new InboundSocketReaderCallback();
				m_inboundWriterContext = MessagePipeSystem.writeToPipe("InboundSocket", "Out", ObjectConfigBuilder.config().build(), (event, context) -> {
//					if (LOG.isInfoEnabled()) {
//						LOG.info("Got event: {}", event.toString());
//					}
					inboundSocketReaderCallback.outboundWriterPipeEmpty();
				});
				MessagePipeSystem.readFromPipe("InboundSocket", "In", ObjectConfigBuilder.config().build(), inboundSocketReaderCallback);
			}
			
			private class OutboundSocketReaderCallback implements IBytePipeBufReaderCallback {
				private final Logger LOG = LogManager.getLogger(OutboundSocketReaderCallback.class);
				private final WriteWorker m_writer = new WriteWorker();
				private long m_outboundSocketReadExpected = 1;
				private long m_toWrite = 1;

				@Override
				public void onData(ByteBuf buf) {
					if (m_testOutboundSocketRead.isDone()) {
						return;
					}
					while (buf.readableBytes() >= 8) {
						long cur = buf.readLong();
						if (cur != m_outboundSocketReadExpected) {
							LOG.error(m_outboundSocketReadExpected + " != " + cur);
							m_testOutboundSocketRead.tryFailure(new RuntimeException("Neuron cur[" + cur + "] did not match expected[" + m_outboundSocketReadExpected + "]"));
							return;
						}
						m_outboundSocketReadExpected++;
						m_outboundSocketReadCount++;
					}
				}
				
				private class WriteWorker extends PerpetualWorkContextAware {

					@Override
					protected void _lockException(Exception ex) {
						LOG.fatal("", ex);
						m_testOutboundSocketRead.tryFailure(ex);
					}

					@Override
					protected void _doWork(INeuronStateLock neuronLock) {
						for(int i=0; i<10; i++) {
							final ByteBuf buf = NeuronApplication.allocateIOBuffer();
							buf.writeLong(m_toWrite);
							if (!m_outboundWriterContext.offerBuf(buf)) {
								buf.release();
								break;
							}
							m_toWrite++;
						}
						requestMoreWork();
					}
					
				}
				
			}
			
			private class InboundSocketReaderCallback implements IMessagePipeReaderCallback {
				private final Logger LOG = LogManager.getLogger(InboundSocketReaderCallback.class);
				private final ReadWriteLock m_rwLock = new ReentrantReadWriteLock(true);
				private final CharSequenceTrie<ClientConnection> m_conns = new CharSequenceTrie<>();
				
				void outboundWriterPipeEmpty() {
					m_rwLock.readLock().lock();
					try {
						m_conns.forEach((key, value) -> {
							value.m_writer.requestMoreWork();
							return true;
						});
					} finally {
						m_rwLock.readLock().unlock();
					}
				}
				
				private ClientConnection connectClient(InboundSocketNeuron.InboundMessage msg) {
					final ClientConnection c;
					m_rwLock.writeLock().lock();
					try {
						m_conns.addOrFetch(msg.connection.toString(), c = new ClientConnection(msg.connection));
					} finally {
						m_rwLock.writeLock().unlock();
					}
					LOG.info("Connected: {}", msg.connection.toString());
					return c;
				}
				
				@Override
				public void onData(ReferenceCounted pipeMsg) {
					InboundSocketNeuron.InboundMessage msg = (InboundSocketNeuron.InboundMessage)pipeMsg;
					if (msg.messageType == InboundSocketNeuron.InboundMessage.MessageType.Connect) {
						final ClientConnection c = connectClient(msg);
						c.m_writer.requestMoreWork();
						
					} else if (msg.messageType == InboundSocketNeuron.InboundMessage.MessageType.Disconnect) {
						final ClientConnection c;
						m_rwLock.writeLock().lock();
						try {
							c = m_conns.remove(msg.connection.toString());
						} finally {
							m_rwLock.writeLock().unlock();
						}
						LOG.info("Disconnected: {}", msg.connection.toString());
						c.disconnected();
						
					} else {
						ClientConnection c;
						m_rwLock.readLock().lock();
						try {
							c = m_conns.get(msg.connection.toString());
						} finally {
							m_rwLock.readLock().unlock();
						}
						if (c == null) {
							// Client connected before we attached to pipe
							c = connectClient(msg);
							c.m_writer.requestMoreWork();
							
						} else if (c.m_disconnected) {
							LOG.info("Data message from disconnected client: {}", msg.connection.toString());
							return;
						}
						
						c.read(msg.data.retain());
					}
				}
				
				private class ClientConnection {
					private final CompositeByteBuf m_localBuffer = NeuronApplication.allocateCompositeBuffer();
					private final WriteWorker m_writer = new WriteWorker();
					private final InboundSocketNeuron.Connection m_connection;
					private long m_inboundSocketReadExpected = 1;
					private long m_toWrite = 1;
					private volatile boolean m_disconnected;
					
					ClientConnection(InboundSocketNeuron.Connection connection) {
						m_connection = connection;
					}
					
					public void read(ByteBuf data) {
						if (m_testInboundSocketRead.isDone()) {
							data.release();
							return;
						}
						m_localBuffer.addComponent(true, data);
						
						ByteBuf buf = m_localBuffer;
						while (buf.readableBytes() >= 8) {
							long cur = buf.readLong();
							
							if (cur != m_inboundSocketReadExpected) {
								LOG.error(m_inboundSocketReadExpected + " != " + cur);
								m_testInboundSocketRead.tryFailure(new RuntimeException("Neuron cur[" + cur + "] did not match expected[" + m_inboundSocketReadExpected + "]"));
								return;
							}
							m_inboundSocketReadCount++;
							m_inboundSocketReadExpected++;
						}
						m_localBuffer.discardReadBytes();
					}

					void disconnected() {
						m_disconnected = true;
					}
					
					private class WriteWorker extends PerpetualWorkContextAware {

						@Override
						protected void _lockException(Exception ex) {
							LOG.fatal("", ex);
							m_testOutboundSocketRead.tryFailure(ex);
						}

						@Override
						protected void _doWork(INeuronStateLock neuronLock) {
							for(int i=0; i<10; i++) {
								if (m_disconnected) {
									return;
								}
								final ByteBuf buf = NeuronApplication.allocateIOBuffer();
								buf.writeLong(m_toWrite);
								final InboundSocketNeuron.OutboundMessage outMsg = new InboundSocketNeuron.OutboundMessage(m_connection, buf);
								if (!m_inboundWriterContext.offer(outMsg)) {
									outMsg.release();
									break;
								}
								m_toWrite++;
							}
							requestMoreWork();
						}
						
					}
				}
				
			}
		}
	}

}
