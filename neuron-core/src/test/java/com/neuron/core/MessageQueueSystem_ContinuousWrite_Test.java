package com.neuron.core;

import static com.neuron.core.test.NeuronStateManagerTestUtils.createFutureForState;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateManager.INeuronManagement;
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateStateManager.ITemplateManagement;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.TemplateStateManagerTestUtils;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Promise;

public class MessageQueueSystem_ContinuousWrite_Test {
	@BeforeAll
	public static void init() {
//		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		System.setProperty("logger.com.neuron.core.MessageQueueSystem", "TRACE");
//		System.setProperty("com.neuron.core.NeuronThreadContext.leakDetection", "true");
		
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
	}
	
	@AfterAll
	public static void deinit() {
		NeuronApplication.shutdown();
	}


	private static Promise<Void> m_testFuture; 
	private static long m_expected = 0;
	
	@Test
	public void testReadWrite() {
		m_testFuture = NeuronApplication.getTaskPool().next().newPromise();
		
		ITemplateManagement tMgt = TemplateStateManager.registerTemplate("RWTestTemplateA", RWTestTemplateA.class);
		assertTrue(TemplateStateManagerTestUtils.bringTemplateOnline(tMgt).syncUninterruptibly().isSuccess());
		
		INeuronManagement nMgt = NeuronStateManager.registerNeuron(tMgt.currentRef(), "NeuronA");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");

				
		ITemplateManagement tMgtB = TemplateStateManager.registerTemplate("RWTestTemplateB", RWTestTemplateB.class);
		INeuronManagement nMgtB = NeuronStateManager.registerNeuron("RWTestTemplateB", "NeuronB");

		for(int i=0; i<100; i++) {
			LogManager.getLogger(MessageQueueSystem_ContinuousWrite_Test.class).info("Bring Template B online");
			assertTrue(TemplateStateManagerTestUtils.bringTemplateOnline(tMgtB).syncUninterruptibly().isSuccess());
			
			LogManager.getLogger(MessageQueueSystem_ContinuousWrite_Test.class).info("Bring Neuron B online");
			assertTrue(nMgtB.bringOnline(ObjectConfigBuilder.config().build()));
			assertTrue(createFutureForState(nMgtB.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron B to enter Online state");
			
			LogManager.getLogger(MessageQueueSystem_ContinuousWrite_Test.class).info("Running test - {}", i);
			// Let the test run for 25ms
			m_testFuture.awaitUninterruptibly(25);
			if (m_testFuture.isDone() && !m_testFuture.isSuccess()) {
				Assertions.fail(m_testFuture.cause());
			}
	
			// Take template B offline (hence neuron B too)
			LogManager.getLogger(MessageQueueSystem_ContinuousWrite_Test.class).info("Take template B offline");
			TemplateStateManagerTestUtils.takeTemplateOffline("RWTestTemplateB").syncUninterruptibly();
			
			LogManager.getLogger(MessageQueueSystem_ContinuousWrite_Test.class).info("Done reading {} expected={}", i, m_expected);
		}
		
		// Take template A offline (hence neuron A too)
		TemplateStateManagerTestUtils.takeTemplateOffline("RWTestTemplateA").syncUninterruptibly();
	}

	public static class RWTestTemplateA extends DefaultTestNeuronTemplateBase {
		
		public RWTestTemplateA(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new NeuronA(ref);
		}
		
		private static class NeuronA extends DefaultNeuronInstanceBase {
			private static final Logger LOG = LogManager.getLogger(NeuronA.class);
			private final QueueListener m_listener = new QueueListener();
			private long m_toWrite = 0;
			private volatile boolean m_shutdown;
			private ConstantWriter m_worker;
			private String m_fqqn;
			private TestMessage m_cur;
			
			public NeuronA(NeuronRef instanceRef) {
				super(instanceRef);
			}
			
			@Override
			public void connectResources() {
				m_worker = new ConstantWriter();
				m_fqqn = MessageQueueSystem.createFQQN("NeuronB", "A->B");
			}

			@Override
			public void nowOnline() {
				m_worker.requestMoreWork();
			}
			
			@Override
			public void deinit(Promise<Void> promise) {
				m_shutdown = true;
				promise.setSuccess((Void)null);
			}

			private class ConstantWriter extends PerpetualWorkContextAware {

				@Override
				protected void _lockException(Exception ex) {
					LogManager.getLogger(NeuronA.class).fatal("Unexpected locking exception", ex);
				}
				
				@Override
				protected void _doWork(INeuronStateLock lock) {
//					LOG.info("ConstantWriter writing. Neuron state={}", lock.currentState());
					try {
						while(!m_shutdown) {
							if (m_cur == null) {
								m_cur = new TestMessage(m_toWrite);
							}
							if (MessageQueueSystem.submitToQueue(m_fqqn, m_cur, m_listener)) {
//								LOG.info("<<<<<<< Submitted {}", m_cur.m_data);
								m_toWrite++;
								m_cur = null;
							} else {
//								LOG.info("<<<<<<< PipeFull");
								break;
							}
						}
					} catch(Exception ex) {
						m_testFuture.tryFailure(ex);
					}
					requestMoreWork();
//					LOG.info("ConstantWriter stopped. Neuron state={}", lock.currentState());
				}
				
			}
			
			private static final class QueueListener implements IMessageQueueSubmissionListener {
 
				@Override
				public void onUndelivered(ReferenceCounted msg) {
					LOG.fatal("Undelivered: {}", ((TestMessage)msg).m_data);
					m_testFuture.tryFailure(new RuntimeException("Just for stack trace"));
				}

				@Override
				public void onReceived(ReferenceCounted msg) {
//					LOG.info("Received: {}", ((TestMessage)msg).m_data);
				}

				@Override
				public void onStartProcessing(ReferenceCounted msg) {
//					LOG.info("Started: {}", ((TestMessage)msg).m_data);
				}

				@Override
				public void onProcessed(ReferenceCounted msg) {
//					LOG.info("Processed: {}", ((TestMessage)msg).m_data);
				}
				
			}
		}
	}
	
	public static class RWTestTemplateB extends DefaultTestNeuronTemplateBase {
		
		public RWTestTemplateB(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new NeuronB(ref);
		}
		
		private static class NeuronB extends DefaultNeuronInstanceBase {
//			private static final Logger LOG = LogManager.getLogger(NeuronB.class);
			
			public NeuronB(NeuronRef instanceRef) {
				super(instanceRef);
			}
			
			@Override
			public void connectResources() {
				MessageQueueSystem.defineQueue("A->B", ObjectConfigBuilder.config().build(), (ReferenceCounted qMsg) -> {
					long cur = ((TestMessage)qMsg).m_data;
//					LOG.info(">>>>>>>> Read {}", cur);
					if (cur != m_expected) {
						m_testFuture.tryFailure(new RuntimeException(cur + " != " + m_expected));
					} else {
						m_expected++;
					}
				});
			}

			@Override
			public void nowOnline() {
				// This method is called after our system state becomes online, but before the caller to bringNeuronOnline
				// gets notified.
			}
		}
	}
	
	private static final class TestMessage extends AbstractReferenceCounted {
		private final long m_data;
		
		TestMessage(long data) {
			m_data = data;
		}

		@Override
		public ReferenceCounted touch(Object hint) {
			return this;
		}

		@Override
		protected void deallocate() {
		}
		
	}
	
}
