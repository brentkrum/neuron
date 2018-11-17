package com.neuron.core;

import static com.neuron.core.test.NeuronStateTestUtils.createFutureForState;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.INeuronManagement;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateStateSystem.ITemplateManagement;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.TemplateStateTestUtils;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.ResourceLeakTracker;
import io.netty.util.concurrent.Promise;

public class MessagePipeSystem_ContinuousWrite_Test {
	@BeforeAll
	public static void init() {
//		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
//		System.setProperty("logger.com.neuron.core.MessagePipeSystem", "DEBUG");
		System.setProperty("com.neuron.core.NeuronThreadContext.leakDetection", "true");
		System.setProperty("io.netty.leakDetection.level", "PARANOID");
		System.setProperty("io.netty.leakDetection.targetRecords", "1");
		
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
	}
	
	@AfterAll
	public static void deinit() {
		NeuronApplication.shutdown();
	}


	private static Promise<Void> m_testFuture; 
	private static long m_firstData = 0;
	private static long m_lastData = 0;
	
	@Test
	public void testReadWrite() {
		m_testFuture = NeuronApplication.getTaskPool().next().newPromise();
		
		ITemplateManagement tMgt = TemplateStateSystem.registerTemplate("RWTestTemplateA", RWTestTemplateA.class);
		assertTrue(TemplateStateTestUtils.bringTemplateOnline(tMgt).syncUninterruptibly().isSuccess());
		
		INeuronManagement nMgt = NeuronStateSystem.registerNeuron(tMgt.currentRef(), "NeuronA");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");

				
		ITemplateManagement tMgtB = TemplateStateSystem.registerTemplate("RWTestTemplateB", RWTestTemplateB.class);
		INeuronManagement nMgtB = NeuronStateSystem.registerNeuron("RWTestTemplateB", "NeuronB");

		for(int i=0; i<100; i++) {
			m_firstData = -1;
			m_lastData = -1;
			LogManager.getLogger(MessagePipeSystem_ContinuousWrite_Test.class).info("Bring Template B online");
			assertTrue(TemplateStateTestUtils.bringTemplateOnline(tMgtB).syncUninterruptibly().isSuccess());
			
			LogManager.getLogger(MessagePipeSystem_ContinuousWrite_Test.class).info("Bring Neuron B online");
			assertTrue(nMgtB.bringOnline(ObjectConfigBuilder.config().build()));
			assertTrue(createFutureForState(nMgtB.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron B to enter Online state");
			
			LogManager.getLogger(MessagePipeSystem_ContinuousWrite_Test.class).info("Running test - {}", i);
			// Let the test run for 25ms
			m_testFuture.awaitUninterruptibly(25);
			if (m_testFuture.isDone() && !m_testFuture.isSuccess()) {
				Assertions.fail(m_testFuture.cause());
			}
	
			// Take template B offline (hence neuron B too)
			LogManager.getLogger(MessagePipeSystem_ContinuousWrite_Test.class).info("Take template B offline");
			TemplateStateTestUtils.takeTemplateOffline("RWTestTemplateB").syncUninterruptibly();
			
			LogManager.getLogger(MessagePipeSystem_ContinuousWrite_Test.class).info("Done reading {} first={} last={}", i, m_firstData, m_lastData);
			Assertions.assertNotEquals(-1, m_firstData);
			Assertions.assertNotEquals(-1, m_lastData);
		}
		if (m_testFuture.cause() != null) {
			Assertions.fail(m_testFuture.cause());
		}
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
//			private static final Logger LOG = LogManager.getLogger(NeuronA.class);
			private long m_toWrite = 0;
			private volatile boolean m_shutdown;
			private ConstantWriter m_worker;
			private TestMessage m_cur;
			private Promise<Void> m_goingOfflinePromise;
			private MessagePipeSystem.IPipeWriterContext m_writerContext;
			public NeuronA(NeuronRef instanceRef) {
				super(instanceRef);
			}
			
			@Override
			public void connectResources() {
				m_worker = new ConstantWriter();
				MessagePipeSystem.configurePipeBroker("A->B", ObjectConfigBuilder.emptyConfig());
				m_writerContext = MessagePipeSystem.writeToPipe("A->B", ObjectConfigBuilder.emptyConfig(), (event, context) -> {
					m_worker.requestMoreWork();
				});
			}

			@Override
			public void nowOnline() {
				m_worker.requestMoreWork();
			}

			@Override
			public void goingOffline(Promise<Void> promise) {
				m_goingOfflinePromise = promise;
				m_shutdown = true;
				m_worker.requestMoreWork();
			}

			private class ConstantWriter extends PerpetualWorkContextAware {

				@Override
				protected void _lockException(Exception ex) {
					LogManager.getLogger(NeuronA.class).fatal("Unexpected locking exception", ex);
				}
				
				@Override
				protected void _doWork(INeuronStateLock lock) {
//					LOG.info("ConstantWriter writing. Neuron state={}", lock.currentState());
					if (m_shutdown && m_goingOfflinePromise.isDone()) {
						return;
					}
					try {
						while(!m_shutdown) {
							if (m_cur == null) {
								m_cur = new TestMessage(m_toWrite);
							}
							if (m_writerContext.offer(m_cur)) {
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
					if (m_shutdown) {
						m_goingOfflinePromise.trySuccess((Void)null);
						m_cur = null;
					} else {
						requestMoreWork();
					}
//					LOG.info("ConstantWriter stopped. Neuron state={}", lock.currentState());
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
				MessagePipeSystem.readFromPipe("NeuronA", "A->B", ObjectConfigBuilder.emptyConfig(), (ReferenceCounted qMsg) -> {
					m_lastData = ((TestMessage)qMsg).m_data;
					if (m_firstData == -1) {
						m_firstData = m_lastData;
					}
//					LOG.info(">>>>>>>> Read {}", cur);
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
		private static final ResourceLeakDetector<TestMessage> LEAK_DETECT = ResourceLeakDetectorFactory.instance().newResourceLeakDetector(TestMessage.class, 1);
		private final long m_data;
		private final ResourceLeakTracker<TestMessage> m_tracker;
		
		TestMessage(long data) {
			m_data = data;
			m_tracker = LEAK_DETECT.track(this);
		}

		@Override
		public ReferenceCounted touch(Object hint) {
			if (m_tracker != null) {
				m_tracker.record(hint);
			}
			return this;
		}

		@Override
		protected void deallocate() {
			if (m_tracker != null) {
				m_tracker.close(this);
			}
		}
		
	}
	
}
