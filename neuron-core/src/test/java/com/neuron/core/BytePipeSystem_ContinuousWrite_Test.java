package com.neuron.core;

import static com.neuron.core.test.NeuronStateTestUtils.createFutureForState;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.BytePipeSystem.IPipeWriterContext;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.INeuronManagement;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateStateSystem.ITemplateManagement;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.TemplateStateTestUtils;

import io.netty.util.concurrent.Promise;

public class BytePipeSystem_ContinuousWrite_Test {
	@BeforeAll
	public static void init() {
//		System.setProperty("io.netty.leakDetection.level", "PARANOID");
//		System.setProperty("io.netty.leakDetection.targetRecords", "1");
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		System.setProperty("logger.com.neuron.core.BytePipeSystem", "DEBUG");
//		System.setProperty("com.neuron.core.NeuronThreadContext.leakDetection", "true");
		
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
	}
	
	@AfterAll
	public static void deinit() {
		NeuronApplication.shutdown();
	}


	private static Promise<Void> m_testFuture; 
	private static long m_firstLong = -1;
	private static long m_lastLong = -1;
	
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
			LogManager.getLogger(BytePipeSystem_ContinuousWrite_Test.class).info("Bring Template B online");
			assertTrue(TemplateStateTestUtils.bringTemplateOnline(tMgtB).syncUninterruptibly().isSuccess());
			
			LogManager.getLogger(BytePipeSystem_ContinuousWrite_Test.class).info("Bring Neuron B online");
			assertTrue(nMgtB.bringOnline(ObjectConfigBuilder.config().build()));
			assertTrue(createFutureForState(nMgtB.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron B to enter Online state");
			
			LogManager.getLogger(BytePipeSystem_ContinuousWrite_Test.class).info("Running test - {}", i);
			// Let the test run for 25ms
			m_testFuture.awaitUninterruptibly(25);
			if (m_testFuture.isDone() && !m_testFuture.isSuccess()) {
				Assertions.fail(m_testFuture.cause());
			}
	
			// Take template B offline (hence neuron B too)
			LogManager.getLogger(BytePipeSystem_ContinuousWrite_Test.class).info("Take template B offline");
			TemplateStateTestUtils.takeTemplateOffline("RWTestTemplateB").syncUninterruptibly();
			
			LogManager.getLogger(BytePipeSystem_ContinuousWrite_Test.class).info("Done reading {} firstLong={} lastLong={}", i, m_firstLong, m_lastLong);
			
			m_firstLong = -1;
			m_lastLong = -1;
		}
		
		// Take template A offline (hence neuron A too)
		TemplateStateTestUtils.takeTemplateOffline("RWTestTemplateA").syncUninterruptibly();
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
			private IPipeWriterContext m_writerContextAB;
			private long m_toWrite = 0;
			private volatile boolean m_shutdown;
			private ConstantWriter m_worker;
			
			public NeuronA(NeuronRef instanceRef) {
				super(instanceRef);
			}
			
			@Override
			public void connectResources() {
				BytePipeSystem.configurePipeBroker("A->B", ObjectConfigBuilder.config().build());
				
				m_worker = new ConstantWriter();
				m_writerContextAB = BytePipeSystem.writeToPipe("A->B", ObjectConfigBuilder.config().build(), (event, context) -> {
					// event will either be PipeEmpty or PipeWriteable... write on either
					m_worker.requestMoreWork();
				});
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
				private BytePipeStreamWriter m_current = null;

				@Override
				protected void _lockException(Exception ex) {
					LogManager.getLogger(NeuronA.class).fatal("Unexpected locking exception", ex);
				}
				
				@Override
				protected void _doWork(INeuronStateLock lock) {
//					LogManager.getLogger(NeuronA.class).info("ConstantWriter writing. Neuron state={}", lock.currentState());
					try {
						while(!m_shutdown) {
							if (m_current == null) {
								m_current = m_writerContextAB.openStreamWriter();
								m_current.writeLong(m_toWrite);
								m_toWrite++;
							}
							if (!m_current.tryClose()) {
//								LogManager.getLogger(NeuronA.class).info("<<<<<<< PipeFull");
								break;
							}
							m_current = null;
						}
					} catch(Exception ex) {
						m_testFuture.tryFailure(ex);
					}
//					LogManager.getLogger(NeuronA.class).info("ConstantWriter stopped. Neuron state={}", lock.currentState());
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
			
			public NeuronB(NeuronRef instanceRef) {
				super(instanceRef);
			}
			
			@Override
			public void connectResources() {
				// We are still GoingOnline here, but even while we are in the middle of the follow function calls,
				// the callbacks and listeners can start firing.  Need to be prepared for that and take appropriate action
				
				BytePipeSystem.readFromPipeAsAppendBuf("NeuronA", "A->B", ObjectConfigBuilder.config().build(), (buf) -> {
//					LogManager.getLogger(NeuronB.class).info(">>>>>>>> Reading");
					while(true) {
						if (buf.readableBytes() < 8) {
//							LogManager.getLogger(NeuronB.class).info("Done reading firstLong={} lastLong={}", m_firstLong, m_lastLong);
							return;
						}
						long cur = buf.readLong();
						if (m_firstLong == -1) {
							m_firstLong = cur;
						}
						m_lastLong = cur;
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
	
}
