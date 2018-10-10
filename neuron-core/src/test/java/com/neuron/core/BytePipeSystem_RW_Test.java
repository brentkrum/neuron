package com.neuron.core;

import static com.neuron.core.test.NeuronStateManagerTestUtils.createFutureForState;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.BytePipeSystem.IPipeWriterContext;
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.TemplateStateManagerTestUtils;

import io.netty.util.concurrent.Promise;

public class BytePipeSystem_RW_Test {
	@BeforeAll
	public static void init() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		System.setProperty("logger.com.neuron.core.BytePipeSystem", "DEBUG");
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
	}
	
	@AfterAll
	public static void deinit() {
		NeuronApplication.shutdown();
	}


	private static Promise<Void> m_testFuture; 
	private static final AtomicInteger m_numCompleted = new AtomicInteger();
	
	@Test
	public void testReadWrite() {
		m_testFuture = NeuronApplication.getTaskPool().next().newPromise();
		
		TemplateStateManagerTestUtils.registerAndBringOnline("RWTestTemplateA", RWTestTemplateA.class).syncUninterruptibly();
		NeuronStateManager.registerNeuron("RWTestTemplateA", "NeuronA").bringOnline(ObjectConfigBuilder.config().build());
		createFutureForState(NeuronStateManager.manage("NeuronA").currentRef(), NeuronState.Online).syncUninterruptibly();

		
		TemplateStateManagerTestUtils.registerAndBringOnline("RWTestTemplateB", RWTestTemplateB.class).syncUninterruptibly();
		NeuronStateManager.registerNeuron("RWTestTemplateB", "NeuronB").bringOnline(ObjectConfigBuilder.config().build());
		createFutureForState(NeuronStateManager.manage("NeuronB").currentRef(), NeuronState.Online).syncUninterruptibly();
		
		Assertions.assertTrue(m_testFuture.awaitUninterruptibly(1000), "Timeout waiting for test");
		if (!m_testFuture.isSuccess()) {
			Assertions.fail(m_testFuture.cause());
		}

		TemplateStateManagerTestUtils.takeTemplateOffline("RWTestTemplateB").syncUninterruptibly();
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
			private IPipeWriterContext m_writerContextAB;
			private long m_expected = 1;
			private long m_toWrite = 1001;
			
			public NeuronA(NeuronRef instanceRef) {
				super(instanceRef);
			}
			
			@Override
			public void connectResources() {
				BytePipeSystem.configurePipeBroker("A->B", ObjectConfigBuilder.config().build());
				
				m_writerContextAB = BytePipeSystem.writeToPipe("A->B", ObjectConfigBuilder.config().build(), (event, context) -> {
					// event will either be PipeEmpty or PipeWriteable... write on either
					LogManager.getLogger(NeuronA.class).debug("Got event: {}", event.toString());
				});
				BytePipeSystem.readFromPipeAsAppendBuf("NeuronB", "B->A", ObjectConfigBuilder.config().build(), (buf) -> {
					if (buf.readableBytes() >= 8) {
						long cur = buf.readLong();
						LogManager.getLogger(NeuronA.class).debug("Read: {} 0x{}", cur, Long.toHexString(cur));
						if (cur != m_expected) {
							m_testFuture.tryFailure(new RuntimeException("NeuronA cur[" + cur + "] did not match expected[" + m_expected + "]"));
							return;
						}
						m_expected++;
						if (m_expected == 501) {
							if (m_numCompleted.incrementAndGet() == 2) {
								m_testFuture.trySuccess((Void)null);
							}
						}
						
						if (m_toWrite < 1501) {
							LogManager.getLogger(NeuronA.class).debug("Writing bytes {}", m_toWrite);
							for(int i=0; i<8; i++) {
								if (!writeByte(m_toWrite, i)) {
									return;
								}
							}
							m_toWrite++;
						}
					}
				});
			}
			
			private boolean writeByte(long num, int byteOffset) {
				try(final BytePipeStreamWriter out = m_writerContextAB.openStreamWriter()) {
					int byteToWrite = (int)((num >> 56-(byteOffset*8)) & 0xFF);
					out.write(byteToWrite);
				} catch(Exception ex) {
					m_testFuture.tryFailure(ex);
					return false;
				}
				return true;
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
			private IPipeWriterContext m_writerContextBA;
			private long m_expected = 1001;
			private long m_toWrite = 1;
			
			public NeuronB(NeuronRef instanceRef) {
				super(instanceRef);
			}


			@Override
			public void connectResources() {
				// We are still GoingOnline here, but even while we are in the middle of the follow function calls,
				// the callbacks and listeners can start firing.  Need to be prepared for that and take appropriate action
				
				BytePipeSystem.configurePipeBroker("B->A", ObjectConfigBuilder.config().build());
				
				m_writerContextBA = BytePipeSystem.writeToPipe("B->A", ObjectConfigBuilder.config().build(), (event, context) -> {
					// event will either be PipeEmpty or PipeWriteable... write on either
					LogManager.getLogger(NeuronB.class).debug("Got event: {}", event.toString());
				});
				BytePipeSystem.readFromPipeAsAppendBuf("NeuronA", "A->B", ObjectConfigBuilder.config().build(), (buf) -> {
					if (buf.readableBytes() >= 8) {
						long cur = buf.readLong();
						LogManager.getLogger(NeuronB.class).debug("Read: {} 0x{}", cur, Long.toHexString(cur));
						if (cur != m_expected) {
							m_testFuture.tryFailure(new RuntimeException("NeuronB cur[" + cur + "] did not match expected[" + m_expected + "]"));
							return;
						}
						m_expected++;
						if (m_expected == 1501) {
							if (m_numCompleted.incrementAndGet() == 2) {
								m_testFuture.trySuccess((Void)null);
							}
						}
						
						if (m_toWrite < 501) {
							LogManager.getLogger(NeuronB.class).debug("Writing: {}", m_toWrite);
							try(final BytePipeStreamWriter out = m_writerContextBA.openStreamWriter()) {
								out.writeLong(m_toWrite);
							} catch(Exception ex) {
								m_testFuture.tryFailure(ex);
								return;
							}
							m_toWrite++;
						}
					}
				});
			}

			@Override
			public void nowOnline() {
				// This method is called after our system state becomes online, but before the caller to bringNeuronOnline
				// gets notified.
				
				// Start the test
				LogManager.getLogger(NeuronB.class).debug("Writing: {}", m_toWrite);
				try(final BytePipeBufWriter out = m_writerContextBA.openBufWriter()) {
					long writeMe = m_toWrite++;
					out.buffer().writeLong(writeMe);
				} catch(Exception ex) {
					m_testFuture.tryFailure(ex);
				}
			}
		}
	}
	
}
