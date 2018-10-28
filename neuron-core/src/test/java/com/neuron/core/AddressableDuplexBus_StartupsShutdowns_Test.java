package com.neuron.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.NeuronStateTestUtils;
import com.neuron.core.test.TemplateStateTestUtils;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public class AddressableDuplexBus_StartupsShutdowns_Test {
	@BeforeAll
	public static void init() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		System.setProperty("logger.com.neuron.core.AddressableDuplexBusSystem", "DEBUG");
//		System.setProperty("com.neuron.core.NeuronThreadContext.leakDetection", "true");
		
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
	}
	
	@AfterAll
	public static void deinit() {
		NeuronApplication.shutdown();
	}
	
// ==============================================================================================================================================================================
	@Test
	public void sameAddress() {
		// two neurons listen on same bus address
		TemplateStateTestUtils.registerAndBringOnline("SameAddressTemplate", SameAddressTemplate.class).syncUninterruptibly();
		Future<Void> f = NeuronStateTestUtils.bringOnline("SameAddressTemplate", "sameAddress", ObjectConfigBuilder.config().build());
		Assertions.assertTrue(f.awaitUninterruptibly(500));
		Assertions.assertFalse(f.isSuccess());
		assertTrue(NeuronStateTestUtils.logContains("sameAddress", "java.lang.UnsupportedOperationException: Attempted to add a second reader for bus 'sameAddress' address 'test'"));
	}
	
	public static class SameAddressTemplate extends DefaultTestNeuronTemplateBase {
		
		public SameAddressTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new MyNeuron(ref);
		}
		
		private static class MyNeuron extends DefaultNeuronInstanceBase {
			public MyNeuron(NeuronRef instanceRef) {
				super(instanceRef);
			}

			@Override
			public void connectResources() {
				AddressableDuplexBusSystem.listenOnBus("sameAddress", "test", ObjectConfigBuilder.config().build(), (ReferenceCounted request) -> {
					return NeuronApplication.<ReferenceCounted>newPromise().setSuccess(new TestMessage());
				});
				AddressableDuplexBusSystem.listenOnBus("sameAddress", "test", ObjectConfigBuilder.config().build(), (ReferenceCounted request) -> {
					return NeuronApplication.<ReferenceCounted>newPromise().setSuccess(new TestMessage());
				});
			}
			
		}
	}

// ==============================================================================================================================================================================
	
	@Test
	public void patternMatchNone() {
		// a neuron listens on a pattern which doesn't match any addresses
		TemplateStateTestUtils.registerAndBringOnline("PatternMatchNoneTemplate", PatternMatchNoneTemplate.class).syncUninterruptibly();
		Future<Void> f = NeuronStateTestUtils.bringOnline("PatternMatchNoneTemplate", "patternMatchNone", ObjectConfigBuilder.config().build());
		Assertions.assertTrue(f.awaitUninterruptibly(500));
		Assertions.assertTrue(f.isSuccess());
		
		TemplateStateTestUtils.takeTemplateOffline("PatternMatchNoneTemplate");
	}
	
	public static class PatternMatchNoneTemplate extends DefaultTestNeuronTemplateBase {
		
		public PatternMatchNoneTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new MyNeuron(ref);
		}
		
		private static class MyNeuron extends DefaultNeuronInstanceBase {
			public MyNeuron(NeuronRef instanceRef) {
				super(instanceRef);
			}

			@Override
			public void connectResources() {
				AddressableDuplexBusSystem.listenOnBus("BUSpatternMatchNone", Pattern.compile("test1"), ObjectConfigBuilder.config().build(), (ReferenceCounted request) -> {
					return NeuronApplication.<ReferenceCounted>newPromise().setSuccess(new TestMessage());
				});
				AddressableDuplexBusSystem.listenOnBus("BUSpatternMatchNone", Pattern.compile("test2"), ObjectConfigBuilder.config().build(), (ReferenceCounted request) -> {
					return NeuronApplication.<ReferenceCounted>newPromise().setSuccess(new TestMessage());
				});
			}
			
		}
	}

// ==============================================================================================================================================================================
	
	@Test
	public void patternMatchInUseAddress() {
		// one neuron listens on a bus address
		// another neuron listens on a pattern which matches this address
		TemplateStateTestUtils.registerAndBringOnline("PatternMatchInUseAddressTemplate", PatternMatchInUseAddressTemplate.class).syncUninterruptibly();
		Future<Void> f = NeuronStateTestUtils.bringOnline("PatternMatchInUseAddressTemplate", "patternMatchInUseAddress", ObjectConfigBuilder.config().build());
		Assertions.assertTrue(f.awaitUninterruptibly(500));
		Assertions.assertFalse(f.isSuccess());
		assertTrue(NeuronStateTestUtils.logContains("patternMatchInUseAddress", "java.lang.IllegalArgumentException: In bus 'BUSpatternMatchInUseAddress' the supplied address pattern matches the existing in-use address 'test'"));
		
		TemplateStateTestUtils.takeTemplateOffline("PatternMatchInUseAddressTemplate");
	}
	
	public static class PatternMatchInUseAddressTemplate extends DefaultTestNeuronTemplateBase {
		
		public PatternMatchInUseAddressTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new MyNeuron(ref);
		}
		
		private static class MyNeuron extends DefaultNeuronInstanceBase {
			public MyNeuron(NeuronRef instanceRef) {
				super(instanceRef);
			}

			@Override
			public void connectResources() {
				AddressableDuplexBusSystem.listenOnBus("BUSpatternMatchInUseAddress", "test", ObjectConfigBuilder.config().build(), (ReferenceCounted request) -> {
					return NeuronApplication.<ReferenceCounted>newPromise().setSuccess(new TestMessage());
				});
				AddressableDuplexBusSystem.listenOnBus("BUSpatternMatchInUseAddress", Pattern.compile("t.*"), ObjectConfigBuilder.config().build(), (ReferenceCounted request) -> {
					return NeuronApplication.<ReferenceCounted>newPromise().setSuccess(new TestMessage());
				});
			}
			
		}
	}
	
// ==============================================================================================================================================================================
	static Promise<Void> patternMatchAddress_MessageProcessed;
	@Test
	public void patternMatchAddress() {
		patternMatchAddress_MessageProcessed = NeuronApplication.newPromise();
		// submit message into a bus at an address 
		TemplateStateTestUtils.registerAndBringOnline("PatternMatchAddressTemplateA", PatternMatchAddressTemplateA.class).syncUninterruptibly();
		Assertions.assertTrue(NeuronStateTestUtils.bringOnline("PatternMatchAddressTemplateA", "patternMatchAddressA", ObjectConfigBuilder.emptyConfig()).syncUninterruptibly().isSuccess());
		
		// another neuron listens on a pattern which matches this address
		TemplateStateTestUtils.registerAndBringOnline("PatternMatchAddressTemplateB", PatternMatchAddressTemplateB.class).syncUninterruptibly();
		Assertions.assertTrue(NeuronStateTestUtils.bringOnline("PatternMatchAddressTemplateB", "patternMatchAddressB", ObjectConfigBuilder.emptyConfig()).syncUninterruptibly().isSuccess());
		
		// Check that the message is delivered
		Assertions.assertTrue(patternMatchAddress_MessageProcessed.awaitUninterruptibly(250));
		if (patternMatchAddress_MessageProcessed.cause() != null) {
			LogManager.getLogger(AddressableDuplexBus_StartupsShutdowns_Test.class).error("", patternMatchAddress_MessageProcessed.cause());
		}
		Assertions.assertTrue(patternMatchAddress_MessageProcessed.isSuccess());
		
		TemplateStateTestUtils.takeTemplateOffline("PatternMatchAddressTemplateA");
		TemplateStateTestUtils.takeTemplateOffline("PatternMatchAddressTemplateB");
	}
	
	public static class PatternMatchAddressTemplateA extends DefaultTestNeuronTemplateBase {
		
		public PatternMatchAddressTemplateA(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new MyNeuron(ref);
		}
		
		private static class MyNeuron extends DefaultNeuronInstanceBase {
			public MyNeuron(NeuronRef instanceRef) {
				super(instanceRef);
			}

			@Override
			public void nowOnline() {
				boolean success = AddressableDuplexBusSystem.submitToBus("BUSpatternMatchAddress", "test", new TestMessage("request"), new IDuplexBusSubmissionListener() {
					@Override
					public void onProcessed(ReferenceCounted requestMsg, ReferenceCounted responseMsg) {
						TestMessage req = (TestMessage)requestMsg;
						TestMessage res = (TestMessage)responseMsg;
						if (req.data.equals("request") && res.data.equals("request-response")) {
							LogManager.getLogger(MyNeuron.class).info("Got response!");
							patternMatchAddress_MessageProcessed.trySuccess((Void)null);
						} else {
							patternMatchAddress_MessageProcessed.tryFailure(new RuntimeException("onProcessed wrong messages"));
						}
					}

					@Override
					public void onSystemFailure(ReferenceCounted requestMsg) {
						patternMatchAddress_MessageProcessed.tryFailure(new RuntimeException("onSystemFailure"));
					}

					@Override
					public void onUndelivered(ReferenceCounted requestMsg) {
						patternMatchAddress_MessageProcessed.tryFailure(new RuntimeException("onUndelivered"));
					}
					
				}, ObjectConfigBuilder.config().option(AddressableDuplexBusSystem.submitConfig_autoCreateAddress, true).build() );
				if (!success) {
					patternMatchAddress_MessageProcessed.tryFailure(new RuntimeException("This should not happen"));
				}
			}
			
		}
	}
	
	public static class PatternMatchAddressTemplateB extends DefaultTestNeuronTemplateBase {
		
		public PatternMatchAddressTemplateB(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new MyNeuron(ref);
		}
		
		private static class MyNeuron extends DefaultNeuronInstanceBase {
			public MyNeuron(NeuronRef instanceRef) {
				super(instanceRef);
			}

			@Override
			public void connectResources() {
				AddressableDuplexBusSystem.listenOnBus("BUSpatternMatchAddress", Pattern.compile("test"), ObjectConfigBuilder.config().build(), (ReferenceCounted busRequest) -> {
					LogManager.getLogger(MyNeuron.class).info("Got request!");
					TestMessage request = (TestMessage)busRequest;
					return NeuronApplication.<ReferenceCounted>newPromise().setSuccess(new TestMessage(request.data + "-response"));
				});
			}
			
		}
	}

// ==============================================================================================================================================================================
	static Promise<Void> patternMatchMultipleAddresses_MessagesProcessed;
	@Test
	public void patternMatchMultipleAddresses() {
		patternMatchMultipleAddresses_MessagesProcessed = NeuronApplication.newPromise();
		// submit messages into a bus at several addresses 
		TemplateStateTestUtils.registerAndBringOnline("PatternMatchMultipleAddressesTemplateA", PatternMatchMultipleAddressesTemplateA.class).syncUninterruptibly();
		Assertions.assertTrue(NeuronStateTestUtils.bringOnline("PatternMatchMultipleAddressesTemplateA", "patternMatchMultipleAddressesA", ObjectConfigBuilder.emptyConfig()).syncUninterruptibly().isSuccess());
		
		// another neuron listens on a pattern which matches all addresses
		TemplateStateTestUtils.registerAndBringOnline("PatternMatchMultipleAddressesTemplateB", PatternMatchMultipleAddressesTemplateB.class).syncUninterruptibly();
		Assertions.assertTrue(NeuronStateTestUtils.bringOnline("PatternMatchMultipleAddressesTemplateB", "patternMatchMultipleAddressesB", ObjectConfigBuilder.emptyConfig()).syncUninterruptibly().isSuccess());
		
		// Check that the messages are all sent once the neuron gets online
		Assertions.assertTrue(patternMatchMultipleAddresses_MessagesProcessed.awaitUninterruptibly(250));
		if (patternMatchMultipleAddresses_MessagesProcessed.cause() != null) {
			LogManager.getLogger(AddressableDuplexBus_StartupsShutdowns_Test.class).error("", patternMatchMultipleAddresses_MessagesProcessed.cause());
		}
		Assertions.assertTrue(patternMatchMultipleAddresses_MessagesProcessed.isSuccess());
		
		TemplateStateTestUtils.takeTemplateOffline("PatternMatchMultipleAddressesTemplateA");
		TemplateStateTestUtils.takeTemplateOffline("PatternMatchMultipleAddressesTemplateB");
		
	}
	
	public static class PatternMatchMultipleAddressesTemplateA extends DefaultTestNeuronTemplateBase {
		
		public PatternMatchMultipleAddressesTemplateA(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new MyNeuron(ref);
		}
		
		private static class MyNeuron extends DefaultNeuronInstanceBase {
			private AtomicInteger m_numMsgs = new AtomicInteger();
			
			public MyNeuron(NeuronRef instanceRef) {
				super(instanceRef);
			}

			@Override
			public void nowOnline() {
				for(int i=0; i<10; i++) {
					boolean success = AddressableDuplexBusSystem.submitToBus("BUSpatternMatchMultipleAddresses", "test" + i, new TestMessage(i + "-request"), new MyListener(), ObjectConfigBuilder.config().option(AddressableDuplexBusSystem.submitConfig_autoCreateAddress, true).build() );
					if (!success) {
						patternMatchMultipleAddresses_MessagesProcessed.tryFailure(new RuntimeException("This should not happen-" + i));
					}
				}
			}
			
			private class MyListener implements IDuplexBusSubmissionListener {
				@Override
				public void onProcessed(ReferenceCounted requestMsg, ReferenceCounted responseMsg) {
					TestMessage req = (TestMessage)requestMsg;
					TestMessage res = (TestMessage)responseMsg;
					if (req.data.endsWith("request") && res.data.endsWith("request-response")) {
						LogManager.getLogger(MyNeuron.class).info("Got response {}!", req.data.charAt(0));
						if (m_numMsgs.incrementAndGet() == 10) {
							patternMatchMultipleAddresses_MessagesProcessed.trySuccess((Void)null);
						}
					} else {
						patternMatchMultipleAddresses_MessagesProcessed.tryFailure(new RuntimeException("onProcessed wrong messages"));
					}
				}
	
				@Override
				public void onSystemFailure(ReferenceCounted requestMsg) {
					patternMatchMultipleAddresses_MessagesProcessed.tryFailure(new RuntimeException("onSystemFailure"));
				}
	
				@Override
				public void onUndelivered(ReferenceCounted requestMsg) {
					patternMatchMultipleAddresses_MessagesProcessed.tryFailure(new RuntimeException("onUndelivered"));
				}
			}
		}
	}
	
	public static class PatternMatchMultipleAddressesTemplateB extends DefaultTestNeuronTemplateBase {
		
		public PatternMatchMultipleAddressesTemplateB(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new MyNeuron(ref);
		}
		
		private static class MyNeuron extends DefaultNeuronInstanceBase {
			public MyNeuron(NeuronRef instanceRef) {
				super(instanceRef);
			}

			@Override
			public void connectResources() {
				AddressableDuplexBusSystem.listenOnBus("BUSpatternMatchMultipleAddresses", Pattern.compile("test.*"), ObjectConfigBuilder.config().build(), (ReferenceCounted busRequest) -> {
					LogManager.getLogger(MyNeuron.class).info("Got request!");
					TestMessage request = (TestMessage)busRequest;
					return NeuronApplication.<ReferenceCounted>newPromise().setSuccess(new TestMessage(request.data + "-response"));
				});
			}
			
		}
	}
	
// ==============================================================================================================================================================================
	
	private static class TestMessage extends AbstractReferenceCounted {
		String data;
		
		TestMessage() {
		}
		
		TestMessage(String data) {
			this.data = data;
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
