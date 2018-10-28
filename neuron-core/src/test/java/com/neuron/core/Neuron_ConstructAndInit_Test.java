package com.neuron.core;

import static com.neuron.core.test.NeuronStateTestUtils.createFutureForState;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.INeuronManagement;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateStateSystem.ITemplateManagement;
import com.neuron.core.TemplateStateSystem.TemplateState;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.NeuronStateTestUtils;
import com.neuron.core.test.TemplateStateTestUtils;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

public class Neuron_ConstructAndInit_Test
{
	@BeforeAll
	public static void init() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
		TemplateStateSystem.enableSelfTest();
	}
	
	@AfterAll
	public static void deinit() {
		NeuronApplication.shutdown();
	}
	
	@Test
	public void constructorException() {
		TemplateStateSystem.registerTemplate("TestTemplate", TestTemplate.class);
		assertTrue(TemplateStateTestUtils.bringTemplateOnline("TestTemplate").awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("TestTemplate", "Test");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "java.lang.RuntimeException: Oops"));
	}
	
	public static class TestTemplate extends DefaultTestNeuronTemplateBase {
		
		public TestTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			throw new RuntimeException("Oops");
		}
	}
	
	@Test
	public void constructorReturnNull() {
		TemplateStateSystem.registerTemplate("ReturnsNullTemplate", ReturnsNullTemplate.class);
		assertTrue(TemplateStateTestUtils.bringTemplateOnline("ReturnsNullTemplate").awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("ReturnsNullTemplate", "ReturnsNullTemplateNeuron");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "template.createNeuron() returned null"));
	}
	public static class ReturnsNullTemplate extends DefaultTestNeuronTemplateBase {
		
		public ReturnsNullTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return null;
		}
	}
	
	@Test
	public void initFail() {
		TemplateStateSystem.registerTemplate("InitFailTestTemplate", InitFailTestTemplate.class);
		assertTrue(TemplateStateTestUtils.bringTemplateOnline("InitFailTestTemplate").awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("InitFailTestTemplate", "InitFailTest");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "java.lang.RuntimeException: Oops"));
	}
	public static class InitFailTestTemplate extends DefaultTestNeuronTemplateBase {
		
		public InitFailTestTemplate(TemplateRef ref)
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
			public void init(Promise<Void> promise) {
				promise.setFailure(new RuntimeException("Oops"));
			}
		}
	}
	
	
	@Test
	public void initFailAsync() {
		TemplateStateSystem.registerTemplate("InitFailAsyncTestTemplate", InitFailAsyncTestTemplate.class);
		assertTrue(TemplateStateTestUtils.bringTemplateOnline("InitFailAsyncTestTemplate").awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("InitFailAsyncTestTemplate", "initFailAsync");
		long startMS = System.currentTimeMillis();
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(System.currentTimeMillis()-startMS > 100, "Returned too soon");
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "java.lang.RuntimeException: Oops"));
	}

	public static class InitFailAsyncTestTemplate extends DefaultTestNeuronTemplateBase {
		
		public InitFailAsyncTestTemplate(TemplateRef ref)
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
			public void init(final Promise<Void> promise) {
				NeuronApplication.getTaskPool().schedule(() -> {
					promise.setFailure(new RuntimeException("Oops"));
				}, 100, TimeUnit.MILLISECONDS);
			}
		}
	}
	
	
	@Test
	public void initTimeout() {
		TemplateStateSystem.registerTemplate("InitTimeoutTestTemplate", InitTimeoutTestTemplate.class);
		assertTrue(TemplateStateTestUtils.bringTemplateOnline("InitTimeoutTestTemplate").awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("InitTimeoutTestTemplate", "InitTimeout");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "java.lang.RuntimeException: Timeout of 1ms initializing neuron"));
	}

	public static class InitTimeoutTestTemplate extends DefaultTestNeuronTemplateBase {
		
		public InitTimeoutTestTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new MyNeuron(ref);
		}
		
		private static class MyNeuron extends DefaultNeuronInstanceBase {
			private ScheduledFuture<?> m_scheduledTask;
			
			public MyNeuron(NeuronRef instanceRef) {
				super(instanceRef);
			}
			
			@Override
			public long initTimeoutInMS()
			{
				return 1;
			}

			@Override
			public void init(final Promise<Void> promise) {
				m_scheduledTask = NeuronApplication.getTaskPool().schedule(() -> {
					promise.trySuccess((Void)null);
				}, 250, TimeUnit.MILLISECONDS);
			}

			@Override
			public void onInitTimeout() {
				NeuronApplication.logInfo(LogManager.getLogger(InitTimeoutTestTemplate.class), "neuron.onInitTimeout()");
				m_scheduledTask.cancel(false);
			}
			
		}
	}
	
	
	@Test
	public void onlineOffline() {
		ITemplateManagement tMgt = TemplateStateSystem.registerTemplate("OnlineOffline", OnlineOffline.class);
		assertTrue(TemplateStateTestUtils.bringTemplateOnline(tMgt).awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("OnlineOffline", "OnlineOfflineTest");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		
		try(INeuronStateLock lock = nMgt.currentRef().lockState()) {
			assertTrue(lock.takeOffline());
		}
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		
		final Future<Void> offlineFuture = TemplateStateTestUtils.createFutureForState(tMgt.currentRef(), TemplateState.Offline);
		TemplateStateTestUtils.takeTemplateOffline(tMgt);
		assertTrue(offlineFuture.awaitUninterruptibly(1000));
	}
	
	public static class OnlineOffline extends DefaultTestNeuronTemplateBase {
		
		public OnlineOffline(TemplateRef ref)
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
		}
	}
	
	@Test
	public void onlineTemplateOffline() {
		ITemplateManagement tMgt = TemplateStateSystem.registerTemplate("OnlineTemplateOffline", OnlineTemplateOffline.class);
		assertTrue(TemplateStateTestUtils.bringTemplateOnline(tMgt).awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("OnlineTemplateOffline", "OnlineTemplateOfflineTest");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		
		final Future<Void> offlineFuture = TemplateStateTestUtils.createFutureForState(tMgt.currentRef(), TemplateState.Offline);
		TemplateStateTestUtils.takeTemplateOffline(tMgt);
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(offlineFuture.awaitUninterruptibly(1000));
	}
	
	public static class OnlineTemplateOffline extends DefaultTestNeuronTemplateBase {
		
		public OnlineTemplateOffline(TemplateRef ref)
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
		}
	}
}
