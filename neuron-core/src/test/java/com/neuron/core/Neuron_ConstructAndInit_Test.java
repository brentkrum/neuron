package com.neuron.core;

import static com.neuron.core.test.NeuronStateManagerTestUtils.createFutureForState;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateManager.INeuronManagement;
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.TemplateStateManager.ITemplateManagement;
import com.neuron.core.TemplateStateManager.TemplateState;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

public class Neuron_ConstructAndInit_Test
{
	@BeforeAll
	public static void init() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
		TemplateStateManager.enableSelfTest();
	}
	
	@AfterAll
	public static void deinit() {
		NeuronApplication.shutdown();
	}
	
	@Test
	public void constructorException() {
		TemplateStateManager.registerTemplate("TestTemplate", TestTemplate.class);
		assertTrue(TemplateStateManager.manage("TestTemplate").bringOnline().awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateManager.registerNeuron("TestTemplate", "Test");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(nMgt.currentRef().logContains("java.lang.RuntimeException: Oops"));
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
		TemplateStateManager.registerTemplate("ReturnsNullTemplate", ReturnsNullTemplate.class);
		assertTrue(TemplateStateManager.manage("ReturnsNullTemplate").bringOnline().awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateManager.registerNeuron("ReturnsNullTemplate", "ReturnsNullTemplateNeuron");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(nMgt.currentRef().logContains("template.createNeuron() returned null"));
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
		TemplateStateManager.registerTemplate("InitFailTestTemplate", InitFailTestTemplate.class);
		assertTrue(TemplateStateManager.manage("InitFailTestTemplate").bringOnline().awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateManager.registerNeuron("InitFailTestTemplate", "InitFailTest");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(nMgt.currentRef().logContains("java.lang.RuntimeException: Oops"));
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
		TemplateStateManager.registerTemplate("InitFailAsyncTestTemplate", InitFailAsyncTestTemplate.class);
		assertTrue(TemplateStateManager.manage("InitFailAsyncTestTemplate").bringOnline().awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateManager.registerNeuron("InitFailAsyncTestTemplate", "initFailAsync");
		long startMS = System.currentTimeMillis();
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(System.currentTimeMillis()-startMS > 100, "Returned too soon");
		assertTrue(nMgt.currentRef().logContains("java.lang.RuntimeException: Oops"));
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
		TemplateStateManager.registerTemplate("InitTimeoutTestTemplate", InitTimeoutTestTemplate.class);
		assertTrue(TemplateStateManager.manage("InitTimeoutTestTemplate").bringOnline().awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateManager.registerNeuron("InitTimeoutTestTemplate", "InitTimeout");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()));
		
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		assertTrue(nMgt.currentRef().logContains("java.lang.RuntimeException: Timeout of 1ms initializing neuron"));
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
		ITemplateManagement tMgt = TemplateStateManager.registerTemplate("OnlineOffline", OnlineOffline.class);
		assertTrue(tMgt.bringOnline().awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateManager.registerNeuron("OnlineOffline", "OnlineOfflineTest");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		
		try(INeuronStateLock lock = nMgt.currentRef().lockState()) {
			assertTrue(lock.takeOffline());
		}
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Offline state");
		
		Future<TemplateRef> offlineFuture;
		try(ITemplateStateLock lock = tMgt.currentRef().lockState()) {
			assertTrue(lock.takeOffline());
			offlineFuture = lock.getStateFuture(TemplateState.Offline);
		}
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
		ITemplateManagement tMgt = TemplateStateManager.registerTemplate("OnlineTemplateOffline", OnlineTemplateOffline.class);
		assertTrue(tMgt.bringOnline().awaitUninterruptibly(1000));

		INeuronManagement nMgt = NeuronStateManager.registerNeuron("OnlineTemplateOffline", "OnlineTemplateOfflineTest");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000), "Timeout waiting for neuron to enter Online state");
		
		Future<TemplateRef> offlineFuture;
		try(ITemplateStateLock lock = tMgt.currentRef().lockState()) {
			assertTrue(lock.takeOffline());
			offlineFuture = lock.getStateFuture(TemplateState.Offline);
		}
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
