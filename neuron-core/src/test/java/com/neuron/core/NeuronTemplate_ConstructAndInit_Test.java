package com.neuron.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.TemplateStateManager.ITemplateManagement;
import com.neuron.core.TemplateStateManager.TemplateState;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.TemplateStateManagerTestUtils;
import com.neuron.core.test.TestUtils;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public class NeuronTemplate_ConstructAndInit_Test
{
	@BeforeAll
	public static void init() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
//		NeuronSystem.enableSelfTest();
	}
	
	@AfterAll
	public static void deinit() {
		TestUtils.printSystemStatuses();
		NeuronApplication.shutdown();
	}
	
	@Test
	public void constructorException() {
		TemplateStateManager.registerTemplate("ConstructorExceptionTemplate", ConstructorExceptionTemplate.class);
		
		ITemplateManagement mgt = TemplateStateManager.manage("ConstructorExceptionTemplate");
		
		Future<Void> f = TemplateStateManagerTestUtils.bringTemplateOnline(mgt);
		assertTrue(f.awaitUninterruptibly(500), "Timeout waiting for template to be created");
		assertFalse(f.isSuccess(), "ConstructorExceptionTemplate was initialized successfully");
		
		assertTrue(TemplateStateManagerTestUtils.logContains(mgt.currentRef(), "InvocationTargetException"));
		assertTrue(TemplateStateManagerTestUtils.logContains(mgt.currentRef(), "Oops"));
		
		try(ITemplateStateLock lock = mgt.currentRef().lockState()) {
			assertTrue(lock.currentState() == TemplateState.Offline);
		}
	}
	
	public static class ConstructorExceptionTemplate extends DefaultTestNeuronTemplateBase {
		
		public ConstructorExceptionTemplate(TemplateRef ref)
		{
			super(ref);
			throw new RuntimeException("Oops");
		}
		
	}
	
	@Test
	public void initException() {
		TemplateStateManager.registerTemplate("InitExceptionTemplate", InitExceptionTemplate.class);
		ITemplateManagement mgt = TemplateStateManager.manage("InitExceptionTemplate");
		Future<Void> f = TemplateStateManagerTestUtils.bringTemplateOnline(mgt);
		assertTrue(f.awaitUninterruptibly(500), "Timeout waiting for template to be created");
		assertFalse(f.isSuccess(), "ConstructorExceptionTemplate was initialized successfully");
		assertFalse(f.isSuccess(), "InitExceptionTemplate was initialized successfully");
		assertTrue(TemplateStateManagerTestUtils.logContains(mgt.currentRef(), "Oops"));

		try(ITemplateStateLock lock = mgt.currentRef().lockState()) {
			assertTrue(lock.currentState() == TemplateState.Offline);
		}
	}
	
	public static class InitExceptionTemplate extends DefaultTestNeuronTemplateBase {
		
		public InitExceptionTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public void init(Promise<Void> p)
		{
			throw new RuntimeException("Oops");
		}
		
	}
	
	@Test
	public void initFail() {
		TemplateStateManager.registerTemplate("InitFailTemplate", InitFailTemplate.class);
		ITemplateManagement mgt = TemplateStateManager.manage("InitFailTemplate");
		Future<Void> f = TemplateStateManagerTestUtils.bringTemplateOnline(mgt);
		
		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for template to be created");
		assertFalse(f.isSuccess(), "InitFailTemplate was initialized successfully");
		assertTrue(TemplateStateManagerTestUtils.logContains(mgt.currentRef(), "Oops"));
		
		try(ITemplateStateLock lock = mgt.currentRef().lockState()) {
			assertTrue(lock.currentState() == TemplateState.Offline);
		}
	}
	
	public static class InitFailTemplate extends DefaultTestNeuronTemplateBase {
		
		public InitFailTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public void init(Promise<Void> p)
		{
			p.setFailure(new RuntimeException("Oops"));
		}
		
	}
	
	@Test
	public void initFailAsync() {
		TemplateStateManager.registerTemplate("InitFailAsyncTemplate", InitFailAsyncTemplate.class);
		ITemplateManagement mgt = TemplateStateManager.manage("InitFailAsyncTemplate");
		Future<Void> f = TemplateStateManagerTestUtils.bringTemplateOnline(mgt);
		long startMS = System.currentTimeMillis();
		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for template to be created");
		assertTrue(System.currentTimeMillis()-startMS > 100, "Returned too soon");
		assertFalse(f.isSuccess());
		assertTrue(TemplateStateManagerTestUtils.logContains(mgt.currentRef(), "Oops"));
		
		try(ITemplateStateLock lock = mgt.currentRef().lockState()) {
			assertTrue(lock.currentState() == TemplateState.Offline);
		}
	}
	
	public static class InitFailAsyncTemplate extends DefaultTestNeuronTemplateBase {
		
		public InitFailAsyncTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public void init(Promise<Void> p)
		{
			NeuronApplication.getTaskPool().schedule(() -> {
				p.setFailure(new RuntimeException("Oops"));
			}, 100, TimeUnit.MILLISECONDS);
		}
		
	}
	
	@Test
	public void initTimeout() {
		TemplateStateManager.registerTemplate("InitTimeoutTemplate", InitTimeoutTemplate.class);
		ITemplateManagement mgt = TemplateStateManager.manage("InitTimeoutTemplate");
		Future<Void> f = TemplateStateManagerTestUtils.bringTemplateOnline(mgt);
		
		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for template to be created");
		assertFalse(f.isSuccess());
		assertTrue(TemplateStateManagerTestUtils.logContains(mgt.currentRef(), "Timeout"));
		
		try(ITemplateStateLock lock = mgt.currentRef().lockState()) {
			assertTrue(lock.currentState() == TemplateState.Offline);
		}
	}
	
	public static class InitTimeoutTemplate extends DefaultTestNeuronTemplateBase {
		public InitTimeoutTemplate(TemplateRef ref)
		{
			super(ref);
		}
		
		@Override
		public long initTimeoutInMS()
		{
			return 1;
		}

		@Override
		public void init(Promise<Void> p)
		{
			NeuronApplication.getTaskPool().schedule(() -> {
				p.trySuccess((Void)null);
			}, 500, TimeUnit.MILLISECONDS);
		}
		
	}
	
	@Test
	public void badConstructor() {
		try {
			TemplateStateManager.registerTemplate("BadConstructorTemplate", BadConstructorTemplate.class);
			Assertions.fail("Should not get here");
		} catch(IllegalArgumentException ex) {
			assertTrue(ex.getCause() instanceof NoSuchMethodException);
		}
	}
	
	public static class BadConstructorTemplate extends DefaultTestNeuronTemplateBase {
		
		public BadConstructorTemplate(int name, int id)
		{
			super(null);
		}
		
	}
	
	@Test
	public void missingTemplate() {
		try {
			TemplateStateManager.manage("NOT-FOUND");
			Assertions.fail("Should not get here");
		} catch(IllegalArgumentException ex) {
		}
	}
	
//	@Test
//	public void selfTestException() {
//		Future<OLDTemplateRef> f = NeuronSystem.bringNeuronTemplateOnline("SelfTestExceptionTemplate", SelfTestExceptionTemplate.class);
//		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for template to be created");
//		assertFalse(f.isSuccess());
//		assertTrue(f.cause() instanceof RuntimeException && f.cause().getMessage().equals("Oops"));
//	}
//	
//	public static class SelfTestExceptionTemplate extends DefaultTestNeuronTemplateBase {
//		
//		public SelfTestExceptionTemplate(OLDTemplateRef ref)
//		{
//			super(ref);
//		}
//
//		@Override
//		public Future<Void> runSelftest()
//		{
//			throw new RuntimeException("Oops");
//		}
//		
//	}
//	
//	@Test
//	public void selfTestFail() {
//		Future<OLDTemplateRef> f = NeuronSystem.bringNeuronTemplateOnline("SelfTestFailTemplate", SelfTestFailTemplate.class);
//		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for template to be created");
//		assertFalse(f.isSuccess());
//		assertTrue(f.cause() instanceof RuntimeException && f.cause().getMessage().equals("Oops"));
//	}
//	
//	public static class SelfTestFailTemplate extends DefaultTestNeuronTemplateBase {
//		
//		public SelfTestFailTemplate(OLDTemplateRef ref)
//		{
//			super(ref);
//		}
//
//		@Override
//		public Future<Void> runSelftest()
//		{
//			return NeuronApplication.getTaskPool().next().newFailedFuture(new RuntimeException("Oops"));
//		}
//	}
//	
//	@Test
//	public void selfTestFailAsync() {
//		long startMS = System.currentTimeMillis();
//		Future<OLDTemplateRef> f = NeuronSystem.bringNeuronTemplateOnline("SelfTestFailAsyncTemplate", SelfTestFailAsyncTemplate.class);
//		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for template to be created");
//		assertTrue(System.currentTimeMillis()-startMS > 500, "Returned too soon");
//		assertFalse(f.isSuccess());
//		assertTrue(f.cause() instanceof RuntimeException && f.cause().getMessage().equals("Oops"));
//	}
//	
//	public static class SelfTestFailAsyncTemplate extends DefaultTestNeuronTemplateBase {
//		private Promise<Void> m_initPromise;
//		
//		public SelfTestFailAsyncTemplate(OLDTemplateRef ref)
//		{
//			super(ref);
//		}
//
//		@Override
//		public Future<Void> runSelftest()
//		{
//			m_initPromise = NeuronApplication.getTaskPool().next().newPromise();
//			NeuronApplication.getTaskPool().schedule(() -> {
//				m_initPromise.setFailure(new RuntimeException("Oops"));
//			}, 500, TimeUnit.MILLISECONDS);
//			return m_initPromise;
//		}
//		
//	}
//	
//	
//	@Test
//	public void selfTestTimeout() {
//		Future<OLDTemplateRef> f = NeuronSystem.bringNeuronTemplateOnline("SelfTestTimeoutTemplate", SelfTestTimeoutTemplate.class);
//		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for template to be created");
//		assertFalse(f.isSuccess());
//		assertTrue(f.cause() instanceof RuntimeException && f.cause().getMessage().contains("Timeout"));
//	}
//	
//	public static class SelfTestTimeoutTemplate extends DefaultTestNeuronTemplateBase {
//		private Promise<Void> m_initPromise;
//		
//		public SelfTestTimeoutTemplate(OLDTemplateRef ref)
//		{
//			super(ref);
//		}
//		
//		@Override
//		public long initTimeoutInMS()
//		{
//			return 1;
//		}
//
//		@Override
//		public Future<Void> runSelftest()
//		{
//			m_initPromise = NeuronApplication.getTaskPool().next().newPromise();
//			NeuronApplication.getTaskPool().schedule(() -> {
//				try {
//					m_initPromise.setSuccess((Void)null);
//				} catch(IllegalStateException ex) {
//					// This may or may not happen
//				}
//			}, 500, TimeUnit.MILLISECONDS);
//			return m_initPromise;
//		}
//		
//	}
//	
//
	private static Promise<Promise<Void>> m_multipleOnlineRequestsWait;
	@Test
	public void multipleOnlineRequests() {
		m_multipleOnlineRequestsWait = NeuronApplication.getTaskPool().next().newPromise();

		TemplateStateManager.registerTemplate("MultiOnlineTemplate", MultiOnlineTemplate.class);
		
		Future<Void> f = TemplateStateManagerTestUtils.bringTemplateOnline("MultiOnlineTemplate");
		// This is a second request for it to go Online and it is already doing so, so we just get a valid future to when it does
		Future<Void> f2 = TemplateStateManagerTestUtils.bringTemplateOnline("MultiOnlineTemplate");

		// Template is on its way Online, let's check correct error when try to take offline
		ITemplateManagement mgt = TemplateStateManager.manage("MultiOnlineTemplate");
		try(ITemplateStateLock lock = mgt.currentRef().lockState()) {
			assertFalse(lock.takeOffline());
		}
		
		assertTrue(m_multipleOnlineRequestsWait.awaitUninterruptibly(500), "Timeout waiting for template.init to be called");
		m_multipleOnlineRequestsWait.getNow().setSuccess((Void)null);
		assertTrue(f.awaitUninterruptibly(Long.MAX_VALUE), "Timeout waiting for template to be created");
		assertTrue(f.isSuccess());
		assertTrue(f2.isSuccess());
	}
	
	public static class MultiOnlineTemplate extends DefaultTestNeuronTemplateBase {
		
		public MultiOnlineTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public void init(Promise<Void> p) {
			m_multipleOnlineRequestsWait.setSuccess(p);
		}
	}
}
