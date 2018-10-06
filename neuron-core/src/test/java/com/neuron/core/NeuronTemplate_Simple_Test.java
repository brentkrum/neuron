package com.neuron.core;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.TemplateStateManager.TemplateState;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.TestUtils;

import io.netty.util.concurrent.Future;

public class NeuronTemplate_Simple_Test
{
	@Test
	public void constructorException() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
		TemplateStateManager.enableSelfTest();
		
		TemplateStateManager.registerTemplate("TestTemplate", TestTemplate.class);
		Future<TemplateRef> f = TemplateStateManager.manage("TestTemplate").bringOnline();
		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for TestTemplate to go online");
		assertTrue(f.isSuccess());
		
//		final INeuronCallToken testTemplate1Token =  NeuronSystem.getCallTokenForTemplate("TestTemplate");
//		assertNotNull(testTemplate1Token, "TestTemplate exists");
//		assertTrue(testTemplate1Token.ref() == f.getNow());
//		assertTrue(testTemplate1Token.ref().generation() > 1, "TestTemplate has an invalid generation");
//		assertTrue(NeuronSystem.acquireCallToken(testTemplate1Token));
//		LogManager.getLogger(NeuronTemplate_Simple_Test.class).info("Test log4j message", testTemplate1Token.ref().logString());
//		NeuronSystem.releaseCallToken();
//		
//		assertNull(NeuronSystem.getCallTokenForTemplate("TestTemplate2"));

		TestUtils.printSystemStatuses();

		final Future<TemplateRef> offlineF;
		CountDownLatch l = new CountDownLatch(1);
		try(ITemplateStateLock lock = f.getNow().lockState()) {
			assertTrue(lock.takeOffline());
			offlineF = lock.getStateFuture(TemplateState.Offline);
			lock.addStateListener(TemplateState.Offline, (offlineFuture)-> {
				l.countDown();
			});
		}
		assertTrue(offlineF.awaitUninterruptibly(1000), "Timeout waiting for template to go offline");
		assertTrue(offlineF.isSuccess());

		TestUtils.printSystemStatuses();

		NeuronApplication.shutdown();
	}
	
	public static class TestTemplate extends DefaultTestNeuronTemplateBase {
		
		public TestTemplate(TemplateRef ref)
		{
			super(ref);
		}
		
	}
}
