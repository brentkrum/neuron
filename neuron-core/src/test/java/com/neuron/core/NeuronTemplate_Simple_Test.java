package com.neuron.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.TemplateStateTestUtils;
import com.neuron.core.test.TestUtils;

import io.netty.util.concurrent.Future;

public class NeuronTemplate_Simple_Test
{
	@BeforeAll
	public static void init() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
	}
	
	@AfterAll
	public static void deinit() {
		TestUtils.printSystemStatuses();
		NeuronApplication.shutdown();
	}
	
	@Test
	public void simpleSuccessfulOnlineOffline() {
		TemplateStateSystem.registerTemplate("TestTemplate", TestTemplate.class);
		Future<Void> f = TemplateStateTestUtils.bringTemplateOnline("TestTemplate");
		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for TestTemplate to go online");
		assertTrue(f.isSuccess());

		TestUtils.printSystemStatuses();

		final Future<Void> offlineF = TemplateStateTestUtils.takeTemplateOffline("TestTemplate");
		assertTrue(offlineF.awaitUninterruptibly(1000), "Timeout waiting for template to go offline");
		assertTrue(offlineF.isSuccess());
	}
	public static class TestTemplate extends DefaultTestNeuronTemplateBase {
		
		public TestTemplate(TemplateRef ref)
		{
			super(ref);
		}
		
	}
}
