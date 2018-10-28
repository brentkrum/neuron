package com.neuron.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.GroupRef.IGroupStateLock;
import com.neuron.core.GroupStateSystem.GroupState;
import com.neuron.core.TemplateStateSystem.TemplateState;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.GroupStateManagerTestUtils;
import com.neuron.core.test.TemplateStateTestUtils;
import com.neuron.core.test.TestUtils;

import io.netty.util.concurrent.Future;

public class NeuronGroupSimple_Test
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
	public void groupOffline() {
		TemplateStateSystem.registerTemplate("TestTemplate2", TestTemplate.class);
		Future<Void> f = TemplateStateTestUtils.bringTemplateOnline("TestTemplate2");
		assertTrue(f.awaitUninterruptibly(1000), "Timeout waiting for TestTemplate to go online");
		assertTrue(f.isSuccess());

		TestUtils.printSystemStatuses();
		try(IGroupStateLock lock = GroupStateSystem.defaultGroupRef().lockState()) {
			assertTrue(lock.takeOffline());
		}
		assertTrue(GroupStateManagerTestUtils.createFutureForState(GroupStateSystem.defaultGroupRef(), GroupState.Offline).awaitUninterruptibly(1000));
		assertTrue(TemplateStateTestUtils.createFutureForState("TestTemplate2", TemplateState.Offline).awaitUninterruptibly(1000));
	}

	public static class TestTemplate extends DefaultTestNeuronTemplateBase {
		
		public TestTemplate(TemplateRef ref)
		{
			super(ref);
		}
		
	}
}
