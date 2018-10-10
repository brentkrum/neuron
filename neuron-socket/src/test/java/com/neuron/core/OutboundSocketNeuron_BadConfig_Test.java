package com.neuron.core;

import static com.neuron.core.test.NeuronStateManagerTestUtils.createFutureForState;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.NeuronStateManager.INeuronManagement;
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.socket.OutboundSocketNeuronTemplate;
import com.neuron.core.test.NeuronStateManagerTestUtils;
import com.neuron.core.test.TemplateStateManagerTestUtils;
import com.neuron.core.test.TestUtils;

public class OutboundSocketNeuron_BadConfig_Test {
	
	@BeforeAll
	public static void init() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
		TemplateStateManagerTestUtils.registerAndBringOnline("OutboundSocketNeuronTemplate", OutboundSocketNeuronTemplate.class).awaitUninterruptibly();
	}
	
	@AfterAll
	public static void deinit() {
		TestUtils.printSystemStatuses();
		NeuronApplication.shutdown();
	}
	
	@Test
	void testNoConfig() {
		INeuronManagement nMgt = NeuronStateManager.registerNeuron("OutboundSocketNeuronTemplate", "testNoConfig");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).isSuccess());
		assertTrue(NeuronStateManagerTestUtils.logContains(nMgt.currentRef(), "Neuron config item Config_InetHost is either missing or invalid"));
	}
	
	@Test
	void testNoPortConfig() {
		INeuronManagement nMgt = NeuronStateManager.registerNeuron("OutboundSocketNeuronTemplate", "testNoPortConfig");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().option(OutboundSocketNeuronTemplate.Config_InetHost, "blah").build()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).isSuccess());
		assertTrue(NeuronStateManagerTestUtils.logContains(nMgt.currentRef(), "Neuron config item Config_Port is either missing or invalid"));
	}
	
	@Test
	void testBadPortConfig() {
		INeuronManagement nMgt = NeuronStateManager.registerNeuron("OutboundSocketNeuronTemplate", "testBadPortConfig");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()
			.option(OutboundSocketNeuronTemplate.Config_InetHost, "127.0.0.1")
			.option(OutboundSocketNeuronTemplate.Config_Port, 0)
			.build()
		));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).isSuccess());
		assertTrue(NeuronStateManagerTestUtils.logContains(nMgt.currentRef(), "java.net.BindException"));
	}
	
	@Test
	void testBadHostConfig() {
		INeuronManagement nMgt = NeuronStateManager.registerNeuron("OutboundSocketNeuronTemplate", "testBadHostConfig");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()
			.option(OutboundSocketNeuronTemplate.Config_InetHost, "blah")
			.option(OutboundSocketNeuronTemplate.Config_Port, 80)
			.build()
		));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
		assertTrue(NeuronStateManagerTestUtils.logContains(nMgt.currentRef(), "java.net.UnknownHostException"));
		assertTrue(NeuronStateManagerTestUtils.takeNeuronOffline(nMgt.currentRef()).awaitUninterruptibly(1000));
	}
	
	@Test
	void testNoConnection() throws InterruptedException {
		INeuronManagement nMgt = NeuronStateManager.registerNeuron("OutboundSocketNeuronTemplate", "testNoConnection");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()
			.option(OutboundSocketNeuronTemplate.Config_InetHost, "127.0.0.1")
			.option(OutboundSocketNeuronTemplate.Config_Port, 9999) // Hopefully this port is unused
			.build()
		));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));
		Thread.sleep(500);
		assertTrue(NeuronStateManagerTestUtils.logContains(nMgt.currentRef(), "java.net.ConnectException"));
		assertTrue(NeuronStateManagerTestUtils.logContains(nMgt.currentRef(), "Connection refused"));
		assertTrue(NeuronStateManagerTestUtils.takeNeuronOffline(nMgt.currentRef()).awaitUninterruptibly(1000));
	}
	
}
