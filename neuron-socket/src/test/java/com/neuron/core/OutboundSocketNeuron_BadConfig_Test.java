package com.neuron.core;

import static com.neuron.core.test.NeuronStateTestUtils.createFutureForState;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.NeuronStateSystem.INeuronManagement;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.socket.OutboundSocketNeuronTemplate;
import com.neuron.core.test.NeuronStateTestUtils;
import com.neuron.core.test.TemplateStateTestUtils;
import com.neuron.core.test.TestUtils;

public class OutboundSocketNeuron_BadConfig_Test {
	
	@BeforeAll
	public static void init() {
		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
		
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
		TemplateStateTestUtils.registerAndBringOnline("OutboundSocketNeuronTemplate", OutboundSocketNeuronTemplate.class).awaitUninterruptibly();
	}
	
	@AfterAll
	public static void deinit() {
		TestUtils.printSystemStatuses();
		NeuronApplication.shutdown();
	}
	
	@Test
	void testNoConfig() {
		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("OutboundSocketNeuronTemplate", "testNoConfig");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().build()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).isSuccess());
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "Neuron config item Config_InetHost is either missing or invalid"));
	}
	
	@Test
	void testNoPortConfig() {
		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("OutboundSocketNeuronTemplate", "testNoPortConfig");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config().option(OutboundSocketNeuronTemplate.Config_InetHost, "blah").build()));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).isSuccess());
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "Neuron config item Config_Port is either missing or invalid"));
	}
	
	@Test
	void testBadPortConfig() {
		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("OutboundSocketNeuronTemplate", "testBadPortConfig");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()
			.option(OutboundSocketNeuronTemplate.Config_InetHost, "127.0.0.1")
			.option(OutboundSocketNeuronTemplate.Config_Port, 0)
			.build()
		));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).isSuccess());
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "java.net.BindException"));
	}
	
	@Test
	void testBadHostConfig() {
		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("OutboundSocketNeuronTemplate", "testBadHostConfig");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()
			.option(OutboundSocketNeuronTemplate.Config_InetHost, "blah")
			.option(OutboundSocketNeuronTemplate.Config_Port, 80)
			.build()
		));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Offline).awaitUninterruptibly(1000));
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "java.net.UnknownHostException"));
		assertTrue(NeuronStateTestUtils.takeNeuronOffline(nMgt.currentRef()).awaitUninterruptibly(1000));
	}
	
	@Test
	void testNoConnection() throws InterruptedException {
		INeuronManagement nMgt = NeuronStateSystem.registerNeuron("OutboundSocketNeuronTemplate", "testNoConnection");
		assertTrue(nMgt.bringOnline(ObjectConfigBuilder.config()
			.option(OutboundSocketNeuronTemplate.Config_InetHost, "127.0.0.1")
			.option(OutboundSocketNeuronTemplate.Config_Port, 9999) // Hopefully this port is unused
			.build()
		));
		assertTrue(createFutureForState(nMgt.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));
		Thread.sleep(500);
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "java.net.ConnectException"));
		assertTrue(NeuronStateTestUtils.logContains(nMgt.currentRef(), "Connection refused"));
		assertTrue(NeuronStateTestUtils.takeNeuronOffline(nMgt.currentRef()).awaitUninterruptibly(1000));
	}
	
}
