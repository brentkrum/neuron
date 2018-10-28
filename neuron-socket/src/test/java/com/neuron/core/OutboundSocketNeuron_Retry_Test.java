package com.neuron.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.NeuronStateSystem.INeuronManagement;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.StatusSystem.CurrentHostStatus;
import com.neuron.core.socket.InboundSocketNeuronTemplate;
import com.neuron.core.socket.OutboundSocketNeuronTemplate;
import com.neuron.core.test.NeuronStateTestUtils;
import com.neuron.core.test.TemplateStateTestUtils;
import com.neuron.core.test.TestUtils;

public class OutboundSocketNeuron_Retry_Test {

	@BeforeAll
	public static void init() {
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();

		TemplateStateTestUtils.registerAndBringOnline("OutboundSocketNeuronTemplate", OutboundSocketNeuronTemplate.class).awaitUninterruptibly();
		TemplateStateTestUtils.registerAndBringOnline("InboundSocketNeuronTemplate", InboundSocketNeuronTemplate.class).awaitUninterruptibly();
	}

	@AfterAll
	public static void deinit() {
		TemplateStateTestUtils.takeTemplateOffline("OutboundSocketNeuronTemplate").syncUninterruptibly();
		TemplateStateTestUtils.takeTemplateOffline("InboundSocketNeuronTemplate").syncUninterruptibly();
		TestUtils.printSystemStatuses(false);

		NeuronApplication.shutdown();
	}

	@Test
	void simpleTest() throws InterruptedException {
		// Start outbound socket, so it fails connection
		LogManager.getLogger(OutboundSocketNeuron_Retry_Test.class).info(">>>>>>>> Create and bring OutboundSocket online");
		INeuronManagement nOutboundSocket = NeuronStateSystem.registerNeuron("OutboundSocketNeuronTemplate", "OutboundSocket");
		assertTrue(nOutboundSocket.bringOnline(ObjectConfigBuilder.config()
				.option(OutboundSocketNeuronTemplate.Config_InetHost, "127.0.0.1")
				.option(OutboundSocketNeuronTemplate.Config_Port, 9999)
				.option(OutboundSocketNeuronTemplate.Config_RetryDelayMS, 100) // Retry every 100ms
				.build()
			));
		assertTrue(NeuronStateTestUtils.createFutureForState(nOutboundSocket.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));

		Thread.sleep(500); // Ensure we retry a few times
		
		boolean foundStatus = false;
		for(StatusSystem.CurrentStatus status : StatusSystem.getCurrentStatus()) {
			if (status instanceof CurrentHostStatus) {
				final CurrentHostStatus hostStatus = (CurrentHostStatus)status;
				if (!hostStatus.isInbound && hostStatus.hostAndPort.equals("127.0.0.1:9999")) {
					Assertions.assertEquals(StatusSystem.StatusType.Down, hostStatus.status);
					foundStatus = true;
					break;
				}
			}
		}
		Assertions.assertTrue(foundStatus);
		TestUtils.printSystemStatuses(false);
		
		// Create server
		LogManager.getLogger(OutboundSocketNeuron_Retry_Test.class).info(">>>>>>>> Create and bring InboundSocket online");
		INeuronManagement nInboundSocket = NeuronStateSystem.registerNeuron("InboundSocketNeuronTemplate", "InboundSocket");
		assertTrue(nInboundSocket.bringOnline(ObjectConfigBuilder.config()
				.option(InboundSocketNeuronTemplate.Config_Port, 9999)
				.option(InboundSocketNeuronTemplate.Config_InPipeMaxPipeMsgCount, 1024)
				.build()
			));
		assertTrue(NeuronStateTestUtils.createFutureForState(nInboundSocket.currentRef(), NeuronState.Online).awaitUninterruptibly(1000));

		Thread.sleep(125); // Ensure we get connected

		// Verify that we are connected
		foundStatus = false;
		for(StatusSystem.CurrentStatus status : StatusSystem.getCurrentStatus()) {
			if (status instanceof CurrentHostStatus) {
				final CurrentHostStatus hostStatus = (CurrentHostStatus)status;
				if (!hostStatus.isInbound && hostStatus.hostAndPort.equals("127.0.0.1:9999")) {
					Assertions.assertEquals(StatusSystem.StatusType.Up, hostStatus.status);
					foundStatus = true;
					break;
				}
			}
		}
		Assertions.assertTrue(foundStatus);
		TestUtils.printSystemStatuses(false);
	}
}
