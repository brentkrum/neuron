package com.neuron.core;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BytePipeSystem_BadThread_Test {
	@BeforeAll
	public static void init() {
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
	}
	
	@AfterAll
	public static void deinit() {
		NeuronApplication.shutdown();
	}
	
	@Test
	public void badThread_createPipeBroker() {
		try {
			BytePipeSystem.configurePipeBroker("asdf", ObjectConfigBuilder.config().build());
			Assertions.fail("Should not get here");
		} catch(Throwable t) {
			Assertions.assertTrue(t instanceof IllegalStateException);
		}
	}
	
	@Test
	public void readFromPipeAsAppendBuf_A() {
		try {
			BytePipeSystem.readFromPipeAsAppendBuf("asdf", ObjectConfigBuilder.config().build(), null);
			Assertions.fail("Should not get here");
		} catch(Throwable t) {
			Assertions.assertTrue(t instanceof IllegalStateException);
		}
	}
	
	@Test
	public void readFromPipeAsAppendBuf_B() {
		try {
			BytePipeSystem.readFromPipeAsAppendBuf("asdf", "asdf", ObjectConfigBuilder.config().build(), null);
			Assertions.fail("Should not get here");
		} catch(Throwable t) {
			Assertions.assertTrue(t instanceof IllegalStateException);
		}
	}
	
	@Test
	public void writeToPipe_A() {
		try {
			BytePipeSystem.writeToPipe("asdf", ObjectConfigBuilder.config().build(), null);
			Assertions.fail("Should not get here");
		} catch(Throwable t) {
			Assertions.assertTrue(t instanceof IllegalStateException);
		}
	}
	
	@Test
	public void writeToPipe_B() {
		try {
			BytePipeSystem.writeToPipe("asdf", "asdf", ObjectConfigBuilder.config().build(), null);
			Assertions.fail("Should not get here");
		} catch(Throwable t) {
			Assertions.assertTrue(t instanceof IllegalStateException);
		}
	}
}
