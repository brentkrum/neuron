package com.neuron.core;

import org.apache.logging.log4j.Level;

public final class NeuronLogEntry {
	public final long timestamp = System.currentTimeMillis();
	public final Level level;
	public final String message;
	
	NeuronLogEntry(Level level, String message) {
		this.level = level;
		this.message = message;
	}
}
