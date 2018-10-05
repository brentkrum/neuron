package com.neuron.core;

import com.neuron.core.BytePipeSystem.IPipeWriterContext;

public interface IBytePipeWriterListener {
	public enum Event { PipeEmpty, PipeWriteable, ReaderOnline };
	void onEvent(Event event, IPipeWriterContext context);
}
