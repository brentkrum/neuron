package com.neuron.core;

import com.neuron.core.MessagePipeSystem.IPipeWriterContext;

public interface IMessagePipeWriterListener {
	public enum Event { PipeEmpty, PipeWriteable, ReaderOnline };
	void onEvent(Event event, IPipeWriterContext context);
}
