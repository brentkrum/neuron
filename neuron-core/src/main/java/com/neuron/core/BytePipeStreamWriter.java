package com.neuron.core;

import java.io.DataOutput;
import java.io.OutputStream;

public abstract class BytePipeStreamWriter extends OutputStream implements DataOutput, AutoCloseable {

	public BytePipeStreamWriter() {
	}
	
	
	public abstract boolean tryClose();
	
	public abstract void close() throws PipeFullException;
}
