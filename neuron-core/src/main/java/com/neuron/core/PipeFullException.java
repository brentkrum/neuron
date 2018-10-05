package com.neuron.core;

import java.io.IOException;

public final class PipeFullException extends IOException {
	private static final long serialVersionUID = 1L;

	public PipeFullException()
	{
		super();
	}

	public PipeFullException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public PipeFullException(String message)
	{
		super(message);
	}

	public PipeFullException(Throwable cause)
	{
		super(cause);
	}
	
}