package com.neuron.core;

public class PipeAlreadyCreatedException extends Exception {
	private static final long serialVersionUID = 1L;

	public PipeAlreadyCreatedException(String message) {
		super(message);
	}
}
