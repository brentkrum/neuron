package com.neuron.core;

public class SingleInstanceTemplateException extends Exception {
	private static final long serialVersionUID = 1L;

	public SingleInstanceTemplateException(String message) {
		super(message);
	}

}
