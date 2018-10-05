package com.neuron.core;

public class NamedValueSystem
{
	static void register() {
		NeuronApplication.register(new Registrant());
	}
	
	private static class Registrant implements INeuronApplicationSystem {

		@Override
		public String systemName()
		{
			return "NamedValueSystem";
		}
	}

}
