package com.neuron.core;

import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

interface ITemplateStateLockInternal {
	boolean canRegisterNeuron(); // This is for single-instance templates to reject more than one instance
	boolean registerNeuron(NeuronRef ref);
	INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config);
}
