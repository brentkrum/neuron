package com.neuron.core;

import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

interface ITemplateStateLockInternal {
	void registerNeuron(NeuronRef ref);
	INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config);
}
