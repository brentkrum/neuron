package com.neuron.core;

import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

interface ITemplateStateLockInternal {
	INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config);
}
