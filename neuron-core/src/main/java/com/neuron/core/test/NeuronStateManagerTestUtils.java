package com.neuron.core.test;

import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateManager.NeuronState;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public final class NeuronStateManagerTestUtils {

	public static Future<Void> createFutureForState(NeuronRef ref, NeuronState state) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		try(INeuronStateLock lock = ref.lockState()) {
			lock.addStateListener(state, (successful) -> {
				reachedState.setSuccess((Void)null);
			});
		}
		return reachedState;
	}
}
