package com.neuron.core.test;

import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronLogEntry;
import com.neuron.core.TemplateRef;
import com.neuron.core.TemplateStateManager;
import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.TemplateStateManager.TemplateState;

import io.netty.util.concurrent.Future;

public final class TemplateStateManagerTestUtils {

	public static Future<TemplateRef> takeTemplateOffline(String templateName) {
		Future<TemplateRef> offlineFuture;
		try(ITemplateStateLock lock = TemplateStateManager.manage(templateName).currentRef().lockState()) {
			if(!lock.takeOffline()) {
				return NeuronApplication.getTaskPool().next().newFailedFuture(new RuntimeException("Call to lock.takeOffline() returned false"));
			}
			offlineFuture = lock.getStateFuture(TemplateState.Offline);
		}
		return offlineFuture;
	}

	public static boolean logContains(TemplateRef ref, String testString) {
		for(NeuronLogEntry entry : ref.getLog()) {
			if (entry.message.contains(testString)) {
				return true;
			}
		}
		return false;
	}
	
}
