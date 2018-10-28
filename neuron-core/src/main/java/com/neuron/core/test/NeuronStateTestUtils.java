package com.neuron.core.test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronLogEntry;
import com.neuron.core.NeuronRef;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem;
import com.neuron.core.NeuronStateSystem.INeuronManagement;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateStateSystem.ITemplateManagement;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public final class NeuronStateTestUtils {
	private static final DateFormat m_dtFormatter = SimpleDateFormat.getDateTimeInstance(SimpleDateFormat.SHORT,SimpleDateFormat.SHORT);

	public static Future<Void> bringOnline(ITemplateManagement tMgt, String neuronName, ObjectConfig config) {
		return bringOnline(tMgt.name(), neuronName, config);
	}
	
	public static Future<Void> bringOnline(String templateName, String neuronName, ObjectConfig config) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		final INeuronManagement mgt = NeuronStateSystem.registerNeuron(templateName, neuronName);
		if (!mgt.bringOnline(config)) {
			reachedState.setFailure(new RuntimeException("bringOnline() returned false"));
			return reachedState;
		}
		try(INeuronStateLock lock = mgt.currentRef().lockState()) {
			lock.addStateListener(NeuronState.Online, (successful) -> {
				if (successful) {
					reachedState.setSuccess((Void)null);
				} else {
					reachedState.setFailure(new RuntimeException("State is in failure condition"));
				}
			});
		}
		return reachedState;
	}
	
	public static Future<Void> createFutureForState(NeuronRef ref, NeuronState state) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		try(INeuronStateLock lock = ref.lockState()) {
			lock.addStateListener(state, (successful) -> {
				if (successful) {
					reachedState.setSuccess((Void)null);
				} else {
					reachedState.setFailure(new RuntimeException("State is in failure condition"));
				}
			});
		}
		return reachedState;
	}

	public static Future<Void> takeNeuronOffline(String name) {
		return takeNeuronOffline(NeuronStateSystem.manage(name).currentRef());
	}
	
	public static Future<Void> takeNeuronOffline(NeuronRef ref) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		try(INeuronStateLock lock = ref.lockState()) {
			if(!lock.takeOffline()) {
				return NeuronApplication.getTaskPool().next().newFailedFuture(new RuntimeException("Call to lock.takeOffline() returned false"));
			}
			lock.addStateListener(NeuronState.Offline, (successful) -> {
				if (successful) {
					reachedState.setSuccess((Void)null);
				} else {
					reachedState.setFailure(new RuntimeException("State is in failure condition"));
				}
			});
		}
		return reachedState;
	}

	public static boolean logContains(String neuronName, String testString) {
		return logContains(neuronName, testString, false);
	}
	public static boolean logContains(NeuronRef ref, String testString) {
		return logContains(ref, testString, false);
	}
	public static boolean logContains(String neuronName, String testString, boolean printLog) {
		return logContains(NeuronStateSystem.manage(neuronName).currentRef(), testString, printLog);
	}
	public static boolean logContains(NeuronRef ref, String testString, boolean printLog) {
		final StringBuilder sb = printLog ? new StringBuilder() : null;
		boolean foundIt = false;
		for(NeuronLogEntry entry : ref.getLog()) {
			if (entry.message.contains(testString)) {
				foundIt = true;
				if (!printLog) {
					break;
				}
			}
			if (printLog) {
				sb.setLength(0);
				sb.append(entry.timestamp)
				.append(' ')
				.append(m_dtFormatter.format(entry.timestamp))
				.append(' ')
				.append(entry.level.toString())
				.append(' ')
				.append(entry.message)
				.append('\n');
				System.out.println(sb);
			}
		}
		return foundIt;
	}
	
}
