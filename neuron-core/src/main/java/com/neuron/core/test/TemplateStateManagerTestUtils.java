package com.neuron.core.test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.neuron.core.INeuronTemplate;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronLogEntry;
import com.neuron.core.TemplateRef;
import com.neuron.core.TemplateStateManager;
import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.TemplateStateManager.ITemplateManagement;
import com.neuron.core.TemplateStateManager.TemplateState;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public final class TemplateStateManagerTestUtils {
	private static final DateFormat m_dtFormatter = SimpleDateFormat.getDateTimeInstance(SimpleDateFormat.SHORT,SimpleDateFormat.SHORT);
	
	public static Future<Void> createFutureForState(TemplateRef ref, TemplateState state) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		try(ITemplateStateLock lock = ref.lockState()) {
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

	public static Future<Void> registerAndBringOnline(String templateName, Class<? extends INeuronTemplate> templateClass) {
		final ITemplateManagement mgt = TemplateStateManager.registerTemplate(templateName, templateClass);
		return bringTemplateOnline(mgt);
	}
	
	public static Future<Void> bringTemplateOnline(ITemplateManagement mgt) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		if (!mgt.bringOnline()) {
			reachedState.setFailure(new RuntimeException("bringOnline() returned false"));
			return reachedState;
		}
		try(ITemplateStateLock lock = mgt.currentRef().lockState()) {
			lock.addStateListener(TemplateState.Online, (successful) -> {
				if (successful) {
					reachedState.setSuccess((Void)null);
				} else {
					reachedState.setFailure(new RuntimeException("State is in failure condition"));
				}
			});
		}
		return reachedState;
	}

	public static Future<Void> bringTemplateOnline(String templateName) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		final ITemplateManagement mgt = TemplateStateManager.manage(templateName);
		if (!mgt.bringOnline()) {
			reachedState.setFailure(new RuntimeException("bringOnline() returned false"));
			return reachedState;
		}
		try(ITemplateStateLock lock = mgt.currentRef().lockState()) {
			lock.addStateListener(TemplateState.Online, (successful) -> {
				if (successful) {
					reachedState.setSuccess((Void)null);
				} else {
					reachedState.setFailure(new RuntimeException("State is in failure condition"));
				}
			});
		}
		return reachedState;
	}

	public static Future<Void> takeTemplateOffline(ITemplateManagement mgt) {
		return takeTemplateOffline(mgt.currentRef().name());
	}
	
	public static Future<Void> takeTemplateOffline(String templateName) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		try(ITemplateStateLock lock = TemplateStateManager.manage(templateName).currentRef().lockState()) {
			if(!lock.takeOffline()) {
				return NeuronApplication.getTaskPool().next().newFailedFuture(new RuntimeException("Call to lock.takeOffline() returned false"));
			}
			lock.addStateListener(TemplateState.Offline, (successful) -> {
				if (successful) {
					reachedState.setSuccess((Void)null);
				} else {
					reachedState.setFailure(new RuntimeException("State is in failure condition"));
				}
			});
		}
		return reachedState;
	}

	public static boolean logContains(TemplateRef ref, String testString) {
		return logContains(ref, testString, false);
	}
	public static boolean logContains(TemplateRef ref, String testString, boolean printLog) {
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
