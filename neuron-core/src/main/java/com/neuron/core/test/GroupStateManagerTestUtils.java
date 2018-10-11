package com.neuron.core.test;

import com.neuron.core.GroupRef;
import com.neuron.core.GroupRef.IGroupStateLock;
import com.neuron.core.GroupStateManager;
import com.neuron.core.GroupStateManager.GroupState;
import com.neuron.core.GroupStateManager.IGroupManagement;
import com.neuron.core.NeuronApplication;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public final class GroupStateManagerTestUtils {
	
	public static Future<Void> createFutureForState(GroupRef ref, GroupState state) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		try(IGroupStateLock lock = ref.lockState()) {
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
	
	public static Future<Void> bringOnline(IGroupManagement mgt) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		if (!mgt.bringOnline()) {
			reachedState.setFailure(new RuntimeException("bringOnline() returned false"));
			return reachedState;
		}
		try(IGroupStateLock lock = mgt.currentRef().lockState()) {
			lock.addStateListener(GroupState.Online, (successful) -> {
				if (successful) {
					reachedState.setSuccess((Void)null);
				} else {
					reachedState.setFailure(new RuntimeException("State is in failure condition"));
				}
			});
		}
		return reachedState;
	}

	public static Future<Void> bringOnline(String groupName) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		final IGroupManagement mgt = GroupStateManager.manage(groupName);
		if (!mgt.bringOnline()) {
			reachedState.setFailure(new RuntimeException("bringOnline() returned false"));
			return reachedState;
		}
		try(IGroupStateLock lock = mgt.currentRef().lockState()) {
			lock.addStateListener(GroupState.Online, (successful) -> {
				if (successful) {
					reachedState.setSuccess((Void)null);
				} else {
					reachedState.setFailure(new RuntimeException("State is in failure condition"));
				}
			});
		}
		return reachedState;
	}

	public static Future<Void> takeOffline(IGroupManagement mgt) {
		return takeOffline(mgt.currentRef().name());
	}
	
	public static Future<Void> takeOffline(String groupName) {
		final Promise<Void> reachedState = NeuronApplication.getTaskPool().next().newPromise();
		try(IGroupStateLock lock = GroupStateManager.manage(groupName).currentRef().lockState()) {
			if(!lock.takeOffline()) {
				return NeuronApplication.getTaskPool().next().newFailedFuture(new RuntimeException("Call to lock.takeOffline() returned false"));
			}
			lock.addStateListener(GroupState.Offline, (successful) -> {
				if (successful) {
					reachedState.setSuccess((Void)null);
				} else {
					reachedState.setFailure(new RuntimeException("State is in failure condition"));
				}
			});
		}
		return reachedState;
	}
}
