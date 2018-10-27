package com.neuron.core;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.StatusSystem.StatusType;
import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.TemplateStateManager.TemplateState;
import com.neuron.core.GroupRef.IGroupStateLock;
import com.neuron.core.netty.TSPromiseCombiner;
import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.FastLinkedList;
import com.neuron.utility.IntTrie;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Promise;

public final class GroupStateManager {
	public enum GroupState { NA, SystemOnline, Online, TakeTemplatesOffline, SystemOffline, Offline };

	private static final boolean LOCK_LEAK_DETECTION = Config.getFWBoolean("core.GroupStateManager.lockLeakDetection", false);
	
	private static final Logger LOG = LogManager.getLogger(GroupStateManager.class);

	private static final ReadWriteLock m_rwLock = new ReentrantReadWriteLock();
	private static final IntTrie<Management> m_groupsById = new IntTrie<>();
	private static final CharSequenceTrie<Management> m_groupsByName = new CharSequenceTrie<>();
	private static final AtomicInteger m_nextGroupId = new AtomicInteger(1);
	private static final AtomicInteger m_nextGroupGen = new AtomicInteger(1);
	
	public static final String DEFAULT_GROUP_NAME = "DEFAULT";

	static {
		registerGroup(DEFAULT_GROUP_NAME).bringOnline();
	}
	
	public static void enableSelfTest() {
	}

	public static GroupRef defaultGroupRef() {
		return manage(DEFAULT_GROUP_NAME).currentRef();
	}
	
	public static IGroupManagement registerGroup(String groupName) {
		m_rwLock.writeLock().lock();
		try {
			final Management mgt = new Management(groupName);
			final Management existing = m_groupsByName.addOrFetch(groupName, mgt);
			if (existing != null) {
				throw new IllegalArgumentException("The group " + groupName + " already exists");
			} else {
				m_groupsById.addOrFetch(mgt.m_id, mgt);
			}
			return mgt;
		} finally {
			m_rwLock.writeLock().unlock();
		}
	}
	
	public static IGroupManagement manage(String groupName) {
		m_rwLock.readLock().lock();
		try {
			final Management mgt = m_groupsByName.get(groupName);
			if (mgt == null) {
				throw new IllegalArgumentException("The group " + groupName + " was not registered");
			}
			return mgt;
		} finally {
			m_rwLock.readLock().unlock();
		}
	}

	public interface IGroupManagement {
		boolean bringOnline();
		GroupRef currentRef();
	}
	
	private static final class Management implements IGroupManagement {
		private final LinkedList<InstanceManagement> m_oldGen = new LinkedList<>();
		private final int m_id;
		private final String m_name;
		
		private InstanceManagement m_current;
		
		Management(String groupName) {
			m_id = m_nextGroupId.incrementAndGet();
			m_name = groupName;
			m_current = new InstanceManagement(m_nextGroupGen.incrementAndGet());
			m_current.initOffline();
		}
		
		@Override
		public boolean bringOnline() {
			// This could be called by anybody anywhere
			synchronized(this) {
				if (m_current != null) {
					try(final IGroupStateLock lock = m_current.lockState()) {
						if (lock.currentState().ordinal() <= GroupState.Online.ordinal()) {
							return true;
						}
						if (lock.currentState() != GroupState.Offline) {
							return false;
						}
					}
					m_oldGen.add(m_current);
				}
				// This is the only place that m_current is ever modified
				m_current = new InstanceManagement(m_nextGroupGen.incrementAndGet());
			}
			// At this point it is safe to use it outside the lock
			// It currently is in the state of NA and there is no way to change that except
			// by the thread there is here.
			m_current.setState(GroupState.SystemOnline);

			return true;
		}
		
		@Override
		public GroupRef currentRef() {
			return m_current;
		}
		
		private final class InstanceManagement extends GroupRef {
			private final StateManagement m_stateMgt[] = new StateManagement[GroupState.values().length];
			private final EventLoop m_myEventLoop;
			private final IntTrie<TemplateRef> m_activeTemplatesByGen = new IntTrie<>();
			private GroupState m_state = GroupState.NA;
			private int m_stateLockCount;
			
			private IntTrie<StateLock> m_lockTracking = new IntTrie<>();
			private int m_nextLockTrackingId = 1;
			private GroupState m_pendingState = null;
			
			private InstanceManagement(int gen) {
				super(m_id, m_name, gen);
				m_myEventLoop = NeuronApplication.getTaskPool().next();
				for(GroupState s : GroupState.values()) {
					m_stateMgt[s.ordinal()] = new StateManagement(s);
				}
				setSystemListeners();
			}
			
			private void setSystemListeners() {
				// NA, SystemOnline, Online
				getStateManager(GroupState.SystemOnline).setPostListener((isSuccessful) -> {
					if (isSuccessful) {
						setState(GroupState.Online);
					}
				});
				
				// TakeTemplatesOffline, SystemOffline, Offline
				getStateManager(GroupState.TakeTemplatesOffline).setPostListener((isSuccessful) -> {
					if (isSuccessful) {
						final int neuronsLeft;
						synchronized(m_activeTemplatesByGen) {
							neuronsLeft = m_activeTemplatesByGen.count();
						}
						if (neuronsLeft == 0) {
							try(IGroupStateLock groupLock = lockState()) {
								if (groupLock.currentState() == GroupState.TakeTemplatesOffline) {
									setState(GroupState.SystemOffline);
								}
							}
						}
					}
				});
				getStateManager(GroupState.SystemOffline).setPostListener((isSuccessful) -> {
					if (isSuccessful) {
						setState(GroupState.Offline);
					}
				});
			}
			
			private void initOffline() {
				final int start;
				synchronized(this) {
					start = Integer.max(1, m_state.ordinal());
					m_state = GroupState.Offline;
					StatusSystem.setStatus(this, StatusType.Offline, "registered");
				}
				Throwable dummy = new RuntimeException();
				for(int i=start; i<m_stateMgt.length-1; i++) {
					m_stateMgt[i].m_reachedStatePromise.tryFailure(dummy);
				}
				m_stateMgt[GroupState.Offline.ordinal()].m_reachedStatePromise.trySuccess(this);
			}

			private StateManagement getStateManager(GroupState state) {
				return m_stateMgt[state.ordinal()];
			}
			
			private void setState(GroupState state) {
				synchronized(this) {
					if (state.ordinal() != m_state.ordinal()+1) {
						throw new IllegalArgumentException("Current state is " + m_state + ", cannot set state to " + state);
					}
					if (m_stateLockCount == 0) {
						setState0(state);
					} else {
						m_pendingState = state;
					}
				}
			}
			
			private void setState0(GroupState state) {
				final Promise<GroupRef> promise = m_stateMgt[state.ordinal()].m_reachedStatePromise;
				m_state = state;
				String reasonText = state.toString();
				final StatusType st;
				if (m_state.ordinal() < GroupState.Online.ordinal()) {
					st = StatusType.GoingOnline;
				} else if (m_state.ordinal() == GroupState.Online.ordinal()) {
					st = StatusType.Online;
					reasonText = "";
				} else if (m_state.ordinal() < GroupState.Offline.ordinal()) {
					st = StatusType.GoingOffline;
				} else {
					st = StatusType.Offline;
					reasonText = "";
				}
				StatusSystem.setStatus(this, st, reasonText);
				if (!promise.trySuccess(InstanceManagement.this)) {
					LOG.fatal("Failed setting promise state to {}. This should never happen.", m_state);
				}
			}

			@Override
			public IGroupStateLock lockState() {
				final StateLock lock = new StateLock();
				synchronized(this) {
					m_stateLockCount++;
					if (LOCK_LEAK_DETECTION) {
						lock.m_lockTrackingId = m_nextLockTrackingId++;
						lock.m_lockStackTrace = Thread.currentThread().getStackTrace();
						if (m_lockTracking == null) {
							m_lockTracking = new IntTrie<>();
						}
						m_lockTracking.addOrFetch(lock.m_lockTrackingId, lock);
					}
				}
				NeuronSystemTLS.add(this);
				return lock;
			}
			
			private void unlockState(int lockTrackingId) {
				try {
					synchronized(InstanceManagement.this) {
						m_stateLockCount--;
						if (m_stateLockCount != 0) {
							return;
						}
						if (LOCK_LEAK_DETECTION) {
							m_lockTracking.remove(lockTrackingId);
						}
						if (m_pendingState != null) {
							setState0(m_pendingState);
							m_pendingState = null;
						}
					}
				} finally {
					NeuronSystemTLS.remove();
				}
			}


			private final class StateManagement {
				private final GroupState m_state;
				private final Promise<GroupRef> m_reachedStatePromise = m_myEventLoop.newPromise();
				private FastLinkedList<ListenerHolder> m_listeners = new FastLinkedList<>();
				private IGroupStateSyncListener m_systemPreListener;
				private IGroupStateSyncListener m_systemPostListener;
				
				StateManagement(GroupState state) {
					m_state = state;
					m_reachedStatePromise.addListener((statePromise) -> {
						final boolean successful = statePromise.isSuccess();
						if (m_systemPreListener != null) {
							m_systemPreListener.onStateReached(successful);
						}
						final FastLinkedList<ListenerHolder> listeners;
						synchronized(StateManagement.this) {
							listeners = m_listeners;
							m_listeners = null;
						}
						final TSPromiseCombiner tsp = new TSPromiseCombiner();
						listeners.forEach((holder) -> {
							final IGroupStateListener listener = holder.m_listener;
							if (listener instanceof IGroupStateSyncListener) {
								tsp.add(NeuronApplication.getTaskPool().submit(() -> {
									NeuronSystemTLS.add(InstanceManagement.this);
									try {
										((IGroupStateSyncListener)listener).onStateReached(successful);
									} catch(Exception ex) {
										LOG.error("Exception calling state listener", ex);
									} finally {
										NeuronSystemTLS.remove();
									}
								}));
							} else {
								Promise<Void> promise = m_myEventLoop.newPromise();
								tsp.add(promise);
								NeuronSystemTLS.add(InstanceManagement.this);
								try {
									((IGroupStateAsyncListener)listener).onStateReached(successful, promise);
								} catch(Exception ex) {
									LOG.error("Exception calling state listener", ex);
								} finally {
									NeuronSystemTLS.remove();
								}
							}
							return true;
						});
						if (m_systemPostListener != null) {
							final Promise<Void> aggregatePromise = m_myEventLoop.newPromise();
							aggregatePromise.addListener((f) -> {
								// Once all listener calls are done, we can let the system process
								m_systemPostListener.onStateReached(successful);
							});
							tsp.finish(aggregatePromise);
						}
					});
				}
				
				@SuppressWarnings("unused")
				void setPreListener(IGroupStateSyncListener listener) {
					m_systemPreListener = listener;
				}
				
				void setPostListener(IGroupStateSyncListener listener) {
					m_systemPostListener = listener;
				}
				
				IGroupStateListenerRemoval addListener(IGroupStateListener listener, GroupState currentState) {
					synchronized(this) {
						// m_listeners goes null once the promise has been set complete and the callback has been called
						// until that time, we can keep adding to it
						if (m_listeners != null) {
							ListenerHolder h = new ListenerHolder(listener);
							m_listeners.addFirst(h);
							return h;
						}
					}
					// Cannot add async listeners for a state already reached.  These listeners'
					// main purpose is to keep the state from switching until they trigger the
					// promise
					if (listener instanceof IGroupStateAsyncListener) {
						throw new IllegalArgumentException("Cannot add async listener to state " + m_state + ", the group is already in the state " + currentState);
					}
					// Our promise listener has fired and will not call any additional listeners, call it on this thread
					NeuronSystemTLS.add(InstanceManagement.this);
					try {
						if (listener instanceof IGroupStateSyncListener) {
							((IGroupStateSyncListener)listener).onStateReached(m_reachedStatePromise.isSuccess());
//						} else {
//							// The promise is ignored, since we already reached the state
//							((IGroupStateAsyncListener)listener).onStateReached(m_reachedStatePromise.isSuccess(), m_myEventLoop.newPromise());
						}
					} catch(Exception ex) {
						LOG.error("Exception calling state listener", ex);
					} finally {
						NeuronSystemTLS.remove();
					}
					return null;
				}
				
				private final class ListenerHolder extends FastLinkedList.LLNode<ListenerHolder> implements IGroupStateListenerRemoval {
					private final IGroupStateListener m_listener;
					
					ListenerHolder(IGroupStateListener listener) {
						m_listener = listener;
					}
					
					@Override
					protected ListenerHolder getObject() {
						return this;
					}

					@Override
					public void remove() {
						synchronized(StateManagement.this) {
							if (m_listeners != null) {
								m_listeners.remove(this);
							}
						}
					}
					
				}
				
			}

			private final class StateLock implements IGroupStateLock {
				private final Thread m_lockingThread;
				private boolean m_locked = true;
				private int m_lockTrackingId;
				@SuppressWarnings("unused")
				private StackTraceElement[] m_lockStackTrace;
				
				StateLock() {
					m_lockingThread = Thread.currentThread();
				}
				
				@Override
				public void registerTemplate(TemplateRef ref) {
					synchronized(m_activeTemplatesByGen) {
						if (m_activeTemplatesByGen.addOrFetch(ref.generation(), ref) != null) {
							LOG.fatal("Added a template reference which already existed.  This should never happen.", new RuntimeException("Just for the stack trace"));
							NeuronApplication.fatalExit();
						}
					}
					try(ITemplateStateLock templateLock = ref.lockState()) {
						templateLock.addStateListener(TemplateState.Offline, (success) -> {
							final int templatesLeft;
							synchronized(m_activeTemplatesByGen) {
								if (m_activeTemplatesByGen.remove(ref.generation()) == null) {
									LOG.fatal("A template reference did not exist.  This should never happen.", new RuntimeException("Just for the stack trace"));
									NeuronApplication.fatalExit();
								}
								templatesLeft = m_activeTemplatesByGen.count();
							}
							if (templatesLeft == 0) {
								try(IGroupStateLock groupLock = InstanceManagement.this.lockState()) {
									if (groupLock.currentState() == GroupState.TakeTemplatesOffline) {
										setState(GroupState.SystemOffline);
									}
								}
							}
						});
					}
				}

				@Override
				public GroupState currentState() {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					return m_state;
				}

				@Override
				public IGroupStateListenerRemoval addStateListener(GroupState state, IGroupStateSyncListener listener) {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					return getStateManager(state).addListener(listener, m_state);
				}
				
				@Override
				public IGroupStateListenerRemoval addStateAsyncListener(GroupState state, IGroupStateAsyncListener listener) {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					return getStateManager(state).addListener(listener, m_state);
				}
				
				@Override
				public boolean takeOffline() {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					if (m_state == GroupState.Online) {
						setState(GroupState.TakeTemplatesOffline);
						return true;
					} else {
						return false;
					}
				}
				
				@Override
				public void close() {
					unlock();
				}

				@Override
				public void unlock() {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					if (m_lockingThread != Thread.currentThread()) {
						throw new IllegalStateException("lock() was called by thread " + m_lockingThread.getName() + " but unlock() is being called from thread " + Thread.currentThread().getName() + ". State locks cannot be passed to other threads.");
					}
					m_locked = false;
					unlockState(m_lockTrackingId);
				}
				
			}
		}
	}
}
