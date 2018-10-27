package com.neuron.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.StatusSystem.StatusType;
import com.neuron.core.TemplateRef.ITemplateStateListenerRemoval;
import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.TemplateStateManager.TemplateState;
import com.neuron.core.netty.TSPromiseCombiner;
import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.FastLinkedList;
import com.neuron.utility.IntTrie;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

public final class NeuronStateManager {
	public enum NeuronState { NA, BeingCreated, Initializing, Connect, SystemOnline, Online, GoingOffline, Disconnecting, Deinitializing, SystemOffline, Offline};

	private static final boolean LOCK_LEAK_DETECTION = Config.getFWBoolean("core.NeuronStateManager.lockLeakDetection", false);
	private static final long DEFAULT_INIT_TIMEOUT_IN_MS = Config.getFWInt("core.NeuronStateManager.defaultInitTimeout", 5000);
	private static final int MAX_LOG_SIZE = Config.getFWInt("core.NeuronStateManager.logSize", 64);
	private static final int MAX_OLD_GEN = Config.getFWInt("core.NeuronStateManager.maxNumOldGen", 4);
	
	private static final Logger LOG = LogManager.getLogger(NeuronStateManager.class);

	private static final ReadWriteLock m_rwLock = new ReentrantReadWriteLock();
	private static final IntTrie<Management> m_neuronsById = new IntTrie<>();
	private static final CharSequenceTrie<Management> m_neuronsByName = new CharSequenceTrie<>();
	private static final AtomicInteger m_nextNeuronId = new AtomicInteger(1);
	private static final AtomicInteger m_nextNeuronGen = new AtomicInteger(1);
	
	
	public static INeuronManagement registerNeuron(TemplateRef templateRef, String neuronName) {
		return registerNeuron(templateRef.name(), neuronName);
	}
	
	public static INeuronManagement registerNeuron(String templateName, String neuronName) {
		m_rwLock.writeLock().lock();
		try {
			final Management mgt = new Management(templateName, neuronName);
			final Management existing = m_neuronsByName.addOrFetch(neuronName, mgt);
			if (existing != null) {
				// This means we fetched the existing one, the optimistically created class is now gone
				throw new IllegalArgumentException("The neuron " + neuronName + " already exists");
			} else {
				m_neuronsById.addOrFetch(mgt.m_id, mgt);
			}
			return mgt;
		} finally {
			m_rwLock.writeLock().unlock();
		}
	}
	
	static NeuronRef currentRef(int neuronId) {
		m_rwLock.readLock().lock();
		try {
			final Management mgt = m_neuronsById.get(neuronId);
			if (mgt == null) {
				return null;
			}
			return mgt.currentRef();
		} finally {
			m_rwLock.readLock().unlock();
		}
	}
	
	public static INeuronManagement manage(String neuronName) {
		m_rwLock.readLock().lock();
		try {
			final Management mgt = m_neuronsByName.get(neuronName);
			if (mgt == null) {
				throw new IllegalArgumentException("The neuron " + neuronName + " was not created");
			}
			return mgt;
		} finally {
			m_rwLock.readLock().unlock();
		}
	}
	
	public static List<NeuronRef> neuronsForTemplate(String templateName) {
		return null; // TODO implements this <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	}

	public interface INeuronManagement {
		boolean bringOnline(ObjectConfig config);
		NeuronRef currentRef();
		NeuronRef getGenerationRef(int generation);
	}
	
	private static final class Management implements INeuronManagement {
		private final LinkedList<InstanceManagement> m_oldGen = new LinkedList<>();
		private final int m_id;
		private final String m_name;
		private final String m_templateName;
		private InstanceManagement m_current;
		
		Management(String templateName, String neuronName) {
			m_id = m_nextNeuronId.incrementAndGet();
			m_templateName = templateName;
			m_name = neuronName;
			m_current = new InstanceManagement(TemplateStateManager.manage(m_templateName).currentRef(), m_nextNeuronGen.incrementAndGet(), null);
			m_current.initOffline();
		}
		
		@Override
		public NeuronRef getGenerationRef(int generation) {
			synchronized(this) {
				for(InstanceManagement i : m_oldGen) {
					if (i.generation() == generation) {
						return i;
					}
				}
			}
			return null;
		}


		// This could be called by anybody anywhere
		@Override
		public boolean bringOnline(ObjectConfig config) {
			final TemplateRef currentTemplateRef = TemplateStateManager.manage(m_templateName).currentRef();
			try(final ITemplateStateLock templateLock = currentTemplateRef.lockState()) {
				if (templateLock.currentState() != TemplateState.Online) {
					final TemplateNotOnlineException ex = new TemplateNotOnlineException(currentTemplateRef);
					NeuronApplication.logError(LOG, "Failed creating an instance of neuron. Template {} is in the {} state", m_templateName, templateLock.currentState(), ex);
					return false;
				}
			
				final InstanceManagement mgt;
				synchronized(this) {
					if (m_current != null) {
						try(final INeuronStateLock lock = m_current.lockState()) {
							if (lock.currentState().ordinal() <= NeuronState.Online.ordinal()) {
								return true;
							}
							if (lock.currentState() != NeuronState.Offline) {
								return false;
							}
						}
						m_oldGen.add(m_current);
						while(m_oldGen.size() > MAX_OLD_GEN) {
							m_oldGen.remove();
						}
					}
					// This is the only place that m_current is ever modified
					m_current = new InstanceManagement(currentTemplateRef, m_nextNeuronGen.incrementAndGet(), config);
					mgt = m_current;
				}
				((ITemplateStateLockInternal)templateLock).registerNeuron(m_current);
				
				// At this point it is safe to use it outside the lock
				// It currently is in the state of NA and there is no way to change that except
				// by the thread there is here.
				
				mgt.setState(NeuronState.BeingCreated);
			}

			return true;
		}
		
		@Override
		public NeuronRef currentRef() {
			return m_current;
		}
		
		private final class InstanceManagement extends NeuronRef {
			private final StateManagement m_stateMgt[] = new StateManagement[NeuronState.values().length];
			private final EventLoop m_myEventLoop;
			private final LinkedList<NeuronLogEntry> m_log = new LinkedList<>();
			private final ObjectConfig m_config; 
			private INeuronInitialization m_neuron;
			private NeuronState m_state = NeuronState.NA;
			private int m_stateLockCount;
			private ITemplateStateListenerRemoval m_templateOfflineTrigger;
			
			private IntTrie<StateLock> m_lockTracking = new IntTrie<>();
			private int m_nextLockTrackingId = 1;
			private NeuronState m_pendingState = null;
			
			private InstanceManagement(TemplateRef templateRef, int gen, ObjectConfig config) {
				super(templateRef, m_id, m_name, gen);
				m_config = config;
				m_myEventLoop = NeuronApplication.getTaskPool().next();
				for(NeuronState s : NeuronState.values()) {
					m_stateMgt[s.ordinal()] = new StateManagement(s);
				}
				setSystemListeners();
			}

			private void setSystemListeners() {
				// NA -> BeingCreated -> Initializing -> Connect -> SystemOnline -> Online
				getStateManager(NeuronState.BeingCreated).setPreListener((boolean successful) -> {
					// We transitioned from NA to BeingCreated
					if (successful) {
						onBeingCreated();
					}
				});
				getStateManager(NeuronState.Initializing).setPreListener((boolean successful) -> {
					if (successful) {
						onInitializing();
					}
				});
				getStateManager(NeuronState.Connect).setPostListener((boolean successful) -> {
					if (successful) {
						onConnectResources();
					}
				});
				getStateManager(NeuronState.SystemOnline).setPostListener((boolean successful) -> {
					if (successful) {
						onSystemOnline();
					}
				});
				
				// GoingOffline -> Disconnecting -> DeInitializing -> SystemOffline -> Offline
				getStateManager(NeuronState.GoingOffline).setPostListener((boolean successful) -> {
					synchronized(InstanceManagement.this) {
						if (m_templateOfflineTrigger != null) {
							m_templateOfflineTrigger.remove();
							m_templateOfflineTrigger = null;
						}
					}
					if (successful) {
						setState(NeuronState.Disconnecting);
					}
				});
				getStateManager(NeuronState.Disconnecting).setPostListener((boolean successful) -> {
					if (successful) {
						onDeInitializing();
					}
				});
				getStateManager(NeuronState.Deinitializing).setPostListener((boolean successful) -> {
					if (successful) {
						setState(NeuronState.SystemOffline);
					}
				});
				getStateManager(NeuronState.SystemOffline).setPostListener((boolean successful) -> {
					if (successful) {
						setState(NeuronState.Offline);
					}
				});
			}

			private void onBeingCreated() {
				try(final ITemplateStateLock lock = templateRef().lockState()) {
					NeuronSystemTLS.add(this);
					try {
						if (lock.currentState() != TemplateState.Online) {
							final TemplateNotOnlineException ex = new TemplateNotOnlineException(templateRef());
							NeuronApplication.logError(LOG, "Failed creating an instance of neuron. Template {} is in the {} state", templateRef().name(), lock.currentState(), ex);
							abortToOffline(ex, false);
							return;
						}
						m_neuron = lock.createNeuron(this, m_config);
						if (m_neuron == null) {
							NeuronApplication.logError(LOG, "template.createNeuron() returned null");
							abortToOffline(new RuntimeException("template.createNeuron() returned null"), false);
							return;
						}
						synchronized(InstanceManagement.this) {
							// When the template enters the state TakeNeuronsOffline
							m_templateOfflineTrigger = lock.addStateListener(TemplateState.TakeNeuronsOffline, (isSuccess) -> {
								if (isSuccess) {
									try(INeuronStateLock neuronLock = InstanceManagement.this.lockState()) {
										// If we are Online or headed that way
										if (neuronLock.currentState().ordinal() <= NeuronState.Online.ordinal() ) {
											// Add a listener to ourselves that triggers when we go Online
											neuronLock.addStateListener(NeuronState.Online, (isSuccess2) -> {
												if (isSuccess2) {
													setState(NeuronState.GoingOffline);
												}
											});
										}
									}
								}
							});
						}
					} catch(Exception ex) {
						NeuronApplication.logError(LOG, "template.createNeuron() threw an exception", ex);
						abortToOffline(ex, false);
						return;
					} finally {
						NeuronSystemTLS.remove();
					}
				}
				setState(NeuronState.Initializing);
			}
			
			private void onInitializing() {
				final Promise<Void> p = m_myEventLoop.newPromise();
				NeuronSystemTLS.add(this);
				try {
					initializeNeuron(m_neuron, m_name, p);
				} finally {
					NeuronSystemTLS.remove();
				}
				if (p.isDone()) {
					try {
						if (p.isSuccess()) {
							setState(NeuronState.Connect);
						} else {
							NeuronSystemTLS.add(this);
							try {
								abortToOffline(p.cause(), true);
							} finally {
								NeuronSystemTLS.remove();
							}
						}
					} catch(Exception ex) {
						LOG.fatal("Unhandled exception should never happen", ex);
					}
				} else {
					p.addListener((f) -> {
						try {
							if (f.isSuccess()) {
								setState(NeuronState.Connect);
							} else {
								NeuronSystemTLS.add(this);
								try {
									abortToOffline(p.cause(), true);
								} finally {
									NeuronSystemTLS.remove();
								}
							}
						} catch(Exception ex) {
							LOG.fatal("Unhandled exception should never happen", ex);
						}
					});
				}
			}

			public void onConnectResources() {
				try(INeuronStateLock lock = m_current.lockState()) {
					try {
						m_neuron.connectResources();
					} catch(Exception ex) {
						NeuronApplication.logError(LOG, "Failure connecting resources", ex);
						abortToOffline(ex, false);
						return;
					}
					setState(NeuronState.SystemOnline);
				}
			}
			
			public void onSystemOnline() {
				try(INeuronStateLock lock = m_current.lockState()) {
					try {
						m_neuron.nowOnline();
					} catch(Exception ex) {
						NeuronApplication.logError(LOG, "Unhandled exception in neuron.nowOnline()", ex);
					}
					setState(NeuronState.Online);
				}
			}
			
			public void onDeInitializing() {
				final Promise<Void> p = m_myEventLoop.newPromise();
				NeuronSystemTLS.add(this);
				try {
					deinitializeNeuron(m_neuron, m_name, p);
				} finally {
					NeuronSystemTLS.remove();
				}
				if (p.isDone()) {
					setState(NeuronState.Deinitializing);
				} else {
					p.addListener((f) -> {
						setState(NeuronState.Deinitializing);
					});
				}
			}
			
			private void abortToOffline(Throwable t, boolean logThis) {
				if (logThis) {
					NeuronApplication.logError(LOG, "Neuron set offline due to exception in startup states", t);
				}
				final int start;
				synchronized(this) {
					start = m_state.ordinal();
					m_state = NeuronState.Offline;
					StatusSystem.setStatus(this, StatusType.Offline, "Neuron set offline due to exception in startup states");
				}
				for(int i=start; i<m_stateMgt.length-1; i++) {
					m_stateMgt[i].m_reachedStatePromise.tryFailure(t);
				}
				m_stateMgt[NeuronState.Offline.ordinal()].m_reachedStatePromise.trySuccess(this);
			}
			
			private void initOffline() {
				final int start;
				synchronized(this) {
					start = m_state.ordinal();
					m_state = NeuronState.Offline;
					StatusSystem.setStatus(this, StatusType.Offline, "registered");
				}
				Throwable dummy = new RuntimeException();
				for(int i=start; i<m_stateMgt.length-1; i++) {
					m_stateMgt[i].m_reachedStatePromise.tryFailure(dummy);
				}
				m_stateMgt[NeuronState.Offline.ordinal()].m_reachedStatePromise.trySuccess(this);
			}

			private StateManagement getStateManager(NeuronState state) {
				return m_stateMgt[state.ordinal()];
			}
			
			private void setState(NeuronState state) {
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
			
			private void setState0(NeuronState state) {
				final Promise<NeuronRef> promise = m_stateMgt[state.ordinal()].m_reachedStatePromise;
				m_state = state;
				String reasonText = state.toString();
				final StatusType st;
				if (m_state.ordinal() < NeuronState.Online.ordinal()) {
					st = StatusType.GoingOnline;
				} else if (m_state.ordinal() == NeuronState.Online.ordinal()) {
					st = StatusType.Online;
					reasonText = "";
				} else if (m_state.ordinal() == NeuronState.GoingOffline.ordinal()) {
					st = StatusType.GoingOffline;
					reasonText = "";
				} else if (m_state.ordinal() < NeuronState.Offline.ordinal()) {
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
			public INeuronStateLock lockState() {
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
			
			@Override
			public List<NeuronLogEntry> getLog() {
				final ArrayList<NeuronLogEntry> out = new ArrayList<>(MAX_LOG_SIZE);
				synchronized(m_log) {
					for(NeuronLogEntry e : m_log) {
						out.add(e);
					}
				}
				return out;
			}
			
			@Override
			public void log(Level level, StringBuilder sb) {
				synchronized(m_log) {
					m_log.add(new NeuronLogEntry(level, sb.toString()));
					while (m_log.size() > MAX_LOG_SIZE) {
						m_log.remove();
					}
				}
			}

			private final class StateManagement {
				private final NeuronState m_state;
				private final Promise<NeuronRef> m_reachedStatePromise = m_myEventLoop.newPromise();
				private FastLinkedList<ListenerHolder> m_listeners = new FastLinkedList<>();
				private INeuronStateSyncListener m_systemPreListener;
				private INeuronStateSyncListener m_systemPostListener;
				
				StateManagement(NeuronState state) {
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
							final INeuronStateListener listener = holder.m_listener;
							if (listener instanceof INeuronStateSyncListener) {
								tsp.add(NeuronApplication.getTaskPool().submit(() -> {
									NeuronSystemTLS.add(InstanceManagement.this);
									try {
										((INeuronStateSyncListener)listener).onStateReached(successful);
									} catch(Exception ex) {
										NeuronApplication.logError(LOG, "Exception calling state listener", ex);
									} finally {
										NeuronSystemTLS.remove();
									}
								}));
							} else {
								Promise<Void> promise = m_myEventLoop.newPromise();
								tsp.add(promise);
								NeuronSystemTLS.add(InstanceManagement.this);
								try {
									((INeuronStateAsyncListener)listener).onStateReached(successful, promise);
								} catch(Exception ex) {
									NeuronApplication.logError(LOG, "Exception calling state listener", ex);
								} finally {
									NeuronSystemTLS.remove();
								}
							}
							return true;
						});
						if (m_systemPostListener != null) {
							final Promise<Void> aggregatePromise = m_myEventLoop.newPromise();
							aggregatePromise.addListener((f) -> {
								NeuronSystemTLS.add(InstanceManagement.this);
								try {
									// Once all listener calls are done, we can let the system process
									m_systemPostListener.onStateReached(successful);
								} catch(Exception ex) {
									LOG.error("Exception calling post state listener", ex);
								} finally {
									NeuronSystemTLS.remove();
								}
							});
							tsp.finish(aggregatePromise);
						}
					});
				}
				
				void setPreListener(INeuronStateSyncListener listener) {
					m_systemPreListener = listener;
				}
				
				void setPostListener(INeuronStateSyncListener listener) {
					m_systemPostListener = listener;
				}
				
				INeuronStateListenerRemoval addListener(INeuronStateListener listener, NeuronState currentState) {
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
					if (listener instanceof INeuronStateAsyncListener) {
						throw new IllegalArgumentException("Cannot add async listener to state " + m_state + ", the neuron is already in the state " + currentState);
					}
					// Our promise listener has fired and will not call any additional listeners, call it on this thread
					NeuronSystemTLS.add(InstanceManagement.this);
					try {
						if (listener instanceof INeuronStateSyncListener) {
							((INeuronStateSyncListener)listener).onStateReached(m_reachedStatePromise.isSuccess());
//						} else {
//							// The promise is ignored, since we already reached the state
//							((INeuronStateAsyncListener)listener).onStateReached(m_reachedStatePromise.isSuccess(), m_myEventLoop.newPromise());
						}
					} catch(Exception ex) {
						NeuronApplication.logError(LOG, "Exception calling state listener", ex);
					} finally {
						NeuronSystemTLS.remove();
					}
					return null;
				}
				
				private final class ListenerHolder extends FastLinkedList.LLNode<ListenerHolder> implements INeuronStateListenerRemoval {
					private final INeuronStateListener m_listener;
					
					ListenerHolder(INeuronStateListener listener) {
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

			private final class StateLock implements INeuronStateLock {
				private final Thread m_lockingThread;
				private boolean m_locked = true;
				private int m_lockTrackingId;
				@SuppressWarnings("unused")
				private StackTraceElement[] m_lockStackTrace;
				
				StateLock() {
					m_lockingThread = Thread.currentThread();
				}
				
				@Override
				public NeuronState currentState() {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					return m_state;
				}

				@Override
				public INeuronStateListenerRemoval addStateListener(NeuronState state, INeuronStateSyncListener listener) {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					return getStateManager(state).addListener(listener, m_state);
				}
				
				@Override
				public INeuronStateListenerRemoval addStateAsyncListener(NeuronState state, INeuronStateAsyncListener listener) {
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
					if (m_state == NeuronState.Online) {
						setState(NeuronState.GoingOffline);
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
	
	private static void initializeNeuron(INeuronInitialization neuron, String name, final Promise<Void> initDone) {
		try {
			neuron.init(initDone);
		} catch(Exception ex) {
//			NeuronApplication.logError(LOG, "Exception thrown from neuron.init()", ex);
			initDone.tryFailure(ex);
			return;
		}
		if (initDone.isDone()) {
			return;
		}
		long timeoutInMS;
		try {
			timeoutInMS = neuron.initTimeoutInMS();
			if (timeoutInMS <= 0) {
				NeuronApplication.logError(LOG, "Init timeout from neuron '{}' is invalid (returned value was {} which is not allowed).  Using default value of {} instead.",  name, timeoutInMS, DEFAULT_INIT_TIMEOUT_IN_MS);
				timeoutInMS = DEFAULT_INIT_TIMEOUT_IN_MS;
			}
		} catch(Exception ex) {
			timeoutInMS = DEFAULT_INIT_TIMEOUT_IN_MS;
			NeuronApplication.logError(LOG, "Failure getting init timeout from neuron '{}',  Using default value of {} instead.", name, timeoutInMS, ex);
		}
		final NeuronRef ref = NeuronSystemTLS.currentNeuron();
		final long toMS = timeoutInMS;
		final ScheduledFuture<?> initTimeout = NeuronApplication.getTaskPool().schedule(() -> {
			if (initDone.tryFailure( new RuntimeException("Timeout of " + toMS + "ms initializing neuron '" + name + "'") )) {
				NeuronSystemTLS.add(ref);
				try {
					neuron.onInitTimeout();
				} catch(Exception ex) {
					NeuronApplication.logError(LOG, "Exception from neuron.onInitTimeout()", ex);
				} finally {
					NeuronSystemTLS.remove();
				}
			}
		}, timeoutInMS, TimeUnit.MILLISECONDS);
		
		initDone.addListener((Future<Void> f) -> {
			initTimeout.cancel(false);
		});
	}
	
	private static void deinitializeNeuron(INeuronInitialization neuron, String name, final Promise<Void> deinitDone) {
		try {
			neuron.deinit(deinitDone);
		} catch(Exception ex) {
			NeuronApplication.logError(LOG, "Exception thrown from neuron.deinit()", ex);
			deinitDone.tryFailure(ex);
			return;
		}
		if (deinitDone.isDone()) {
			return;
		}
		long timeoutInMS;
		try {
			timeoutInMS = neuron.deinitTimeoutInMS();
			if (timeoutInMS <= 0) {
				NeuronApplication.logError(LOG, "Deinit timeout from neuron '{}' is invalid (returned value was {} which is not allowed).  Using default value of {} instead.",  name, timeoutInMS, DEFAULT_INIT_TIMEOUT_IN_MS);
				timeoutInMS = DEFAULT_INIT_TIMEOUT_IN_MS;
			}
		} catch(Exception ex) {
			timeoutInMS = DEFAULT_INIT_TIMEOUT_IN_MS;
			NeuronApplication.logError(LOG, "Failure getting Deinit timeout from neuron '{}',  Using default value of {} instead.", name, timeoutInMS, ex);
		}
		final NeuronRef ref = NeuronSystemTLS.currentNeuron();
		final long toMS = timeoutInMS;
		final ScheduledFuture<?> deinitTimeout = NeuronApplication.getTaskPool().schedule(() -> {
			if (deinitDone.tryFailure( new RuntimeException("Timeout of " + toMS + "ms de-initializing neuron '" + name + "'") )) {
				NeuronSystemTLS.add(ref);
				try {
					neuron.onDeinitTimeout();
				} catch(Exception ex) {
					NeuronApplication.logError(LOG, "Exception from neuron.onDeinitTimeout()", ex);
				} finally {
					NeuronSystemTLS.remove();
				}
			}
		}, timeoutInMS, TimeUnit.MILLISECONDS);
		
		deinitDone.addListener((Future<Void> f) -> {
			deinitTimeout.cancel(false);
		});
	}
	
}
