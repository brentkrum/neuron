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
import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.TemplateStateManager.TemplateState;
import com.neuron.core.netty.TSPromiseCombiner;
import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.IntTrie;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

public final class NeuronStateManager {
	public enum NeuronState { NA, BeingCreated, Initializing, Connect, SystemOnline, Online, GoingOffline, Disconnecting, DeInitializing, SystemOffline, Offline};

	private static final boolean LOCK_LEAK_DETECTION = Config.getFWBoolean("core.NeuronStateManager.lockLeakDetection", false);
	private static final long DEFAULT_INIT_TIMEOUT_IN_MS = Config.getFWInt("core.NeuronStateManager.defaultInitTimeout", 5000);
	private static final int MAX_LOG_SIZE = Config.getFWInt("core.NeuronStateManager.logSize", 64);
	
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
		}
		
		// This could be called by anybody anywhere
		@Override
		public boolean bringOnline(ObjectConfig config) {
			final TemplateRef currentTemplateRef = TemplateStateManager.manage(m_templateName).currentRef();
			try(final ITemplateStateLock lock = currentTemplateRef.lockState()) {
				if (lock.currentState() != TemplateState.Online) {
					final TemplateNotOnlineException ex = new TemplateNotOnlineException(currentTemplateRef);
					NeuronApplication.logError(LOG, "Failed creating an instance of neuron. Template {} is in the {} state", m_templateName, lock.currentState(), ex);
					return false;
				}
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
				}
				// This is the only place that m_current is ever modified
				m_current = new InstanceManagement(currentTemplateRef, m_nextNeuronGen.incrementAndGet());
				mgt = m_current;
			}
			// At this point it is safe to use it outside the lock
			// It currently is in the state of NA and there is no way to change that except
			// by the thread there is here.
			
			// NA -> BeingCreated -> Initializing -> Connect -> SystemOnline -> Online
			mgt.getStateManager(NeuronState.BeingCreated).setPreListener((boolean successful) -> {
				// We transitioned from NA to BeingCreated
				mgt.onBeingCreated(config);
			});
			mgt.getStateManager(NeuronState.Initializing).setPreListener((boolean successful) -> {
				if (successful) {
					mgt.onInitializing();
				}
			});
			mgt.getStateManager(NeuronState.Connect).setPostListener((boolean successful) -> {
				if (successful) {
					mgt.onConnectResources();
				}
			});
			mgt.getStateManager(NeuronState.SystemOnline).setPostListener((boolean successful) -> {
				if (successful) {
					mgt.onSystemOnline();
				}
			});
			
			// GoingOffline -> Disconnecting -> DeInitializing -> SystemOffline -> Offline
			mgt.getStateManager(NeuronState.GoingOffline).setPostListener((boolean successful) -> {
				if (successful) {
					mgt.setState(NeuronState.Disconnecting);
				}
			});
			mgt.getStateManager(NeuronState.Disconnecting).setPostListener((boolean successful) -> {
				if (successful) {
					mgt.setState(NeuronState.DeInitializing);
				}
			});
			mgt.getStateManager(NeuronState.DeInitializing).setPostListener((boolean successful) -> {
				if (successful) {
					mgt.setState(NeuronState.SystemOffline);
				}
			});
			mgt.getStateManager(NeuronState.SystemOffline).setPostListener((boolean successful) -> {
				if (successful) {
					mgt.setState(NeuronState.Offline);
				}
			});
			
			mgt.setState(NeuronState.BeingCreated);

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
			private INeuronInitialization m_neuron;
			private NeuronState m_state = NeuronState.NA;
			private int m_stateLockCount;
			
			private IntTrie<StateLock> m_lockTracking = new IntTrie<>();
			private int m_nextLockTrackingId = 1;
			private NeuronState m_pendingState = null;
			
			private InstanceManagement(TemplateRef templateRef, int gen) {
				super(templateRef, m_id, m_name, gen);
				m_myEventLoop = NeuronApplication.getTaskPool().next();
				for(NeuronState s : NeuronState.values()) {
					m_stateMgt[s.ordinal()] = new StateManagement(s);
				}
			}

			private void onBeingCreated(ObjectConfig config) {
				try(final ITemplateStateLock lock = templateRef().lockState()) {
					NeuronSystemTLS.add(this);
					try {
						if (lock.currentState() != TemplateState.Online) {
							final TemplateNotOnlineException ex = new TemplateNotOnlineException(templateRef());
							NeuronApplication.logError(LOG, "Failed creating an instance of neuron. Template {} is in the {} state", templateRef().name(), lock.currentState(), ex);
							abortToOffline(ex, false);
							return;
						}
						m_neuron = lock.createNeuron(this, config);
						if (m_neuron == null) {
							NeuronApplication.logError(LOG, "template.createNeuron() returned null");
							abortToOffline(new RuntimeException("template.createNeuron() returned null"), false);
							return;
						}
						// TODO need to remove the listener when we go offline (but only if the template is still online) <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
						lock.addStateListener(TemplateState.TakeNeuronsOffline, (f) -> {
							if (f.isSuccess()) {
								try(INeuronStateLock offlineLock = m_current.lockState()) {
									if (offlineLock.currentState() == NeuronState.Online) {
										setState(NeuronState.GoingOffline);
									}
								}
							}
						});
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
			
			private void abortToOffline(Throwable t, boolean logThis) {
				if (logThis) {
					NeuronApplication.logError(LOG, "Neuron going offline due to exception in startup states", t);
				}
				final int start;
				synchronized(this) {
					start = m_state.ordinal();
					m_state = NeuronState.Offline;
					StatusSystem.setStatus(this, m_state.toString());
				}
				for(int i=start; i<m_stateMgt.length-1; i++) {
					m_stateMgt[i].m_promise.tryFailure(t);
				}
				m_stateMgt[NeuronState.Offline.ordinal()].m_promise.trySuccess(this);
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
						final Promise<NeuronRef> promise = m_stateMgt[state.ordinal()].m_promise;
						m_state = state;
						StatusSystem.setStatus(this, m_state.toString());
						if (!promise.trySuccess(InstanceManagement.this)) {
							LOG.fatal("Failed setting promise state to {}. This should never happen.", m_state);
						}
					} else {
						m_pendingState = state;
					}
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
							Promise<NeuronRef> promise = m_stateMgt[m_pendingState.ordinal()].m_promise;
							m_state = m_pendingState;
							m_pendingState = null;
							StatusSystem.setStatus(InstanceManagement.this, m_state.toString());
							if (!promise.trySuccess(InstanceManagement.this)) {
								LOG.fatal("Failed setting promise state to {}. This should never happen.", m_state);
							}
						}
					}
				} finally {
					NeuronSystemTLS.remove();
				}
			}
			
			@Override
			List<NeuronLogEntry> getLog() {
				final ArrayList<NeuronLogEntry> out = new ArrayList<>(MAX_LOG_SIZE);
				synchronized(m_log) {
					for(NeuronLogEntry e : m_log) {
						out.add(e);
					}
				}
				return out;
			}
			
			@Override
			void log(Level level, StringBuilder sb) {
				synchronized(m_log) {
					m_log.add(new NeuronLogEntry(level, sb.toString()));
					while (m_log.size() > MAX_LOG_SIZE) {
						m_log.remove();
					}
				}
			}

			private final class StateManagement {
				@SuppressWarnings("unused")
				private final NeuronState m_state;
				private final Promise<NeuronRef> m_promise = m_myEventLoop.newPromise();
				private LinkedList<INeuronStateListener> m_listeners = new LinkedList<>();
				private INeuronStateListener m_systemPreListener;
				private INeuronStateListener m_systemPostListener;
				
				StateManagement(NeuronState state) {
					m_state = state;
					m_promise.addListener((statePromise) -> {
						final boolean successful = statePromise.isSuccess();
						if (m_systemPreListener != null) {
							m_systemPreListener.onStateReached(successful);
						}
						final LinkedList<INeuronStateListener> listeners;
						synchronized(StateManagement.this) {
							listeners = m_listeners;
							m_listeners = null;
						}
						final TSPromiseCombiner tsp = new TSPromiseCombiner();
						for(INeuronStateListener listener : listeners) {
							tsp.add(NeuronApplication.getTaskPool().submit(() -> {
								NeuronSystemTLS.add(InstanceManagement.this);
								try {
									listener.onStateReached(successful);
								} catch(Exception ex) {
									NeuronApplication.logError(LOG, "Exception calling state listener", ex);
								} finally {
									NeuronSystemTLS.remove();
								}
							}));
						}
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
				
				void setPreListener(INeuronStateListener listener) {
					m_systemPreListener = listener;
				}
				
				void setPostListener(INeuronStateListener listener) {
					m_systemPostListener = listener;
				}
				
				void addListener(INeuronStateListener listener) {
					synchronized(this) {
						// m_listeners goes null once the promise has been set complete and the callback has been called
						// until that time, we can keep adding to it
						if (m_listeners != null) {
							m_listeners.addFirst(listener);
							return;
						}
					}
					// Our promise listener has fired and will not call any additional listeners, call it on this thread
					NeuronSystemTLS.add(InstanceManagement.this);
					try {
						listener.onStateReached(m_promise.isSuccess());
					} catch(Exception ex) {
						NeuronApplication.logError(LOG, "Exception calling state listener", ex);
					} finally {
						NeuronSystemTLS.remove();
					}
				}
				
				
			}

			private final class StateLock implements INeuronStateLock {
				private final Thread m_lockingThread;
				private boolean m_locked = true;
				private int m_lockTrackingId;
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
				public void addStateListener(NeuronState state, INeuronStateListener listener) {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					getStateManager(state).addListener(listener);
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
			NeuronApplication.logError(LOG, "Exception thrown from neuron.init()", ex);
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
		final long toMS = timeoutInMS;
		final ScheduledFuture<?> initTimeout = NeuronApplication.getTaskPool().schedule(() -> {
			initDone.tryFailure( new RuntimeException("Timeout of " + toMS + "ms initializing neuron '" + name + "'") );
		}, timeoutInMS, TimeUnit.MILLISECONDS);
		
		NeuronRef ref = NeuronSystemTLS.currentNeuron();
		initDone.addListener((Future<Void> f) -> {
			initTimeout.cancel(false);
			NeuronSystemTLS.add(ref);
			try {
				neuron.onInitTimeout();
			} catch(Exception ex) {
				NeuronApplication.logError(LOG, "Exception from neuron.onInitTimeout()", ex);
			} finally {
				NeuronSystemTLS.remove();
			}
		});
	}
}
