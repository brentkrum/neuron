package com.neuron.core;

import java.lang.reflect.Constructor;
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
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.StatusSystem.StatusType;
import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.netty.TSPromiseCombiner;
import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.FastLinkedList;
import com.neuron.utility.IntTrie;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

public final class TemplateStateManager {
	public enum TemplateState { NA, BeingCreated, Initializing, RunSelfTest, SystemOnline, Online, TakeNeuronsOffline, SystemOffline, Offline };

	private static final boolean LOCK_LEAK_DETECTION = Config.getFWBoolean("core.TemplateStateManager.lockLeakDetection", false);
	private static final long DEFAULT_INIT_TIMEOUT_IN_MS = Config.getFWInt("core.TemplateStateManager.defaultInitTimeout", 5000);
	private static final long DEFAULT_SELF_TEST_TIMEOUT_IN_MS = Config.getFWInt("core.TemplateStateManager.defaultSelfTestTimeout", 15000);
	private static final int MAX_LOG_SIZE = Config.getFWInt("core.TemplateStateManager.logSize", 64);
	
	private static final Logger LOG = LogManager.getLogger(TemplateStateManager.class);

	private static final ReadWriteLock m_rwLock = new ReentrantReadWriteLock();
	private static final IntTrie<Management> m_templatesById = new IntTrie<>();
	private static final CharSequenceTrie<Management> m_templatesByName = new CharSequenceTrie<>();
	private static final AtomicInteger m_nextTemplateId = new AtomicInteger(1);
	private static final AtomicInteger m_nextTemplateGen = new AtomicInteger(1);
	
	public static void enableSelfTest() {
	}
	
	public static ITemplateManagement registerTemplate(String templateName, Class<? extends INeuronTemplate> templateClass) {
		m_rwLock.writeLock().lock();
		try {
			final Management mgt = new Management(templateName, templateClass);
			final Management existing = m_templatesByName.addOrFetch(templateName, mgt);
			if (existing != null) {
				// This means we fetched the existing one, the optimistically created class is now gone
				throw new IllegalArgumentException("The template " + templateName + " already exists registered with the class " + existing.m_templateClass.getCanonicalName());
			} else {
				m_templatesById.addOrFetch(mgt.m_id, mgt);
			}
			return mgt;
		} finally {
			m_rwLock.writeLock().unlock();
		}
	}
	
	public static ITemplateManagement manage(String templateName) {
		m_rwLock.readLock().lock();
		try {
			final Management mgt = m_templatesByName.get(templateName);
			if (mgt == null) {
				throw new IllegalArgumentException("The template " + templateName + " was not registered");
			}
			return mgt;
		} finally {
			m_rwLock.readLock().unlock();
		}
	}

	public interface ITemplateManagement {
		boolean bringOnline();
		TemplateRef currentRef();
	}
	
	private static final class Management implements ITemplateManagement {
		private final LinkedList<InstanceManagement> m_oldGen = new LinkedList<>();
		private final int m_id;
		private final String m_name;
		private final Class<? extends INeuronTemplate> m_templateClass;
		private final Constructor<? extends INeuronTemplate> m_constructor;
		
		private InstanceManagement m_current;
		
		Management(String templateName, Class<? extends INeuronTemplate> templateClass) {
			m_id = m_nextTemplateId.incrementAndGet();
			m_name = templateName;
			m_templateClass = templateClass;
			try {
				m_constructor = m_templateClass.getConstructor(TemplateRef.class);
			} catch (Exception ex) {
				throw new IllegalArgumentException("The template " + templateName + " with the class " + m_templateClass.getCanonicalName() + " does not have a constructor which takes only a TemplateRef", ex);
			}
			m_current = new InstanceManagement(m_nextTemplateGen.incrementAndGet());
			m_current.initOffline();
		}
		
		@Override
		public boolean bringOnline() {
			// This could be called by anybody anywhere
			synchronized(this) {
				if (m_current != null) {
					try(final ITemplateStateLock lock = m_current.lockState()) {
						if (lock.currentState().ordinal() <= TemplateState.Online.ordinal()) {
							return true;
						}
						if (lock.currentState() != TemplateState.Offline) {
							return false;
						}
					}
					m_oldGen.add(m_current);
				}
				// This is the only place that m_current is ever modified
				m_current = new InstanceManagement(m_nextTemplateGen.incrementAndGet());
			}
			// At this point it is safe to use it outside the lock
			// It currently is in the state of NA and there is no way to change that except
			// by the thread there is here.
			m_current.setState(TemplateState.BeingCreated);

			return true;
		}
		
		@Override
		public TemplateRef currentRef() {
			return m_current;
		}
		
		private final class InstanceManagement extends TemplateRef {
			private final StateManagement m_stateMgt[] = new StateManagement[TemplateState.values().length];
			private final EventLoop m_myEventLoop;
			private final LinkedList<NeuronLogEntry> m_log = new LinkedList<>();
			private final IntTrie<NeuronRef> m_activeNeuronsByGen = new IntTrie<>();
			private INeuronTemplate m_template;
			private TemplateState m_state = TemplateState.NA;
			private int m_stateLockCount;
			
			private IntTrie<StateLock> m_lockTracking = new IntTrie<>();
			private int m_nextLockTrackingId = 1;
			private TemplateState m_pendingState = null;
			
			private InstanceManagement(int gen) {
				super(m_id, m_name, gen);
				m_myEventLoop = NeuronApplication.getTaskPool().next();
				for(TemplateState s : TemplateState.values()) {
					m_stateMgt[s.ordinal()] = new StateManagement(s);
				}
				setSystemListeners();
			}
			
			private void setSystemListeners() {
				// NA -> BeingCreated -> Initializing -> RunSelfTest -> SystemOnline -> Online
				getStateManager(TemplateState.BeingCreated).setPreListener((isSuccessful) -> {
					if (isSuccessful) {
						onBeingCreated();
					}
				});
				getStateManager(TemplateState.Initializing).setPreListener((isSuccessful) -> {
					if (isSuccessful) {
						onInitializing();
					}
				});
				getStateManager(TemplateState.RunSelfTest).setPostListener((isSuccessful) -> {
					if (isSuccessful) {
						setState(TemplateState.SystemOnline);
					}
				});
				getStateManager(TemplateState.SystemOnline).setPostListener((isSuccessful) -> {
					if (isSuccessful) {
						setState(TemplateState.Online);
					}
				});
				
				// TakeNeuronsOffline -> SystemOffline -> Offline
				getStateManager(TemplateState.TakeNeuronsOffline).setPostListener((isSuccessful) -> {
					if (isSuccessful) {
						try(ITemplateStateLock templateLock = lockState()) {
							final int neuronsLeft;
							synchronized(m_activeNeuronsByGen) {
								neuronsLeft = m_activeNeuronsByGen.count();
//								if (neuronsLeft > 0) {
//									m_activeNeuronsByGen.forEach((key, value) -> {
//										try(INeuronStateLock nlock = value.lockState()) {
//											LOG.error("Neuron {} is in state {} and still attached", value.logString(), nlock.currentState());
//										}
//										return true;
//									});
//								}
							}
							if (neuronsLeft == 0) {
								setState(TemplateState.SystemOffline);
//							} else {
//								LOG.error("There are {} neurons still online", neuronsLeft);
							}
						}
					}
				});
				getStateManager(TemplateState.SystemOffline).setPostListener((isSuccessful) -> {
					if (isSuccessful) {
						setState(TemplateState.Offline);
					}
				});
			}

			private void onBeingCreated() {
				NeuronSystemTLS.add(this);
				try {
					m_template = m_constructor.newInstance((TemplateRef)this) ;
				} catch (Exception ex) {
					NeuronApplication.logError(LOG, "Failed creating an instance of neuron template class {}", m_templateClass.getCanonicalName(), ex);
					abortToOffline(ex, false);
					return;
				} finally {
					NeuronSystemTLS.remove();
				}
				setState(TemplateState.Initializing);
			}
			
			private void onInitializing() {
				final Promise<Void> p = m_myEventLoop.newPromise();
				NeuronSystemTLS.add(this);
				try {
					initializeTemplate(m_template, m_name, p);
				} finally {
					NeuronSystemTLS.remove();
				}
				if (p.isDone()) {
					try {
						if (p.isSuccess()) {
							setState(TemplateState.RunSelfTest);
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
								setState(TemplateState.RunSelfTest);
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
			
			private void abortToOffline(Throwable t, boolean logThis) {
				if (logThis) {
					NeuronApplication.logError(LOG, "Template set offline due to exception in startup states", t);
				}
				final int start;
				synchronized(this) {
					start = Integer.max(1, m_state.ordinal());
					m_state = TemplateState.Offline;
					StatusSystem.setStatus(this, StatusType.Offline, "Template set offline due to exception in startup states");
				}
				for(int i=start; i<m_stateMgt.length-1; i++) {
					m_stateMgt[i].m_reachedStatePromise.tryFailure(t);
				}
				m_stateMgt[TemplateState.Offline.ordinal()].m_reachedStatePromise.trySuccess(this);
			}
			
			private void initOffline() {
				final int start;
				synchronized(this) {
					start = Integer.max(1, m_state.ordinal());
					m_state = TemplateState.Offline;
					StatusSystem.setStatus(this, StatusType.Offline, "registered");
				}
				Throwable dummy = new RuntimeException();
				for(int i=start; i<m_stateMgt.length-1; i++) {
					m_stateMgt[i].m_reachedStatePromise.tryFailure(dummy);
				}
				m_stateMgt[TemplateState.Offline.ordinal()].m_reachedStatePromise.trySuccess(this);
			}

			private StateManagement getStateManager(TemplateState state) {
				return m_stateMgt[state.ordinal()];
			}
			
			private void setState(TemplateState state) {
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
			
			private void setState0(TemplateState state) {
				final Promise<TemplateRef> promise = m_stateMgt[state.ordinal()].m_reachedStatePromise;
				m_state = state;
				String reasonText = state.toString();
				final StatusType st;
				if (m_state.ordinal() < TemplateState.Online.ordinal()) {
					st = StatusType.GoingOnline;
				} else if (m_state.ordinal() == TemplateState.Online.ordinal()) {
					st = StatusType.Online;
					reasonText = "";
				} else if (m_state.ordinal() < TemplateState.Offline.ordinal()) {
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
			public ITemplateStateLock lockState() {
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
				private final TemplateState m_state;
				private final Promise<TemplateRef> m_reachedStatePromise = m_myEventLoop.newPromise();
				private FastLinkedList<ListenerHolder> m_listeners = new FastLinkedList<>();
				private ITemplateStateSyncListener m_systemPreListener;
				private ITemplateStateSyncListener m_systemPostListener;
				
				StateManagement(TemplateState state) {
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
							final ITemplateStateListener listener = holder.m_listener;
							if (listener instanceof ITemplateStateSyncListener) {
								tsp.add(NeuronApplication.getTaskPool().submit(() -> {
									NeuronSystemTLS.add(InstanceManagement.this);
									try {
										((ITemplateStateSyncListener)listener).onStateReached(successful);
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
									((ITemplateStateAsyncListener)listener).onStateReached(successful, promise);
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
								// Once all listener calls are done, we can let the system process
								m_systemPostListener.onStateReached(successful);
							});
							tsp.finish(aggregatePromise);
						}
					});
				}
				
				void setPreListener(ITemplateStateSyncListener listener) {
					m_systemPreListener = listener;
				}
				
				void setPostListener(ITemplateStateSyncListener listener) {
					m_systemPostListener = listener;
				}
				
				ITemplateStateListenerRemoval addListener(ITemplateStateListener listener, TemplateState currentState) {
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
					if (listener instanceof ITemplateStateAsyncListener) {
						throw new IllegalArgumentException("Cannot add async listener to state " + m_state + ", the template is already in the state " + currentState);
					}
					// Our promise listener has fired and will not call any additional listeners, call it on this thread
					NeuronSystemTLS.add(InstanceManagement.this);
					try {
						if (listener instanceof ITemplateStateSyncListener) {
							((ITemplateStateSyncListener)listener).onStateReached(m_reachedStatePromise.isSuccess());
//						} else {
//							// The promise is ignored, since we already reached the state
//							((ITemplateStateAsyncListener)listener).onStateReached(m_reachedStatePromise.isSuccess(), m_myEventLoop.newPromise());
						}
					} catch(Exception ex) {
						NeuronApplication.logError(LOG, "Exception calling state listener", ex);
					} finally {
						NeuronSystemTLS.remove();
					}
					return null;
				}
				
				private final class ListenerHolder extends FastLinkedList.LLNode<ListenerHolder> implements ITemplateStateListenerRemoval {
					private final ITemplateStateListener m_listener;
					
					ListenerHolder(ITemplateStateListener listener) {
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

			private final class StateLock implements ITemplateStateLock {
				private final Thread m_lockingThread;
				private boolean m_locked = true;
				private int m_lockTrackingId;
				private StackTraceElement[] m_lockStackTrace;
				
				StateLock() {
					m_lockingThread = Thread.currentThread();
				}
				
				@Override
				public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
					synchronized(m_activeNeuronsByGen) {
						if (m_activeNeuronsByGen.addOrFetch(ref.generation(), ref) != null) {
							LOG.fatal("Added a neuron reference which already existed.  This should never happen.", new RuntimeException("Just for the stack trace"));
							NeuronApplication.fatalExit();
						}
					}
					try(INeuronStateLock neuronLock = ref.lockState()) {
						neuronLock.addStateListener(NeuronState.Offline, (success) -> {
							final int neuronsLeft;
							synchronized(m_activeNeuronsByGen) {
								if (m_activeNeuronsByGen.remove(ref.generation()) == null) {
									LOG.fatal("A neuron reference did not exist.  This should never happen.", new RuntimeException("Just for the stack trace"));
									NeuronApplication.fatalExit();
								}
								neuronsLeft = m_activeNeuronsByGen.count();
//								if (neuronsLeft > 0) {
//									m_activeNeuronsByGen.forEach((key, value) -> {
//										try(INeuronStateLock nlock = value.lockState()) {
//											LOG.error("(2)Neuron {} is in state {} and still attached", value.logString(), nlock.currentState());
//										}
//										return true;
//									});
//								}
							}
							if (neuronsLeft == 0) {
								setState(TemplateState.SystemOffline);
//							} else {
//								LOG.error("(2)There are {} neurons still online", neuronsLeft);
							}
						});
					}
					// Exceptions are handled by NeuronStateManager
					INeuronInitialization neuron = m_template.createNeuron(ref, config);
					return neuron;
				}

				@Override
				public TemplateState currentState() {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					return m_state;
				}

				@Override
				public ITemplateStateListenerRemoval addStateListener(TemplateState state, ITemplateStateSyncListener listener) {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					return getStateManager(state).addListener(listener, m_state);
				}
				
				@Override
				public ITemplateStateListenerRemoval addStateAsyncListener(TemplateState state, ITemplateStateAsyncListener listener) {
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
					if (m_state == TemplateState.Online) {
						setState(TemplateState.TakeNeuronsOffline);
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
	
	private static void initializeTemplate(INeuronTemplate template, String templateName, final Promise<Void> initDone) {
		try {
			template.init(initDone);
		} catch(Exception ex) {
//			NeuronApplication.logError(LOG, "Exception thrown from template.init()", ex);
			initDone.tryFailure(ex);
			return;
		}
		if (initDone.isDone()) {
			return;
		}
		long timeoutInMS;
		try {
			timeoutInMS = template.initTimeoutInMS();
			if (timeoutInMS <= 0) {
				NeuronApplication.logError(LOG, "Init timeout from template '{}' is invalid (returned value was {} which is not allowed).  Using default value of {} instead.",  templateName, timeoutInMS, DEFAULT_INIT_TIMEOUT_IN_MS);
				timeoutInMS = DEFAULT_INIT_TIMEOUT_IN_MS;
			}
		} catch(Exception ex) {
			timeoutInMS = DEFAULT_INIT_TIMEOUT_IN_MS;
			NeuronApplication.logError(LOG, "Failure getting init timeout from template '{}',  Using default value of {} instead.", templateName, timeoutInMS, ex);
		}
		final long toMS = timeoutInMS;
		final ScheduledFuture<?> initTimeout = NeuronApplication.getTaskPool().schedule(() -> {
			initDone.tryFailure( new RuntimeException("Timeout of " + toMS + "ms initializing neuron template '" + templateName + "'") );
		}, timeoutInMS, TimeUnit.MILLISECONDS);
		
		initDone.addListener((Future<Void> f) -> {
			initTimeout.cancel(false);
		});
	}
}
