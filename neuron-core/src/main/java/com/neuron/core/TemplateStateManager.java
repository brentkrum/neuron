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
import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.IntTrie;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
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
		Future<TemplateRef> bringOnline();
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
		public Future<TemplateRef> bringOnline() {
			// This could be called by anybody anywhere
			synchronized(this) {
				if (m_current != null) {
					try(final ITemplateStateLock lock = m_current.lockState()) {
						if (lock.currentState().ordinal() <= TemplateState.Online.ordinal()) {
							return m_current.getStateFuture(TemplateState.Online);
						}
						if (lock.currentState() != TemplateState.Offline) {
							return null;
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

			return m_current.getStateFuture(TemplateState.Online);
		}
		
		@Override
		public TemplateRef currentRef() {
			return m_current;
		}
		
		private final class InstanceManagement extends TemplateRef {
			@SuppressWarnings("unchecked")
			private final Promise<TemplateRef> m_statePromise[] = new Promise[TemplateState.values().length];
			private final EventLoop m_myEventLoop;
			private final LinkedList<NeuronLogEntry> m_log = new LinkedList<>();
			private final IntTrie<Boolean> m_activeNeuronsById = new IntTrie<>();
			private INeuronTemplate m_template;
			private TemplateState m_state = TemplateState.NA;
			private int m_stateLockCount;
			
			private IntTrie<StateLock> m_lockTracking = new IntTrie<>();
			private int m_nextLockTrackingId = 1;
			private TemplateState m_pendingState = null;
			
			private InstanceManagement(int gen) {
				super(m_id, m_name, gen);
				m_myEventLoop = NeuronApplication.getTaskPool().next();
				for(int i=1; i<m_statePromise.length; i++) {
					m_statePromise[i] = m_myEventLoop.newPromise();
				}
				setSystemListeners();
			}
			
			private void setSystemListeners() {
				// NA -> BeingCreated -> Initializing -> RunSelfTest -> SystemOnline -> Online
				getStateFuture(TemplateState.BeingCreated).addListener((f) -> {
					if (f.isSuccess()) {
						onBeingCreated();
					}
				});
				getStateFuture(TemplateState.Initializing).addListener((f) -> {
					if (f.isSuccess()) {
						onInitializing();
					}
				});
				getStateFuture(TemplateState.RunSelfTest).addListener((f) -> {
					if (f.isSuccess()) {
						setState(TemplateState.SystemOnline);
					}
				});
				getStateFuture(TemplateState.SystemOnline).addListener((f) -> {
					if (f.isSuccess()) {
						setState(TemplateState.Online);
					}
				});
				
				// TakeNeuronsOffline -> SystemOffline -> Offline
				getStateFuture(TemplateState.TakeNeuronsOffline).addListener((f) -> {
					if (f.isSuccess()) {
						try(ITemplateStateLock templateLock = lockState()) {
							if (templateLock.currentState() == TemplateState.TakeNeuronsOffline) {
								final int neuronsLeft;
								synchronized(m_activeNeuronsById) {
									neuronsLeft = m_activeNeuronsById.count();
								}
								if (neuronsLeft == 0) {
									setState(TemplateState.SystemOffline);
								}
							}
						}
					}
				});
				getStateFuture(TemplateState.SystemOffline).addListener((f) -> {
					if (f.isSuccess()) {
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
					NeuronApplication.logError("Template set offline due to exception in startup states", t);
				}
				final int start;
				synchronized(this) {
					start = Integer.max(1, m_state.ordinal());
					m_state = TemplateState.Offline;
					StatusSystem.setStatus(this, StatusType.Offline, "Template set offline due to exception in startup states");
				}
				for(int i=start; i<m_statePromise.length-1; i++) {
					m_statePromise[i].tryFailure(t);
				}
				m_statePromise[TemplateState.Offline.ordinal()].trySuccess(this);
			}
			
			private void initOffline() {
				final int start;
				synchronized(this) {
					start = Integer.max(1, m_state.ordinal());
					m_state = TemplateState.Offline;
					StatusSystem.setStatus(this, StatusType.Offline, "registered");
				}
				Throwable t = new RuntimeException();
				for(int i=start; i<m_statePromise.length-1; i++) {
					m_statePromise[i].tryFailure(t);
				}
				m_statePromise[TemplateState.Offline.ordinal()].trySuccess(this);
			}

			private Future<TemplateRef> getStateFuture(TemplateState state) {
				return m_statePromise[state.ordinal()];
			}
			
			private Future<TemplateRef> setState(TemplateState state) {
				final Promise<TemplateRef> promise;
				synchronized(this) {
					if (state.ordinal() != m_state.ordinal()+1) {
						throw new IllegalArgumentException("Current state is " + m_state + ", cannot set state to " + state);
					}
					promise = m_statePromise[state.ordinal()];
					if (m_stateLockCount == 0) {
						setState0(state, promise);
					} else {
						m_pendingState = state;
					}
				}
				return promise;
			}
			
			private void setState0(TemplateState state, Promise<TemplateRef> promise) {
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
							Promise<TemplateRef> promise = m_statePromise[m_pendingState.ordinal()];
							setState0(m_pendingState, promise);
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
					synchronized(m_activeNeuronsById) {
						m_activeNeuronsById.addOrFetch(ref.id(), Boolean.TRUE);
					}
					try(INeuronStateLock neuronLock = ref.lockState()) {
						neuronLock.addStateListener(NeuronState.Offline, (success) -> {
							final int neuronsLeft;
							synchronized(m_activeNeuronsById) {
								m_activeNeuronsById.remove(ref.id());
								neuronsLeft = m_activeNeuronsById.count();
							}
							if (neuronsLeft == 0) {
								try(ITemplateStateLock templateLock = lockState()) {
									if (templateLock.currentState() == TemplateState.TakeNeuronsOffline) {
										setState(TemplateState.SystemOffline);
									}
								}
							}
						});
					}
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
				public Future<TemplateRef> getStateFuture(TemplateState forState) {
					return InstanceManagement.this.getStateFuture(forState);
				}

				@Override
				public boolean isStateOneOf(TemplateState... states) {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					for(TemplateState s : states) {
						if (s == m_state) {
							return true;
						}
					}
					return false;
				}

				@Override
				public void addStateListener(TemplateState state, GenericFutureListener<? extends Future<? super TemplateRef>> listener) {
					if (!m_locked) {
						throw new IllegalStateException("unlock() was already called");
					}
					getStateFuture(state).addListener(listener);
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
			NeuronApplication.logError(LOG, "Exception thrown from template.init()", ex);
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
