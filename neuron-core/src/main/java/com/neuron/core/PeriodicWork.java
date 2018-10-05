package com.neuron.core;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.neuron.core.NeuronRef.INeuronStateLock;

import io.netty.util.concurrent.ScheduledFuture;

public abstract class PeriodicWork implements Runnable {
	private final long m_intervalMS;
	private final Semaphore m_taskRun = new Semaphore(1);
	private final NeuronRef m_neuronRef;
	private ScheduledFuture<?> m_future;
	private volatile boolean m_stoppedTask;
	private volatile boolean m_shutdown;

	public PeriodicWork(NeuronRef neuronRef, long intervalMS) {
		m_neuronRef = neuronRef;
		m_intervalMS = intervalMS;
	}
	
	public PeriodicWork(long intervalMS) {
		m_neuronRef = NeuronSystemTLS.currentNeuron();
		m_intervalMS = intervalMS;
	}

	public void start() {
		m_future = NeuronApplication.getTaskPool().scheduleAtFixedRate(this, m_intervalMS, m_intervalMS, TimeUnit.MILLISECONDS);
	}

	public void shutdown() {
		// Attempt to keep it from being scheduled to run
		synchronized(this) {
			if (m_shutdown) {
				return;
			}
			if (m_future != null) {
				m_future.cancel(false);
				m_future = null;
			}
			m_shutdown = true;
		}
		// Acquiring this will prevent the persist task from running if it was scheduled but
		// the thread has not started yet.
		try {
			m_stoppedTask = m_taskRun.tryAcquire(0, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}
	}
	
	public void awaitTermination(long waitTimeoutInMS) {
		if (!m_stoppedTask) {
			// Waiting on and acquiring this will allow a running persist task to finish and
			// prevent it from starting
			try {
				m_taskRun.tryAcquire(waitTimeoutInMS, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
			}
		}
	}
	
	protected abstract void _run();
	
	@Override
	public void run() {
		synchronized(this) {
			if (m_shutdown) {
				return;
			}
			try {
				if (!m_taskRun.tryAcquire(0, TimeUnit.MILLISECONDS)) {
					return;
				}
			} catch (InterruptedException e) {
				return;
			}
		}
		try(INeuronStateLock lock = m_neuronRef.lockState()) {
			_run();
		} catch(Exception ex) {
			unhandledException(ex);
		}
		m_taskRun.release();
	}
	
	protected void unhandledException(Exception ex) {
		ex.printStackTrace();
	}
	
}
