package com.neuron.core;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.utility.StackTraceUtil;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.PlatformDependent;

class NeuronThreadContext {
	private static final Logger LOG = LogManager.getLogger(NeuronThreadContext.class);
	private static final int MAX_THING_STACK_SIZE = Config.getFWInt("core.NeuronThreadContext.maxThingStackSize", 32);
	private static final boolean LEAK_DETECTION = Config.getFWBoolean("core.NeuronThreadContext.leakDetection", false);
	private static final ArrayList<NeuronThreadContext> m_tsaPool = new ArrayList<>(32);
	private static final TSAThreadLocal m_currentTSA = new TSAThreadLocal();

	private Object[] m_currentThingStack = new Object[8];
	private LeakDetectionContext[] m_currentLeakDetStack = new LeakDetectionContext[m_currentThingStack.length];
	private int m_thingStackTop = 0;
	private Object m_currentThing;
	
	/*
	 * No need to synchronize, this is a per-thread data structure
	 */
	public Object pushThingStack(Object newCurrentThing) {
		if (LEAK_DETECTION) {
			m_currentLeakDetStack[m_thingStackTop] = new LeakDetectionContext();
		}
		m_currentThingStack[m_thingStackTop++] = m_currentThing;
		if (m_thingStackTop == m_currentThingStack.length) {
			if (m_thingStackTop >= MAX_THING_STACK_SIZE) {
				final RuntimeException ex = new RuntimeException("Token stack overflow.  This thread has gone " + MAX_THING_STACK_SIZE + " nested calls deep into neuron methods.  Something is wrong with this.  If this is ok, set com.neuron.NeuronSystem.maxTokenStackSize to a value to allow it.");
				LOG.fatal("", ex);
				if (LEAK_DETECTION) {
					final StringBuilder sb = new StringBuilder();
					sb.append("Current push locations\n");
					for(int i=0; i<m_currentLeakDetStack.length; i++ ) {
						sb.append(m_currentLeakDetStack[i].m_lockingThread.getName()).append("\n");
						StackTraceUtil.stackTraceFormat(sb, m_currentLeakDetStack[i].m_lockStackTrace);
					}
					LOG.fatal(sb);
				}
				PlatformDependent.throwException(ex);
			}
			m_currentThingStack = Arrays.copyOf(m_currentThingStack, m_currentThingStack.length * 2);
			if (LEAK_DETECTION) {
				m_currentLeakDetStack = Arrays.copyOf(m_currentLeakDetStack, m_currentLeakDetStack.length * 2);
			}
		}
		final Object prev = m_currentThing;
		m_currentThing = newCurrentThing;
		return prev;
	}
	
	/*
	 * No need to synchronize, this is a per-thread data structure
	 */
	public Object popThingStack() {
		if (m_thingStackTop != 0) {
			final Object prev = m_currentThing;
			m_currentThing = m_currentThingStack[--m_thingStackTop];
			m_currentThingStack[m_thingStackTop] = null;
			if (LEAK_DETECTION) {
				m_currentLeakDetStack[m_thingStackTop] = null;
			}
			return prev;
		}
		PlatformDependent.throwException(new RuntimeException("Token stack underflow.  This thread encountered a popTokenStack that was not matched with a pushTokenStack."));
		return null; // To make compiler happy
	}

	public Object currentThing() {
		return m_currentThing;
	}
	
	public static NeuronThreadContext get() {
		return m_currentTSA.get();
	}
	
	/*
	 * This is only called by the thread local
	 */
	private static NeuronThreadContext aquire() {
		synchronized(m_tsaPool) {
			final int size = m_tsaPool.size();
			if (size > 0) {
				return m_tsaPool.remove(size-1);
			}
		}
		return new NeuronThreadContext();
	}
	
	/*
	 * This is only called by the thread local
	 */
	private void release() {
		// TODO verify and report on a stack that wasn't cleared
		m_thingStackTop = 0; // Just in case
		synchronized(m_tsaPool) {
			m_tsaPool.add(this);
		}
	}
	
	private static class TSAThreadLocal extends FastThreadLocal<NeuronThreadContext> {

		@Override
		protected NeuronThreadContext initialValue() throws Exception
		{
			return NeuronThreadContext.aquire();
		}

		@Override
		protected void onRemoval(NeuronThreadContext value) throws Exception
		{
			value.release();
		}
		
	}
	
	private static class LeakDetectionContext {
		private final Thread m_lockingThread;
		private final StackTraceElement[] m_lockStackTrace;
		
		LeakDetectionContext() {
			m_lockingThread = Thread.currentThread();
			m_lockStackTrace = m_lockingThread.getStackTrace();
		}
	}
}
