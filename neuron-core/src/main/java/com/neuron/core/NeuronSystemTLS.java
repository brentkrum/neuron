package com.neuron.core;

import org.apache.logging.log4j.ThreadContext;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateManager.NeuronState;

import io.netty.util.internal.PlatformDependent;

public final class NeuronSystemTLS
{
	static void add(TemplateRef ref) {
		Object prev = NeuronThreadContext.get().pushThingStack(ref);
		if (ref != prev) {
			setLog4jThreadContextValues(ref);
		}
	}
	static void add(NeuronRef ref) {
		Object prev = NeuronThreadContext.get().pushThingStack(ref);
		if (ref != prev) {
			setLog4jThreadContextValues(ref);
		}
	}
//	private static void _add(Object ref) {
//		Object prev = NeuronThreadContext.get().pushThingStack(ref);
//		if (ref != prev) {
//			if (ref instanceof NeuronRef) {
//				setLog4jThreadContextValues((NeuronRef)ref);
//			} else {
//				setLog4jThreadContextValues((TemplateRef)ref);
//			}
//		}
//	}
	
	static void remove() {
		final NeuronThreadContext tls = NeuronThreadContext.get();
		Object prev = tls.popThingStack();
		Object cur = tls.currentThing();
		if (prev != cur) {
			if (cur instanceof NeuronRef) {
				setLog4jThreadContextValues((NeuronRef)cur);
			} else {
				setLog4jThreadContextValues((TemplateRef)cur);
			}
		}
	}
	
	static TemplateRef currentTemplate() {
		final Object cur = NeuronThreadContext.get().currentThing();
		if (cur instanceof NeuronRef) {
			return ((NeuronRef)cur).templateRef();
		} else {
			return (TemplateRef)cur;
		}
	}
	
	public static NeuronRef currentNeuron() {
		final Object cur = NeuronThreadContext.get().currentThing();
		if (cur instanceof NeuronRef) {
			return (NeuronRef)cur;
		} else {
			return null;
		}
	}
	
	static boolean isNeuronCurrent() {
		final Object cur = NeuronThreadContext.get().currentThing();
		if (cur instanceof NeuronRef) {
			return true;
		} else {
			return false;
		}
	}
	
	public static void validateInNeuronConnectResources(INeuronStateLock lock) {
		if (lock.currentState() != NeuronState.Connect) {
			PlatformDependent.throwException( new IllegalStateException("The neuron must be locked the Connect state when calling this method") );
		}
	}
	
	public static void validateContextAwareThread() {
		if (NeuronThreadContext.get().currentThing() == null) {
			PlatformDependent.throwException( new IllegalStateException("This method is being called from a thread that is not associated with a current neuron or template") );
		}
	}
	
	public static void validateNeuronAwareThread() {
		if (NeuronThreadContext.get().currentThing() instanceof NeuronRef) {
			return;
		}
		PlatformDependent.throwException( new IllegalStateException("This method is being called from a thread that is not associated with a current neuron") );
	}
	
	public static void validateTemplateAwareThread() {
		if (NeuronThreadContext.get().currentThing() instanceof TemplateRef) {
			return;
		}
		PlatformDependent.throwException( new IllegalStateException("This method is being called from a thread that is not associated with a current template") );
	}
	
	
	public static Runnable wrapForCurrentNeuron(Runnable runnable, NeuronState... runnableStates) {
		return new Runnable() {
			private final NeuronRef m_cur = NeuronSystemTLS.currentNeuron();
			private final NeuronState[] m_runnableStates = runnableStates;
			
			@Override
			public void run()
			{
				INeuronStateLock lock = m_cur.lockState();
				try {
					if (lock.isStateOneOf(m_runnableStates)) {
						runnable.run();
					}
				} finally {
					lock.unlock();
				}
			}
			
		};
	}
	
	
	private static void setLog4jThreadContextValues(TemplateRef templateRef) {
		if (templateRef == null) {
			ThreadContext.remove("nT");
		} else {
			ThreadContext.put("nT", templateRef.logString());
		}
		ThreadContext.remove("nI");
	}	
	private static void setLog4jThreadContextValues(NeuronRef neuronRef) {
		if (neuronRef == null) {
			ThreadContext.remove("nT");
			ThreadContext.remove("nI");
		} else {
			ThreadContext.put("nT", neuronRef.templateRef().logString());
			ThreadContext.put("nI", neuronRef.logString());
		}
	}
}
