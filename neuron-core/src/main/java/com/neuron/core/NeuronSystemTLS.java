package com.neuron.core;

import org.apache.logging.log4j.ThreadContext;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;

import io.netty.util.internal.PlatformDependent;

public final class NeuronSystemTLS
{
	enum ThingType { None, Group, Template, Neuron};
	
	static void add(GroupRef ref) {
		Object prev = NeuronThreadContext.get().pushThingStack(ref);
		if (ref != prev) {
			setLog4jThreadContextValues(ref);
		}
	}
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
	
	static void remove() {
		final NeuronThreadContext tls = NeuronThreadContext.get();
		Object prev = tls.popThingStack();
		Object cur = tls.currentThing();
		if (prev != cur) {
			if (cur instanceof NeuronRef) {
				setLog4jThreadContextValues((NeuronRef)cur);
			} else if (cur instanceof TemplateRef) {
					setLog4jThreadContextValues((TemplateRef)cur);
			} else {
				setLog4jThreadContextValues((GroupRef)cur);
			}
		}
	}

	static GroupRef currentGroup() {
		final Object cur = NeuronThreadContext.get().currentThing();
		if (cur instanceof NeuronRef) {
			return ((NeuronRef)cur).templateRef().groupRef();
		} else if (cur instanceof TemplateRef) {
			return ((TemplateRef)cur).groupRef();
		} else {
			return (GroupRef)cur;
		}
	}
	
	static TemplateRef currentTemplate() {
		final Object cur = NeuronThreadContext.get().currentThing();
		if (cur instanceof NeuronRef) {
			return ((NeuronRef)cur).templateRef();
		} else if (cur instanceof TemplateRef) {
			return (TemplateRef)cur;
		} else {
			return null;
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
	
	static ThingType whatIsCurrent() {
		final Object cur = NeuronThreadContext.get().currentThing();
		if (cur == null) {
			return ThingType.None;
		} else if (cur instanceof NeuronRef) {
			return ThingType.Neuron;
		} else if (cur instanceof TemplateRef) {
			return ThingType.Template;
		} else {
			return ThingType.Group;
		}
	}
	
	static boolean isTemplateCurrent() {
		final Object cur = NeuronThreadContext.get().currentThing();
		if (cur instanceof TemplateRef) {
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
	
	
	private static void setLog4jThreadContextValues(GroupRef groupRef) {
		if (groupRef == null) {
			ThreadContext.remove("nG");
		} else {
			ThreadContext.put("nG", groupRef.logString());
		}
		ThreadContext.remove("nT");
		ThreadContext.remove("nI");
	}	
	
	private static void setLog4jThreadContextValues(TemplateRef templateRef) {
		if (templateRef == null) {
			ThreadContext.remove("nG");
			ThreadContext.remove("nT");
		} else {
			ThreadContext.put("nG", templateRef.groupRef().logString());
			ThreadContext.put("nT", templateRef.logString());
		}
		ThreadContext.remove("nI");
	}	
	private static void setLog4jThreadContextValues(NeuronRef neuronRef) {
		if (neuronRef == null) {
			ThreadContext.remove("nG");
			ThreadContext.remove("nT");
			ThreadContext.remove("nI");
		} else {
			ThreadContext.put("nG", neuronRef.templateRef().groupRef().logString());
			ThreadContext.put("nT", neuronRef.templateRef().logString());
			ThreadContext.put("nI", neuronRef.logString());
		}
	}
}
