package com.neuron.core;

public class CallbackSystem
{
//	private static Object m_lock = new Object();
//	private static IntTrie<TemplateCallbackSet> m_templateCallbacks = new IntTrie<>();
//	private static AtomicInteger m_nextId = new AtomicInteger();
//	
//	static void register() {
//		NeuronApplication.register(new Registrant());
//	}
//	
//	public static Runnable createNeuronCallback(Runnable callback) {
//		final InstanceCallbackSet.Wrapper wrapper = createNeuronCallback0(NeuronSystem.getCurrentTemplateId(), NeuronSystem.getCurrentInstanceId(), callback);
//		final ITLSStorage tlsCapture = NeuronApplication.captureTLSForMigration();
//		return new Runnable() {
//			private final InstanceCallbackSet.Wrapper m_wrapper = wrapper;
//			
//			@Override
//			public void run()
//			{
//				final Runnable callback = (Runnable)m_wrapper.getCallback();
//				if (callback == null) {
//					return;
//				}
//				NeuronApplication.restoreTLSAfterMigration(tlsCapture);
//				try {
//					callback.run();
//				} finally {
//					NeuronApplication.clearTLS();
//				}
//			}
//		};
//	}
//	
//	public static Callable<Boolean> createNeuronCallbackOK() {
//		NeuronSystem.validateNeuronAwareThread();
//		final ITLSStorage tlsCapture = NeuronApplication.captureTLSForMigration();
//		return new Callable<Boolean>() {
//			@Override
//			public Boolean call() throws Exception
//			{
//				NeuronApplication.restoreTLSAfterMigration(tlsCapture);
//				try {
//					return NeuronSystem.isCurrentNeuronKnown();
//				} finally {
//					NeuronApplication.clearTLS();
//				}
//			}
//		};
//	}
//
//	private static InstanceCallbackSet.Wrapper createNeuronCallback0(int templateId, int neuronInstanceId, Object callback) {
//		synchronized(m_lock) {
//			TemplateCallbackSet set = m_templateCallbacks.get(templateId);
//			if (set == null) {
//				set = new TemplateCallbackSet();
//				m_templateCallbacks.addOrFetch(templateId, set);
//			}
//			return set.addCallback(neuronInstanceId, callback);
//		}
//	}
//	
//	private static void removeTemplateInstance(int templateId, int neuronInstanceId) {
//		synchronized(m_lock) {
//			TemplateCallbackSet set = m_templateCallbacks.get(templateId);
//			if (set == null) {
//				return;
//			}
//			set.removeInstance(neuronInstanceId);
//		}
//	}
//	
//	private static class TemplateCallbackSet {
//		private final IntTrie<InstanceCallbackSet> m_instanceCallbacks = new IntTrie<>();
//		
//		public InstanceCallbackSet.Wrapper addCallback(int neuronInstanceId, Object callback) {
//			synchronized(this) {
//				InstanceCallbackSet set = m_instanceCallbacks.get(neuronInstanceId);
//				if (set == null) {
//					set = new InstanceCallbackSet();
//					m_instanceCallbacks.addOrFetch(neuronInstanceId, set);
//				}
//				return set.addCallback(callback);
//			}
//		}
//		
//		public void removeInstance(int neuronInstanceId) {
//			final InstanceCallbackSet set = m_instanceCallbacks.remove(neuronInstanceId);
//			if (set == null) {
//				return;
//			}
//			set.setAsInvalid();
//		}
//	}
//	
//	private static class InstanceCallbackSet {
//		private final IntTrie<Wrapper> m_instanceCallbacks = new IntTrie<>();
//		private boolean m_invalid;
//		
//		Wrapper addCallback(Object callback) {
//			final int key = m_nextId.incrementAndGet();
//			final Wrapper wrapper = new Wrapper(key, callback);
//			m_instanceCallbacks.addOrFetch(key, wrapper);
//			return wrapper;
//		}
//		
//		synchronized void setAsInvalid() {
//			m_invalid = true;
//			// We remove the reference to the original callback so the garbage collector
//			// can remove whatever we are pointing at... even if this callback doesn't come
//			// back for a long time
//			m_instanceCallbacks.forEach((wrapper) -> {
//				wrapper.m_originalCallback = null;
//				return true;
//			});
//		}
//		
//		private class Wrapper {
//			private final int m_key;
//			private Object m_originalCallback;
//			
//			Wrapper(int key, Object callback) {
//				m_key = key;
//				m_originalCallback = callback;
//			}
//			
//			Object getCallback() {
//				synchronized(InstanceCallbackSet.this) {
//					if (m_invalid) {
//						return null;
//					}
//					m_instanceCallbacks.remove(m_key);
//					return m_originalCallback;
//				}
//			}
//		}
//	}
//	
//	
//	private static class Registrant implements INeuronApplicationSystem {
//
//		@Override
//		public String systemName()
//		{
//			return "CallbackSystem";
//		}
//
//		@Override
//		public void neuronTemplateUnloaded(int templateId)
//		{
//		}
//
//		@Override
//		public void neuronUnloaded(int templateId, int neuronInstanceId)
//		{
//			removeTemplateInstance(templateId, neuronInstanceId);
//		}
//		
//	}
//
}
