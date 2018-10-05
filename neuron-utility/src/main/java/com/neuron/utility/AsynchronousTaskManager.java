package com.neuron.utility;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AsynchronousTaskManager {
	private final FastLinkedList<Task> m_runningTasks = new FastLinkedList<>();
	
	public AsynchronousTaskManager() {
	}
	
	/**
	 * The onXXX methods are for logging or debugging, not for any kind of
	 * application logic.
	 * 
	 * @param task
	 */
	protected void onCreate(Task task) {
	}
	
	/**
	 * The onXXX methods are for logging or debugging, not for any kind of
	 * application logic.
	 * 
	 * @param task
	 */
	protected void onCompleted(Task task) {
	}

	public IAsynchronousTask createTask(String name, ISubTaskCallback callback) {
		if (name == null) {
			throw new java.lang.IllegalArgumentException("name cannot be null");
		}
		final Task task = new Task(null, name, callback);
		m_runningTasks.add(task);
		onCreate(task);
		return task;
	}
	
	public Map<String,Object> snapshot() {
		final TreeMap<String,Object> map = new TreeMap<>();
		final List<Task> tasks = m_runningTasks.snapshotList();
		for(Task t : tasks) {
			map.put(t.getName(), t.snapshot());
		}
		return map;
	}
	
	public interface IAsynchronousTask {
		String getName();
		IAsynchronousTask startSubTask(String name, ISubTaskCallback callback);
		void completed();
	}
	public interface ISubTaskCallback {
		void onCompleted();
	}
	protected class Task extends FastLinkedList.LLNode<Task> implements IAsynchronousTask {
		private final Task m_parent;
		private final String m_name;
		private final ISubTaskCallback m_callback;
		private FastLinkedList<Task> m_activeSubTasks;
		private boolean m_completedCalled;
		private boolean m_taskCompleted;
		
		Task(Task parent, String name, ISubTaskCallback callback) {
			m_parent = parent;
			m_name = name;
			m_callback = callback;
		}
		
		private Object snapshot() {
			synchronized(this) {
				if (m_activeSubTasks == null) {
					return m_name;
				} else {
					final TreeMap<String,Object> map = new TreeMap<>();
					final List<Task> tasks = m_activeSubTasks.snapshotList();
					for(Task t : tasks) {
						if (t.getName() == null) {
							continue;
						}
						final Object o = t.snapshot();
						if (o != null) {
							map.put(t.getName(), t.snapshot());
						}
					}
					return map;
				}
			}
		}
		
		@Override
		public String getName() {
			return m_name;
		}
		
		@Override
		public Task startSubTask(String name, ISubTaskCallback callback) {
			final Task task;
			synchronized(this) {
				if (m_taskCompleted) {
					throw new IllegalStateException("completed() already called on this Task");
				}
				if (m_activeSubTasks == null) {
					m_activeSubTasks = new FastLinkedList<>();
				}
				task = new Task(this, name, callback);
				m_activeSubTasks.add(task);
			}
			onCreate(task);
			return task;
		}
		
		private void _subTaskCompleted(Task task) {
			final boolean completed;
			synchronized(this) {
				m_activeSubTasks.remove(task);
				completed = m_completedCalled && m_activeSubTasks.count()==0;
				m_taskCompleted = completed;
			}
			if (completed) {
				try {
					onCompleted(this);
					if (m_callback != null) {
						m_callback.onCompleted();
					}
				} finally {
					if (m_parent != null) {
						m_parent._subTaskCompleted(this);
					} else {
						m_runningTasks.remove(this);
					}
				}
			}
		}
		
		@Override
		public void completed() {
			final boolean completed;
			synchronized(this) {
				if (m_taskCompleted) {
					throw new IllegalStateException("completed() already called on this Task");
				}
				if (m_completedCalled) {
					throw new IllegalStateException("completed() already called on this Task");
				}
				m_completedCalled = true;
				if (m_activeSubTasks == null) {
					m_taskCompleted = true;
				} else {
					m_taskCompleted = m_activeSubTasks.count()==0;
				}
				completed = m_taskCompleted;
			}
			if (completed) {
				try {
					onCompleted(this);
					if (m_callback != null) {
						m_callback.onCompleted();
					}
				} finally {
					if (m_parent != null) {
						m_parent._subTaskCompleted(this);
					} else {
						m_runningTasks.remove(this);
					}
				}
			}
		}

		@Override
		protected Task getObject() {
			return this;
		}
	}
}
