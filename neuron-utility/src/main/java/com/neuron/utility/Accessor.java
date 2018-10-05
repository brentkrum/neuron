package com.neuron.utility;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;

public class Accessor<T>
{
	private final Class<T> m_clazz;
	private HashMap<String, Field> m_fields = new HashMap<>();
	private HashMap<String, Method> m_methods = new HashMap<>();
	
	public Accessor(Class<T> clazz)
	{
		m_clazz = clazz;
	}
	
	private Field field(String fieldName)
	{
		Field f = m_fields.get(fieldName);
		if (f == null)
		{
			try
			{
				f = m_clazz.getDeclaredField(fieldName);
				f.setAccessible(true);
			}
			catch(Exception ex)
			{
				throw new RuntimeException(ex);
			}
			m_fields.put(fieldName, f);
		}
		return f;
	}

	public void declareMethod(String name, Class<?>... parameterTypes)
	{
		Method m;
		try
		{
			m = m_clazz.getDeclaredMethod(name, parameterTypes);
			m.setAccessible(true);
		}
		catch(Exception ex)
		{
			throw new RuntimeException(ex);
		}
		m_methods.put(name, m);
	}
	
	private Method method(String name)
	{
		Method m = m_methods.get(name);
		if (m == null)
		{
			throw new RuntimeException("Method has not been declared with delcareMethod(\"" + name + "\"...)");
		}
		return m;
	}
	
	@SuppressWarnings("unchecked")
	public <ValueType> ValueType getStaticFieldValue(String fieldName)
	{
		try
		{
			return (ValueType)field(fieldName).get(null);
		}
		catch(Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	public <ValueType> ValueType getFieldValue(Object o, String fieldName)
	{
		try
		{
			return (ValueType)field(fieldName).get(o);
		}
		catch(Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}
	
	@SuppressWarnings("unchecked")
	public <ValueType> ValueType invokeMethod(Object o, String name, Object... args)
	{
		try
		{
			return (ValueType)method(name).invoke(o, args);
		}
		catch(Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}
	
	@SuppressWarnings("unchecked")
	public <ValueType> ValueType invokeStaticMethod(String name, Object... args)
	{
		try
		{
			return (ValueType)method(name).invoke(null, args);
		}
		catch(Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}
}
