package com.neuron.core.log4j;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.Config;
import com.neuron.core.Config.IConfigLogger;

public class Log4jConsoleLogger implements IConfigLogger
{
	private static final Logger LOG = LogManager.getLogger(Config.class);
	
	@Override
	public void info(String format, Object... args)
	{
		LOG.info(String.format(format, args));
	}
	
	@Override
	public void warn(String format, Object... args)
	{
		LOG.warn(String.format(format, args));
	}
	
	@Override
	public void fatal(String format, Object... args)
	{
		LOG.fatal(String.format(format, args));
	}
}
