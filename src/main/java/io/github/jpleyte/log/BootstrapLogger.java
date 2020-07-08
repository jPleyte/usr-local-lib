package io.github.jpleyte.log;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Creates a configured logger
 * 
 * @author j
 *
 */
public final class BootstrapLogger {

	/**
	 * Returns a logger configured from internal logging configuration, unless the user defined property
	 * "java.util.logging.config.file" has been set.
	 * 
	 * @param name
	 * @return
	 */
	public static final Logger configureLogger(String name) {
		if (System.getProperty("java.util.logging.config.file") == null) {
			InputStream stream = BootstrapLogger.class.getClassLoader().getResourceAsStream("logging.properties");
			try {
				LogManager.getLogManager().readConfiguration(stream);
			} catch (SecurityException | IOException e) {
				System.err.println("Unable to read logging.properties");
				System.exit(1);
			}
		}

		return Logger.getLogger(name);
	}

	/**
	 * Return the log level of the logger
	 * 
	 * @param logger
	 * @return
	 */
	public static final Level getLogLevel(Logger logger) {
		return logger.getLevel() == null ? logger.getParent().getLevel() : logger.getLevel();
	}
}
