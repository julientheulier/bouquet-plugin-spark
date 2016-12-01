package com.squid.core.jdbc.vendor.spark;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.plugins.BaseBouquetPlugin;

public class SparkBouquetPlugin extends BaseBouquetPlugin {

	private static final Logger logger = LoggerFactory.getLogger(SparkBouquetPlugin.class);

	public static final String driverName = "org.apache.hive.jdbc.HiveDriver";

	@Override
	public void loadDriver() {
		URL[] paths = new URL[1];
		paths[0] = this.getClass().getProtectionDomain().getCodeSource().getLocation();

		// load the driver within an isolated classLoader
		this.driverCL = new URLClassLoader(paths);
		ClassLoader rollback = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(driverCL);

		this.drivers = new ArrayList<Driver>();

		try {
			Driver driver = (Driver) Class.forName(driverName, true, driverCL).newInstance();
			drivers.add(driver);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			logger.error("Could not load driver " + driverName + " for Spark plugin");
			;
		}
		Thread.currentThread().setContextClassLoader(rollback);

	}

}
