package com.squid.core.jdbc.vendor.spark;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.util.ArrayList;

import com.squid.core.database.plugins.BaseBouquetPlugin;

public class SparkBouquetPlugin extends BaseBouquetPlugin {

	
	
	public static final String driverName = "org.apache.hive.jdbc.HiveDriver" ;

	@Override
	public void loadDriver() {
		URL[] paths = new URL[1];
		paths[0] = this.getClass().getProtectionDomain().getCodeSource().getLocation();

		// load the driver within an isolated classLoader
		URLClassLoader cl = new URLClassLoader(paths);
		ClassLoader rollback = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(cl);

		this.drivers = new ArrayList<Driver>();
		
		try {
			Driver driver = (Driver) Class.forName(driverName, true, cl).newInstance();
			drivers.add(driver) ;
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Thread.currentThread().setContextClassLoader(rollback);

	}


}
