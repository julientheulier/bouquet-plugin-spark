/*******************************************************************************
 * Copyright © Squid Solutions, 2016
 *
 * This file is part of Open Bouquet software.
 *  
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation (version 3 of the License).
 *
 * There is a special FOSS exception to the terms and conditions of the 
 * licenses as they are applied to this program. See LICENSE.txt in
 * the directory of this program distribution.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * Squid Solutions also offers commercial licenses with additional warranties,
 * professional functionalities or services. If you purchase a commercial
 * license, then it supersedes and replaces any other agreement between
 * you and Squid Solutions (above licenses and LICENSE.txt included).
 * See http://www.squidsolutions.com/EnterpriseBouquet/
 *******************************************************************************/
package com.squid.core.jdbc.vendor.spark;

import java.io.IOException;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.metadata.IMetadataEngine;
import com.squid.core.database.metadata.VendorMetadataSupport;
import com.squid.core.database.model.DatabaseProduct;
import com.squid.core.jdbc.formatter.DataFormatter;
import com.squid.core.jdbc.formatter.IJDBCDataFormatter;
import com.squid.core.jdbc.vendor.DefaultVendorSupport;
import com.squid.core.jdbc.vendor.JdbcUrlParameter;
import com.squid.core.jdbc.vendor.JdbcUrlTemplate;

public class SparkVendorSupport extends DefaultVendorSupport {
	
	public static final String VENDOR_ID =  IMetadataEngine.SPARK_NAME;
	
	public static final String HIVE_VERSION = "1.1.0";
	
    static final Logger logger = LoggerFactory.getLogger(SparkVendorSupport.class);
	static final VendorMetadataSupport SPARK = new SparkMetadataSupport();
	private Properties properties;

	@Override
	public String getVendorId() {
		return VENDOR_ID+" ("+HIVE_VERSION+")";
	}

	@Override
	public String getVendorVersion() {
		try {
			this.properties = new Properties();
			properties.load(this.getClass().getClassLoader().getResourceAsStream("application.properties"));
			return properties.getProperty("application.version");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "-1";
	}

	@Override
	public boolean isSupported(DatabaseProduct product) {
		return VENDOR_ID.equals(product.getProductName()) && isVersionSupported(product);
	}
	
	/**
	 * only supporting 1.1.x versions
	 * @param product
	 * @return
	 */
	public boolean isVersionSupported(DatabaseProduct product) {
		return product.getMajorVersion()==1 && product.getMinorVersion()==1;
	}

	@Override
	public IJDBCDataFormatter createFormatter(DataFormatter formatter,
			Connection connection) {
		return new SparkJDBCDataFormatter(formatter, connection);
	}

	@Override
	public VendorMetadataSupport getVendorMetadataSupport() {
		return SPARK;
	}
	
	/* (non-Javadoc)
	 * @see com.squid.core.jdbc.vendor.DefaultVendorSupport#getJdbcUrlTemplate()
	 */
	@Override
	public JdbcUrlTemplate getJdbcUrlTemplate() {
		JdbcUrlTemplate template = new JdbcUrlTemplate("Hive/SparkSQL","jdbc:hive2://[<hostname>][:<port=10000>]");
		template.add(new JdbcUrlParameter("hostname", false));
		template.add(new JdbcUrlParameter("port", true, "10000"));
		return template;
	}
	
	/* (non-Javadoc)
	 * @see com.squid.core.jdbc.vendor.DefaultVendorSupport#buildJdbcUrl(java.util.Map)
	 */
	@Override
	public String buildJdbcUrl(Map<String, String> arguments) throws IllegalArgumentException {
		String url = "jdbc:hive2://";
		String hostname = arguments.get("hostname");
		if (hostname==null) throw new IllegalArgumentException("cannot build JDBC url, missing mandatory argument [hostname]");
		url += hostname;
		String port = arguments.get("port");
		if (port!=null) {
			// check it's an integer
			try {
				int p = Integer.valueOf(port);
				url += ":"+Math.abs(p);// just in case
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("cannot build JDBC url, [port] value must be a valid port number");
			}
		}
		// validate ?
		return url;
	}
	
}
