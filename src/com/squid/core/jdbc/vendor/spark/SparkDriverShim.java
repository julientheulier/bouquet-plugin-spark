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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import com.squid.core.database.impl.ConnectionShim;
import com.squid.core.database.impl.DefaultDriverShim;

public class SparkDriverShim extends DefaultDriverShim {

	public SparkDriverShim(Driver d) {
		super(d);
	}

	@Override
	public Connection connect(String u, Properties p) throws SQLException {
		// make sure to use the driverLoader ctx
		ClassLoader loader = this.driver.getClass().getClassLoader();
		ClassLoader rollback = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(loader);
		try {
			return new ConnectionShim(this.driver.connect(u, p));
		} finally {
			Thread.currentThread().setContextClassLoader(rollback);
		}
	}

}
