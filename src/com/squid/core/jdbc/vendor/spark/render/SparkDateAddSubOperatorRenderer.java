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
package com.squid.core.jdbc.vendor.spark.render;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.sql.db.render.DateAddSubOperatorRenderer;

/**
 * The SQL render for substracting/adding to a date another date or a constant
 * @author julien theulier
 *
 */
public class SparkDateAddSubOperatorRenderer
extends DateAddSubOperatorRenderer
{
	static final Logger logger = LoggerFactory.getLogger(SparkDateAddSubOperatorRenderer.class);

	public SparkDateAddSubOperatorRenderer(OperatorType builtinType) {
		super(builtinType);
	}

	@Override
	protected String getDate(String date) {
		if(logger.isDebugEnabled()){logger.debug(("TO_DAYS("+date+")"));}
		return "TO_DAYS("+date+")";
	}

}