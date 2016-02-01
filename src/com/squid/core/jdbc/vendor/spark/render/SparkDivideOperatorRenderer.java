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

import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.sql.db.render.BaseOperatorRenderer;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SQLSkin;

/*
 * TODO Check this claim on Drill
 * On MYSQL, we do not need to cast the numerator into a floating type when dividing a integer
 * @author sfantino
 *
 */
public class SparkDivideOperatorRenderer 
extends BaseOperatorRenderer
{

	static final Logger logger = LoggerFactory.getLogger(SparkDivideOperatorRenderer.class);
	
	@Override
	public String prettyPrint(SQLSkin skin, OperatorDefinition opDef,
			String[] args) throws RenderingException {
		if (args.length==2) {
			// does NOT handle bug T#627
			//return "(CASE WHEN "+args[1]+"=0 THEN NULL ELSE "+args[0]+"/"+args[1]+" END)";
			//String arg1 = "(NULLIF("+args[1]+",0))";
			String arg0 = args[0];
			String arg1 = args[1];
			String safeOp = arg0+opDef.getSymbol()+arg1;
			if(logger.isDebugEnabled()){logger.debug((safeOp));}
			return safeOp;
		} else {
			return opDef.prettyPrint(args, true);
		}
	}

}
