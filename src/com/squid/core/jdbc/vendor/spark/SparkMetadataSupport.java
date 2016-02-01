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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.metadata.ColumnData;
import com.squid.core.database.metadata.MetadataConst;
import com.squid.core.database.metadata.VendorMetadataSupport;
import com.squid.core.database.model.DatabaseFactory;
import com.squid.core.database.model.Schema;
import com.squid.core.database.model.Table;
import com.squid.core.database.model.TableType;

/**
 * Spark version
 * @author louisrabiet
 *
 */
public class SparkMetadataSupport implements VendorMetadataSupport {

    private Hashtable<String, String> m_definitions;
    
    private final String[] COLUMNS_CNAMES = new String[]{
			 getColumnDef(MetadataConst.COLUMN_NAME),  //0
			 getColumnDef(MetadataConst.TYPE_NAME),    //1
			 getColumnDef(MetadataConst.COLUMN_SIZE),  //2
			 getColumnDef(MetadataConst.IS_NULLABLE),  //3
			 getColumnDef(MetadataConst.COLUMN_DEF),   //4
			 getColumnDef(MetadataConst.DATA_TYPE),    //5
			 getColumnDef(MetadataConst.TABLE_NAME),    //6
			 getColumnDef(MetadataConst.DECIMAL_DIGITS), // 7
			 getColumnDef(MetadataConst.REMARKS) // 8
	 };

	 private int[] COLUMNS_CPOS = null;

	 private int[] computeColumnPos(String[] columns, ResultSet result) throws SQLException {
			ResultSetMetaData meta = result.getMetaData();
			List<String> lookup = Arrays.asList(columns);
			int[] indexes = new int[columns.length];
			Arrays.fill(indexes,-1);
			for (int i=1;i<=meta.getColumnCount();i++) {
				String cname = meta.getColumnLabel(i);
				int index = lookup.indexOf(cname.toUpperCase());
				if (index>=0) {
					indexes[index] = i;
				}
			}
			return indexes;
		}
	 
    private void loadColumnData(ResultSet res, ColumnData data) throws SQLException {
		 if (COLUMNS_CPOS==null) {
			 COLUMNS_CPOS = computeColumnPos(COLUMNS_CNAMES,res);
		 }
		 //
		 data.table_name = res.getString(COLUMNS_CPOS[6]);           // TABLE_NAME
		 data.column_name = res.getString(COLUMNS_CPOS[0]).trim();   // COLUMN_NAME
		 data.type_name = res.getString(COLUMNS_CPOS[1]);            // TYPE_NAME
		 data.column_size = res.getInt(COLUMNS_CPOS[2]);             // COLUMN_SIZE
		 data.decimal_digits = res.getInt(COLUMNS_CPOS[7]);			// DECIMAL_DIGITS
		 //
		 // Oracle: issue: jdbc driver throwing exception when getting the DEfault_value column value after
		 data.column_def = res.getString(COLUMNS_CPOS[4]);           // COLUMN_DEF
		 data.is_nullable = res.getString(COLUMNS_CPOS[3]);          // IS_NULLABLE
		 //
		 data.data_type = res.getInt(COLUMNS_CPOS[5]);               // DATA_TYPE
		 //
		 data.remarks = res.getString(COLUMNS_CPOS[8]); // remarks
	 }
    
    /**
     * 
     *  @param catalog a catalog name; must match the catalog name as it is stored in the database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
	 *	@param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
	 *	@param tableNamePattern a table name pattern; must match the table name as it is stored in the database
	 *	@param columnNamePattern a column name pattern; must match the column name as it is stored in the database
     */
    public List<ColumnData> getColumns(Connection conn, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
         //conn.getMetaData().getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    	List<ColumnData> datas = new ArrayList<ColumnData>();
    	Statement statement = conn.createStatement();
    	if(tableNamePattern != null){
    		ResultSet columnSet = statement.executeQuery("describe "+tableNamePattern);
    		/**
    		 * 0: jdbc:hive2://192.168.200.93:10000> describe people;
    		 * +-----------+------------+----------+
    		 * | col_name  | data_type  | comment  |
    		 * +-----------+------------+----------+
			 * | name      | string     |          |
			 * | age       | string     |          |
			 * +-----------+------------+----------+
    		 */
    		while(columnSet.next()){ //  no pattern.
				String col_name = columnSet.getString("col_name");
				String data_type_name = columnSet.getString("data_type");
				ColumnData data = new ColumnData();
				data.table_name = tableNamePattern;           // TABLE_NAME
				 data.column_name = col_name;   // COLUMN_NAME
				 data.type_name = data_type_name;            // TYPE_NAME
				 data.column_size = SparkDataTypeMapping.getColumnSizefromName(data_type_name);             // COLUMN_SIZE
				 //data.decimal_digits = res.getInt(COLUMNS_CPOS[7]);			// DECIMAL_DIGITS
				 //
				 // Oracle: issue: jdbc driver throwing exception when getting the DEfault_value column value after
				 //data.column_def = res.getString(COLUMNS_CPOS[4]);           // COLUMN_DEF
				 data.is_nullable = SparkDataTypeMapping.getIsNullablefromName(data_type_name);          // IS_NULLABLE
				 //
				 data.data_type = SparkDataTypeMapping.getSqlTypefromName(data_type_name);
				 //data.data_type = data_type;               // DATA_TYPE
				 //
				 //data.remarks = res.getString(COLUMNS_CPOS[8]); // remarks
				 datas.add(normalizeColumnData(data));
				
			}
    	}else{
			ResultSet tableSet = statement.executeQuery("show tables");
			while(tableSet.next()){ //  no pattern.
				String tablename = tableSet.getString("tableName");
				ResultSet columnSet = statement.executeQuery("describe "+tablename);
	    		while(columnSet.next()){ //  no pattern.
	    			String col_name = columnSet.getString("col_name");
					String data_type_name = columnSet.getString("data_type");
					ColumnData data = new ColumnData();
					data.table_name = tablename;           // TABLE_NAME
					 data.column_name = col_name;   // COLUMN_NAME
					 data.type_name = data_type_name;            // TYPE_NAME
					 data.column_size = SparkDataTypeMapping.getColumnSizefromName(data_type_name);             // COLUMN_SIZE
					 //data.decimal_digits = res.getInt(COLUMNS_CPOS[7]);			// DECIMAL_DIGITS
					 //
					 // Oracle: issue: jdbc driver throwing exception when getting the DEfault_value column value after
					 //data.column_def = res.getString(COLUMNS_CPOS[4]);           // COLUMN_DEF
					 data.is_nullable = SparkDataTypeMapping.getIsNullablefromName(data_type_name);          // IS_NULLABLE
					 //
					 data.data_type = SparkDataTypeMapping.getSqlTypefromName(data_type_name);
					 //
					 //data.remarks = res.getString(COLUMNS_CPOS[8]); // remarks
					 datas.add(normalizeColumnData(data));
	    		}
			}
    	}
		return datas;
         
    }

    public ResultSet getIndexInfo(Connection conn, String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return conn.getMetaData().getIndexInfo(catalog, schema, table, unique, approximate);
    }

    public ResultSet getPrimaryKeys(Connection conn, String catalog, String schema, String table) throws SQLException {
        return conn.getMetaData().getPrimaryKeys(catalog, schema, table);
    }

    
    public ColumnData normalizeColumnData(ColumnData data) {
        // default do nothing
    	return data;
    }
    
    /**
     * check if the database support UTF-8 surrogate Characters - the default seems to be false anyway ?
     * @return
     */
    public boolean handleSurrogateCharacters() {
        return false;
    }

    
	public  int[] normalizeColumnType(java.sql.ResultSet rs) throws SQLException{
		ResultSetMetaData metadata = rs.getMetaData();
		int columnCount =  metadata.getColumnCount();
		int[] columnTypes = new int[columnCount];
		for (int i=0;i<columnCount;i++) {
			columnTypes[i] = metadata.getColumnType(i+1);
		}
		return columnTypes;
	}

	
	public List<Schema> getSchemas(DatabaseFactory df, Connection conn) throws DatabaseServiceException {
		List<Schema> result = new ArrayList<Schema>();
		DatabaseMetaData metadata;
		try {
			Statement statement = conn.createStatement();
			ResultSet dbSet = statement.executeQuery("show databases");
			/*
			 * 0: jdbc:hive2://...:10000> show databases;
			 * +----------+
             * |  result  |
			 * +----------+
			 * | default  |
			 * +----------+
			 */
			while(dbSet.next()){
				String schemaName = dbSet.getString(1);
				Schema schema = df.createSchema();
				schema.setName(schemaName);
				schema.setSystem(isSystemSchema(schemaName));
				result.add(schema); 
			}
			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new DatabaseServiceException("Not able to get schemas");
		}
	}
	
	@Override
	public boolean isSystemSchema(String name) {
		return false;
	}

	
	public List<Schema> getCatalogs(DatabaseFactory df, Connection conn) throws DatabaseServiceException {
		// TODO Auto-generated method stub
		throw new DatabaseServiceException("getCatalogs not implemented in Hive Spark");
}

	public void init() {
		 if (m_definitions==null) {
			 m_definitions = new Hashtable<String, String>();
			 for (String def : MetadataConst.definitions) {
				 m_definitions.put(def,def);
			 }
		 }
	 }
	
	 protected String getColumnDef(String def) {
		 if (m_definitions==null) {
			 init();
		 }
		 final String result = m_definitions.get(def);
		 return result!=null?result:def;
	 }
	 
	
	public List<Table> getTables(DatabaseFactory df, Connection conn, String catalog, String schemaPattern, String tableNamePattern)
			throws DatabaseServiceException {
		List<Table> result = new ArrayList<Table>(); 
		try {
			//DatabaseMetaData metadata = conn.getMetaData();
			//ResultSet res = metadata.getTables(catalog, schemaPattern , tableNamePattern,null);
			// parameters for metadata.getTables
			/* 
			 * catalog a catalog name; must match the catalog name as it is stored in the database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
			 * schemaPattern a schema name pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
			 * tableNamePattern a table name pattern; must match the table name as it is stored in the database
			 * types a list of table types, which must be from the list of table types returned from getTableTypes,to include; null returns all types
			 */
			try {
					conn.setSchema(schemaPattern);
			} catch (SQLException e) {
				throw new DatabaseServiceException("Spark does not support schemaPattern to populateSchemas, only exact name");
			}
			Statement statement = conn.createStatement();
			ResultSet tableSet = statement.executeQuery("show tables");
			// On hive jdbc return
			/* 0: jdbc:hive2://...> show tables;
				+------------+--------------+
				| tableName  | isTemporary  |
				+------------+--------------+
				| example    | false        |
				| people     | false        |
				| persons    | false        |
				| src        | false        |
				+------------+--------------+
			*/
			while(tableSet.next()){ //  no pattern.
				String tablename = tableSet.getString("tableName");
				if(tablename!=null && tableNamePattern != null){
					if(tablename.contains(tableNamePattern)){
						String xcatalog = null;
						String remarks = "isTemporary: "+tableSet.getString("isTemporary");
						//only table, no view with Hive.
						String type = "T";
						TableType tableType = null;
						 if (type.compareTo("T")==0||getColumnDef(MetadataConst.TABLE_TYPE_TABLE).compareTo(type)==0) {
						     tableType = TableType.Table;
						 } else if (type.compareTo("V")==0||getColumnDef(MetadataConst.TABLE_TYPE_VIEW).compareTo(type)==0) {
							tableType = TableType.View;
						 } else {
							 // it's a procedure, skip it...
						 }
						 if (tableType!=null) {// skip if not set
							 Table table = df.createTable();
							 table.setType(tableType);
							 table.setName(tablename);
							 if (xcatalog!=null) table.setCatalog(xcatalog);
							 if (remarks!=null) table.setDescription(remarks);
							 result.add(table);
						 }
					}
				}else{
					String xcatalog = null;
					String remarks = "isTemporary: "+tableSet.getString("isTemporary");
					//only table, no view with Hive.
					String type = "T";
					TableType tableType = null;
					 if (type.compareTo("T")==0||getColumnDef(MetadataConst.TABLE_TYPE_TABLE).compareTo(type)==0) {
					     tableType = TableType.Table;
					 } else if (type.compareTo("V")==0||getColumnDef(MetadataConst.TABLE_TYPE_VIEW).compareTo(type)==0) {
						tableType = TableType.View;
					 } else {
						 // it's a procedure, skip it...
					 }
					 if (tableType!=null) {// skip if not set
						 Table table = df.createTable();
						 table.setType(tableType);
						 table.setName(tablename);
						 if (xcatalog!=null) table.setCatalog(xcatalog);
						 if (remarks!=null) table.setDescription(remarks);
						 result.add(table);
					 }
				}
				
			}
			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new DatabaseServiceException("Not able to get tables");
		}
	}

	
	public ResultSet getImportedKeys(Connection conn, String catalog, String name, String tableName)
			throws DatabaseServiceException {
		try {
			return conn.getMetaData().getImportedKeys(catalog, name, tableName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new DatabaseServiceException("Not able to import keys");
		}
	}

}
