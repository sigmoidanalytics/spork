package org.apache.pig.backend.hadoop.hbase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;


/**
 * Class to encapsulate logic around which column names were specified in each
 * position of the column list. Users can specify columns names in one of 4
 * ways: 'Foo:', 'Foo:*', 'Foo:bar*' or 'Foo:bar'. The first 3 result in a
 * Map being added to the tuple, while the last results in a scalar. The 3rd
 * form results in a prefix-filtered Map.
 */
public class ColumnInfo implements Serializable {

	private final static String ASTERISK = "*";
	private final static String COLON = ":";
	final String originalColumnName;  // always set
	final byte[] columnFamily; // always set
	final byte[] columnName; // set if it exists and doesn't contain '*'
	final byte[] columnPrefix; // set if contains a prefix followed by '*'

	public ColumnInfo(String colName) {
		originalColumnName = colName;
		String[] cfAndColumn = colName.split(COLON, 2);

		//CFs are byte[1] and columns are byte[2]
		columnFamily = Bytes.toBytes(cfAndColumn[0]);
		if (cfAndColumn.length > 1 &&
				cfAndColumn[1].length() > 0 && !ASTERISK.equals(cfAndColumn[1])) {
			if (cfAndColumn[1].endsWith(ASTERISK)) {
				columnPrefix = Bytes.toBytes(cfAndColumn[1].substring(0,
						cfAndColumn[1].length() - 1));
				columnName = null;
			}
			else {
				columnName = Bytes.toBytes(cfAndColumn[1]);
				columnPrefix = null;
			}
		} else {
			columnPrefix = null;
			columnName = null;
		}
	}

	public byte[] getColumnFamily() { return columnFamily; }
	public byte[] getColumnName() { return columnName; }
	public byte[] getColumnPrefix() { return columnPrefix; }
	public boolean isColumnMap() { return columnName == null; }

	public boolean hasPrefixMatch(byte[] qualifier) {
		return Bytes.startsWith(qualifier, columnPrefix);
	}

	@Override
	public String toString() { return originalColumnName; }
}    
