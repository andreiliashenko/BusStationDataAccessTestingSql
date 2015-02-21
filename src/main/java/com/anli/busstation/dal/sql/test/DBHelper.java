package com.anli.busstation.dal.sql.test;

import com.anli.sqlexecution.execution.SqlExecutor;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class DBHelper {
    
    protected static final String POOL_NAME = "jdbc/BSA";
    protected static DataSource source = getDataSource();
    
    public static SqlExecutor getExecutor() {
        return new SqlExecutor(source, null);
    }
    
    protected static DataSource getDataSource() {
        try {
            InitialContext ic = new InitialContext();
            return (DataSource) ic.lookup(POOL_NAME);
        } catch (NamingException nex) {
            throw new RuntimeException(nex);
        }
    }
}
