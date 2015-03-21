package com.anli.busstation.dal.sql.test;

import com.anli.sqlexecution.execution.SqlExecutor;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBHelper {

    private static final Logger LOG = LoggerFactory.getLogger(DBHelper.class);

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
            LOG.error("Could not resolve datasource " + POOL_NAME, nex);
            throw new RuntimeException(nex);
        }
    }
}
