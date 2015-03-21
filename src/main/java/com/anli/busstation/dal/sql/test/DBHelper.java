package com.anli.busstation.dal.sql.test;

import com.anli.configuration.Configurator;
import com.anli.sqlexecution.execution.SqlExecutor;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBHelper {

    private static final Logger LOG = LoggerFactory.getLogger(DBHelper.class);

    protected static final String DB_GROUP = "db";
    protected static final String CONNECTION_POOL_PROPERTY = "connection_pool";

    protected static DataSource source = getDataSource();

    public static SqlExecutor getExecutor() {
        return new SqlExecutor(source, null);
    }

    protected static DataSource getDataSource() {
        String poolName = Configurator.getConfig(DB_GROUP).getProperty(CONNECTION_POOL_PROPERTY);
        try {
            InitialContext ic = new InitialContext();
            return (DataSource) ic.lookup(poolName);
        } catch (NamingException nex) {
            LOG.error("Could not resolve datasource " + poolName, nex);
            throw new RuntimeException(nex);
        }
    }
}
