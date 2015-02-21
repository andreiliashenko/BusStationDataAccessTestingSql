package com.anli.busstation.dal.sql.test;

import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class IdSelector implements ResultSetHandler<List<BigInteger>> {

    @Override
    public List<BigInteger> handle(TransformingResultSet resultSet) throws SQLException {
        List<BigInteger> idList = new ArrayList<>();
        while (resultSet.next()) {
            idList.add(resultSet.getValue(1, BigDecimal.class).toBigInteger());
        }
        return idList;
    }
}
