package com.anli.busstation.dal.sql.test.vehicles;

import com.anli.busstation.dal.interfaces.entities.vehicles.GasLabel;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class GasLabelTest extends com.anli.busstation.dal.test.vehicles.GasLabelTest {

    protected class GasLabelSelector implements ResultSetHandler<GasLabel> {

        @Override
        public GasLabel handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger id = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            String name = resultSet.getValue(2, String.class);
            BigDecimal price = resultSet.getValue(3, BigDecimal.class);

            GasLabel label = getNewGasLabel(id, name, price);
            return label;
        }
    }

    @Override
    protected BigInteger createEntityManually(GasLabel label) throws Exception {
        String name = label.getName();
        BigDecimal price = label.getPrice();
        BigInteger id = generateId();

        String createQuery = "insert into gas_labels (label_id, name, price) values(?, ?, ?)";
        List createParams = new ArrayList(3);
        createParams.add(new BigDecimal(id));
        createParams.add(name);
        createParams.add(price);

        DBHelper.getExecutor().executeUpdate(createQuery, createParams);
        return id;
    }

    @Override
    protected GasLabel getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select label_id, name, price from gas_labels where label_id = ?";

        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)), new GasLabelSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteQuery = "delete from gas_labels";
        DBHelper.getExecutor().executeUpdate(deleteQuery, null);
    }
}
