package com.anli.busstation.dal.sql.test.vehicles;

import com.anli.busstation.dal.interfaces.entities.vehicles.GasLabel;
import com.anli.busstation.dal.interfaces.entities.vehicles.Model;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class ModelTest extends com.anli.busstation.dal.test.vehicles.ModelTest {

    protected class ModelSelector implements ResultSetHandler<Model> {

        @Override
        public Model handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger id = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            BigDecimal bdLabelId = resultSet.getValue(2, BigDecimal.class);
            BigInteger gasLabelId = bdLabelId != null ? bdLabelId.toBigInteger() : null;
            BigDecimal gasRate = resultSet.getValue(3, BigDecimal.class);
            String name = resultSet.getValue(4, String.class);
            Integer seatsNumber = resultSet.getValue(5, Integer.class);
            Integer tankVolume = resultSet.getValue(6, Integer.class);
            return getNewModel(id, gasLabelId, gasRate, name, seatsNumber, tankVolume, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(Model model) throws Exception {
        GasLabel gasLabel = model.getGasLabel();
        BigInteger gasLabelId = gasLabel != null ? gasLabel.getId() : null;
        BigDecimal gasRate = model.getGasRate();
        String name = model.getName();
        Integer seatsNumber = model.getSeatsNumber();
        Integer tankVolume = model.getTankVolume();
        BigInteger id = generateId();
        String createQuery = "insert into models (model_id, gas_label, gas_rate, name, seats_number, "
                + "tank_volume) values(?, ?, ?, ?, ?, ?)";
        List params = new ArrayList(6);
        params.add(new BigDecimal(id));
        params.add(gasLabelId != null ? new BigDecimal(gasLabelId) : null);
        params.add(gasRate);
        params.add(name);
        params.add(seatsNumber);
        params.add(tankVolume);
        DBHelper.getExecutor().executeUpdate(createQuery, params);
        return id;
    }

    @Override
    protected Model getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select model_id, gas_label, gas_rate, name, seats_number, tank_volume"
                + " from models where model_id = ?";
        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)), new ModelSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteQuery = "delete from models";
        DBHelper.getExecutor().executeUpdate(deleteQuery, null);
    }
}
