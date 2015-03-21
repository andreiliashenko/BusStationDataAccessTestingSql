package com.anli.busstation.dal.sql.test.vehicles;

import com.anli.busstation.dal.interfaces.entities.vehicles.Bus;
import com.anli.busstation.dal.interfaces.entities.vehicles.Model;
import com.anli.busstation.dal.interfaces.entities.vehicles.TechnicalState;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class BusTest extends com.anli.busstation.dal.test.vehicles.BusTest {

    protected class BusSelector implements ResultSetHandler<Bus> {

        @Override
        public Bus handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            BigDecimal bdModelId = resultSet.getValue(2, BigDecimal.class);
            BigInteger modelId = bdModelId != null ? bdModelId.toBigInteger() : null;
            String plate = resultSet.getValue(3, String.class);
            BigDecimal bdStateId = resultSet.getValue(4, BigDecimal.class);
            BigInteger stateId = bdStateId != null ? bdStateId.toBigInteger() : null;
            return getNewBus(resultId, modelId, plate, stateId, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(Bus bus) throws Exception {
        Model model = bus.getModel();
        BigInteger modelId = model != null ? model.getId() : null;
        String plate = bus.getPlate();
        TechnicalState state = bus.getState();
        BigInteger stateId = state != null ? state.getId() : null;
        BigInteger id = generateId();
        String createQuery = "insert into buses (bus_id, model, plate, state)"
                + " values(?, ?, ?, ?)";
        List params = new ArrayList(4);
        params.add(new BigDecimal(id));
        params.add(modelId != null ? new BigDecimal(modelId) : null);
        params.add(plate);
        params.add(stateId != null ? new BigDecimal(stateId) : null);
        DBHelper.getExecutor().executeUpdate(createQuery, params);
        return id;
    }

    @Override
    protected Bus getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select bus_id, model, plate, state"
                + " from buses where bus_id = ?";
        return DBHelper.getExecutor().executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)),
                new BusSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteBrQuery = "delete from buses";
        DBHelper.getExecutor().executeUpdate(deleteBrQuery, null);
    }
}
