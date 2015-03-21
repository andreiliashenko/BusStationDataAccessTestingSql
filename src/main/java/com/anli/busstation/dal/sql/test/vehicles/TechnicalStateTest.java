package com.anli.busstation.dal.sql.test.vehicles;

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

public abstract class TechnicalStateTest extends com.anli.busstation.dal.test.vehicles.TechnicalStateTest {

    protected class TechnicalStateSelector implements ResultSetHandler<TechnicalState> {

        @Override
        public TechnicalState handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger id = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            String description = resultSet.getValue(2, String.class);
            Integer diffLevel = resultSet.getValue(3, Integer.class);
            return getNewState(id, description, diffLevel);
        }
    }

    @Override
    protected BigInteger createEntityManually(TechnicalState state) throws Exception {
        String description = state.getDescription();
        Integer diffLevel = state.getDifficultyLevel();
        BigInteger id = generateId();
        String createQuery = "insert into technical_states (state_id, description, difficulty_level) "
                + "values(?, ?, ?)";
        List params = new ArrayList(3);
        params.add(new BigDecimal(id));
        params.add(description);
        params.add(diffLevel);
        DBHelper.getExecutor().executeUpdate(createQuery, params);
        return id;
    }

    @Override
    protected TechnicalState getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select state_id, description, difficulty_level "
                + "from technical_states where state_id = ?";
        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)),
                        new TechnicalStateSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteQuery = "delete from technical_states";
        DBHelper.getExecutor().executeUpdate(deleteQuery, null);
    }
}
