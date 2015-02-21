package com.anli.busstation.dal.sql.test.staff;

import com.anli.busstation.dal.interfaces.entities.staff.DriverSkill;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class DriverSkillTest extends com.anli.busstation.dal.test.staff.DriverSkillTest {

    protected class DriverSkillSelector implements ResultSetHandler<DriverSkill> {

        @Override
        public DriverSkill handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger id = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            String name = resultSet.getValue(2, String.class);
            Integer maxPassengers = resultSet.getValue(3, Integer.class);
            Integer maxRideLength = resultSet.getValue(4, Integer.class);
            return getNewDriverSkill(id, name, maxPassengers, maxRideLength);
        }
    }

    @Override
    protected BigInteger createEntityManually(DriverSkill skill) throws Exception {
        String name = skill.getName();
        Integer maxPassengers = skill.getMaxPassengers();
        Integer maxRideLength = skill.getMaxRideLength();
        BigInteger id = generateId();
        String createQuery = "insert into driver_skills (skill_id, name, max_passengers, max_ride_length) values(?, ?, ?, ?)";
        List params = new ArrayList(4);
        params.add(new BigDecimal(id));
        params.add(name);
        params.add(maxPassengers);
        params.add(maxRideLength);

        DBHelper.getExecutor().executeUpdate(createQuery, params);
        return id;
    }

    @Override
    protected DriverSkill getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select skill_id, name, max_passengers, max_ride_length from driver_skills where skill_id = ?";
        return DBHelper.getExecutor().executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)),
                new DriverSkillSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteBrQuery = "delete from driver_skills";
        DBHelper.getExecutor().executeUpdate(deleteBrQuery, null);
    }
}
