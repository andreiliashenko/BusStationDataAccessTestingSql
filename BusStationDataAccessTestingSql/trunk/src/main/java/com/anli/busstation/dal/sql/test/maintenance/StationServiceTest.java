package com.anli.busstation.dal.sql.test.maintenance;

import com.anli.busstation.dal.interfaces.entities.staff.Mechanic;
import com.anli.busstation.dal.interfaces.entities.maintenance.StationService;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.execution.SqlExecutor;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.joda.time.DateTime;

public abstract class StationServiceTest extends com.anli.busstation.dal.test.maintenance.StationServiceTest {

    protected class StationServiceSelector implements ResultSetHandler<StationService> {

        @Override
        public StationService handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            BigDecimal bdMechanicId = resultSet.getValue(2, BigDecimal.class);
            BigInteger mechanicId = bdMechanicId != null ? bdMechanicId.toBigInteger() : null;
            Timestamp beginTimeStamp = resultSet.getValue(3, Timestamp.class);
            DateTime beginTime = beginTimeStamp != null ? new DateTime(beginTimeStamp.getTime()) : null;
            Timestamp endTimeStamp = resultSet.getValue(4, Timestamp.class);
            DateTime endTime = endTimeStamp != null ? new DateTime(endTimeStamp.getTime()) : null;
            BigDecimal cost = resultSet.getValue(5, BigDecimal.class);
            String description = resultSet.getValue(6, String.class);
            return getNewService(resultId, mechanicId, beginTime, endTime, cost, description, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(StationService service) throws Exception {
        DateTime beginTime = service.getBeginTime();
        Timestamp beginTimeStamp = beginTime != null ? new Timestamp(beginTime.getMillis()) : null;
        String description = service.getDescription();
        DateTime endTime = service.getEndTime();
        Timestamp endTimeStamp = endTime != null ? new Timestamp(endTime.getMillis()) : null;
        Mechanic mechanic = service.getMechanic();
        BigInteger mechanicId = mechanic != null ? mechanic.getId() : null;
        BigDecimal cost = service.getServiceCost();
        BigInteger id = generateId();

        SqlExecutor executor = DBHelper.getExecutor();
        String createTaQuery = "insert into technical_assignments (assignment_id, mechanic, begin_time, end_time, service_cost)"
                + " values(?, ?, ?, ?, ?)";
        List taParams = new ArrayList(5);
        taParams.add(new BigDecimal(id));
        taParams.add(mechanicId != null ? new BigDecimal(mechanicId) : null);
        taParams.add(beginTimeStamp);
        taParams.add(endTimeStamp);
        taParams.add(cost);

        String createSsQuery = "insert into station_services(assignment_id, description)"
                + " values(?, ?)";
        List ssParams = new ArrayList(2);
        ssParams.add(new BigDecimal(id));
        ssParams.add(description);

        executor.executeUpdate(createTaQuery, taParams);
        executor.executeUpdate(createSsQuery, ssParams);
        return id;
    }

    @Override
    protected StationService getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select ta.assignment_id, ta.mechanic, ta.begin_time, ta.end_time, ta.service_cost, "
                + "ss.description from technical_assignments ta join station_services ss "
                + " on ta.assignment_id = ss.assignment_id where ta.assignment_id = ?";
        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)), new StationServiceSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteSsQuery = "delete ss, ta from station_services ss join technical_assignments ta "
                + "on ss.assignment_id = ta.assignment_id";
        DBHelper.getExecutor().executeUpdate(deleteSsQuery, null);
    }
}
