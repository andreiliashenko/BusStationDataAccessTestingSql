package com.anli.busstation.dal.sql.test.maintenance;

import com.anli.busstation.dal.interfaces.entities.vehicles.Bus;
import com.anli.busstation.dal.interfaces.entities.maintenance.BusRefuelling;
import com.anli.busstation.dal.interfaces.entities.staff.Mechanic;
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

public abstract class BusRefuellingTest extends com.anli.busstation.dal.test.maintenance.BusRefuellingTest {

    protected class RefuellingSelector implements ResultSetHandler<BusRefuelling> {

        @Override
        public BusRefuelling handle(TransformingResultSet resultSet) throws SQLException {
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
            BigDecimal bdBusId = resultSet.getValue(6, BigDecimal.class);
            BigInteger busId = bdBusId != null ? bdBusId.toBigInteger() : null;
            Integer volume = resultSet.getValue(7, Integer.class);
            return getNewRefuelling(resultId, mechanicId, beginTime, endTime, cost, busId, volume, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(BusRefuelling refuelling) throws Exception {
        DateTime beginTime = refuelling.getBeginTime();
        Timestamp beginTimeStamp = beginTime != null ? new Timestamp(beginTime.getMillis()) : null;
        Bus bus = refuelling.getBus();
        BigInteger busId = bus != null ? bus.getId() : null;
        DateTime endTime = refuelling.getEndTime();
        Timestamp endTimeStamp = endTime != null ? new Timestamp(endTime.getMillis()) : null;
        Mechanic mechanic = refuelling.getMechanic();
        BigInteger mechanicId = mechanic != null ? mechanic.getId() : null;
        BigDecimal cost = refuelling.getServiceCost();
        Integer volume = refuelling.getVolume();
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
        executor.executeUpdate(createTaQuery, taParams);

        String createBsQuery = "insert into bus_services(assignment_id, bus)"
                + " values(?, ?)";
        List bsParams = new ArrayList(2);
        bsParams.add(new BigDecimal(id));
        bsParams.add(busId != null ? new BigDecimal(busId) : null);
        executor.executeUpdate(createBsQuery, bsParams);

        String createBrQuery = "insert into bus_refuellings(assignment_id, gas_volume)"
                + " values(?, ?)";
        List brParams = new ArrayList(2);
        brParams.add(new BigDecimal(id));
        brParams.add(volume);
        executor.executeUpdate(createBrQuery, brParams);

        return id;
    }

    @Override
    protected BusRefuelling getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select ta.assignment_id, ta.mechanic, ta.begin_time, ta.end_time, ta.service_cost, "
                + "bs.bus, br.gas_volume from technical_assignments ta join bus_services bs "
                + " on ta.assignment_id = bs.assignment_id join bus_refuellings br "
                + " on ta.assignment_id = br.assignment_id where ta.assignment_id = ?";
        return DBHelper.getExecutor().executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)), new RefuellingSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteBrQuery = "delete br, bs, ta from bus_refuellings br join bus_services bs "
                + "on br.assignment_id = bs.assignment_id join technical_assignments ta "
                + "on bs.assignment_id = ta.assignment_id";
        DBHelper.getExecutor().executeUpdate(deleteBrQuery, null);
    }

}
