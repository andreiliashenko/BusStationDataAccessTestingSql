package com.anli.busstation.dal.sql.test.maintenance;

import com.anli.busstation.dal.interfaces.entities.vehicles.Bus;
import com.anli.busstation.dal.interfaces.entities.maintenance.BusRepairment;
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

public abstract class BusRepairmentTest extends com.anli.busstation.dal.test.maintenance.BusRepairmentTest {

    protected class RepairmentSelector implements ResultSetHandler<BusRepairment> {

        @Override
        public BusRepairment handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = BigInteger.valueOf(resultSet.getValue(1, Long.class));
            Long longMechanicId = resultSet.getValue(2, Long.class);
            BigInteger mechanicId = longMechanicId != null ? BigInteger.valueOf(longMechanicId) : null;
            Timestamp beginTimeStamp = resultSet.getValue(3, Timestamp.class);
            DateTime beginTime = beginTimeStamp != null ? new DateTime(beginTimeStamp.getTime()) : null;
            Timestamp endTimeStamp = resultSet.getValue(4, Timestamp.class);
            DateTime endTime = endTimeStamp != null ? new DateTime(endTimeStamp.getTime()) : null;
            BigDecimal cost = resultSet.getValue(5, BigDecimal.class);
            Long longBusId = resultSet.getValue(6, Long.class);
            BigInteger busId = longBusId != null ? BigInteger.valueOf(longBusId) : null;
            BigDecimal price = resultSet.getValue(7, BigDecimal.class);
            return getNewRepairment(resultId, mechanicId, beginTime, endTime, cost, busId, price, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(BusRepairment repairment) throws Exception {
        DateTime beginTime = repairment.getBeginTime();
        Timestamp beginTimeStamp = beginTime != null ? new Timestamp(beginTime.getMillis()) : null;
        Bus bus = repairment.getBus();
        BigInteger busId = bus != null ? bus.getId() : null;
        DateTime endTime = repairment.getEndTime();
        Timestamp endTimeStamp = endTime != null ? new Timestamp(endTime.getMillis()) : null;
        Mechanic mechanic = repairment.getMechanic();
        BigInteger mechanicId = mechanic != null ? mechanic.getId() : null;
        BigDecimal cost = repairment.getServiceCost();
        BigDecimal price = repairment.getExpendablesPrice();
        BigInteger id = generateId();

        SqlExecutor executor = DBHelper.getExecutor();
        String createTaQuery = "insert into technical_assignments (assignment_id, mechanic, begin_time, end_time, service_cost)"
                + " values(?, ?, ?, ?, ?)";
        List taParams = new ArrayList(5);
        taParams.add(id.longValue());
        taParams.add(mechanicId != null ? mechanicId.longValue() : null);
        taParams.add(beginTimeStamp);
        taParams.add(endTimeStamp);
        taParams.add(cost);
        executor.executeUpdate(createTaQuery, taParams);

        String createBsQuery = "insert into bus_services(assignment_id, bus)"
                + " values(?, ?)";
        List bsParams = new ArrayList(2);
        bsParams.add(id.longValue());
        bsParams.add(busId != null ? busId.longValue() : null);
        executor.executeUpdate(createBsQuery, bsParams);

        String createBrQuery = "insert into bus_repairments(assignment_id, expendables_price)"
                + " values(?, ?)";
        List brParams = new ArrayList(2);
        brParams.add(id.longValue());
        brParams.add(price);
        executor.executeUpdate(createBrQuery, brParams);

        return id;
    }

    @Override
    protected BusRepairment getEntityManually(BigInteger id) throws Exception {
        SqlExecutor executor = DBHelper.getExecutor();
        String selectQuery = "select ta.assignment_id, ta.mechanic, ta.begin_time, ta.end_time, ta.service_cost, "
                + "bs.bus, br.expendables_price from technical_assignments ta join bus_services bs "
                + " on ta.assignment_id = bs.assignment_id join bus_repairments br "
                + " on ta.assignment_id = br.assignment_id where ta.assignment_id = ?";
        return executor.executeSelect(selectQuery, Arrays.asList(id.longValue()), new RepairmentSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteBrQuery = "delete br, bs, ta from bus_repairments br join bus_services bs "
                + "on br.assignment_id = bs.assignment_id join technical_assignments ta "
                + "on bs.assignment_id = ta.assignment_id";
        DBHelper.getExecutor().executeUpdate(deleteBrQuery, null);
    }

}
