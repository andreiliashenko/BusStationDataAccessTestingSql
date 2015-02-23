package com.anli.busstation.dal.sql.test.maintenance;

import com.anli.busstation.dal.interfaces.entities.maintenance.BusRefuelling;
import com.anli.busstation.dal.interfaces.entities.maintenance.BusRepairment;
import com.anli.busstation.dal.interfaces.entities.maintenance.BusService;
import com.anli.busstation.dal.interfaces.entities.staff.Mechanic;
import com.anli.busstation.dal.interfaces.entities.vehicles.Bus;
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

public abstract class BusServiceTest extends com.anli.busstation.dal.test.maintenance.BusServiceTest {

    protected class BusServiceSelector implements ResultSetHandler<BusService> {

        @Override
        public BusService handle(TransformingResultSet resultSet) throws SQLException {
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

            BigDecimal bdRefuellingId = resultSet.getValue(7, BigDecimal.class);
            BusService service = null;
            if (bdRefuellingId != null) {
                Integer volume = resultSet.getValue(8, Integer.class);
                service = getNewRefuelling(resultId, mechanicId, beginTime, endTime, cost, busId, volume, true);
            }
            BigDecimal bdRepairmentId = resultSet.getValue(9, BigDecimal.class);
            if (bdRepairmentId != null) {
                BigDecimal price = resultSet.getValue(10, BigDecimal.class);
                service = getNewRepairment(resultId, mechanicId, beginTime, endTime, cost, busId, price, true);
            }
            return service;
        }
    }

    @Override
    protected BigInteger createEntityManually(BusService service) throws Exception {
        DateTime beginTime = service.getBeginTime();
        Timestamp beginTimeStamp = beginTime != null ? new Timestamp(beginTime.getMillis()) : null;
        Bus bus = service.getBus();
        BigInteger busId = bus != null ? bus.getId() : null;
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
        executor.executeUpdate(createTaQuery, taParams);

        String createBsQuery = "insert into bus_services(assignment_id, bus)"
                + " values(?, ?)";
        List bsParams = new ArrayList(2);
        bsParams.add(new BigDecimal(id));
        bsParams.add(busId != null ? new BigDecimal(busId) : null);
        executor.executeUpdate(createBsQuery, bsParams);

        if (service instanceof BusRepairment) {
            BigDecimal price = ((BusRepairment) service).getExpendablesPrice();
            String createBrQuery = "insert into bus_repairments(assignment_id, expendables_price)"
                    + " values(?, ?)";
            List brParams = new ArrayList(2);
            brParams.add(new BigDecimal(id));
            brParams.add(price);
            executor.executeUpdate(createBrQuery, brParams);
        }
        if (service instanceof BusRefuelling) {
            Integer volume = ((BusRefuelling) service).getVolume();
            String createBrQuery = "insert into bus_refuellings(assignment_id, gas_volume)"
                    + " values(?, ?)";
            List brParams = new ArrayList(2);
            brParams.add(new BigDecimal(id));
            brParams.add(volume);
            executor.executeUpdate(createBrQuery, brParams);
        }
        return id;
    }

    @Override
    protected BusService getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select ta.assignment_id, ta.mechanic, ta.begin_time, ta.end_time, ta.service_cost, "
                + "bs.bus, bref.assignment_id, bref.gas_volume, brep.assignment_id, brep.expendables_price "
                + "from technical_assignments ta join bus_services bs "
                + " on ta.assignment_id = bs.assignment_id left join bus_refuellings bref"
                + " on bs.assignment_id = bref.assignment_id left join bus_repairments brep "
                + " on ta.assignment_id = brep.assignment_id where ta.assignment_id = ?";
        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)), new BusServiceSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteServicesQuery = "delete ta, bref, brep, bs "
                + "from technical_assignments ta join bus_services bs "
                + "on ta.assignment_id = bs.assignment_id left join bus_refuellings bref "
                + "on bref.assignment_id = bs.assignment_id left join bus_repairments brep "
                + "on brep.assignment_id = bs.assignment_id";
        DBHelper.getExecutor().executeUpdate(deleteServicesQuery, null);
    }
}
