package com.anli.busstation.dal.sql.test.maintenance;

import com.anli.busstation.dal.interfaces.entities.maintenance.BusRefuelling;
import com.anli.busstation.dal.interfaces.entities.maintenance.BusRepairment;
import com.anli.busstation.dal.interfaces.entities.maintenance.BusService;
import com.anli.busstation.dal.interfaces.entities.maintenance.StationService;
import com.anli.busstation.dal.interfaces.entities.maintenance.TechnicalAssignment;
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

public abstract class TechnicalAssignmentTest extends com.anli.busstation.dal.test.maintenance.TechnicalAssignmentTest {

    protected class AssignmentSelector implements ResultSetHandler<TechnicalAssignment> {

        @Override
        public TechnicalAssignment handle(TransformingResultSet resultSet) throws SQLException {
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

            TechnicalAssignment assignment = null;
            BigDecimal bdBusServiceId = resultSet.getValue(6, BigDecimal.class);
            if (bdBusServiceId != null) {
                BigDecimal bdBusId = resultSet.getValue(7, BigDecimal.class);
                BigInteger busId = bdBusId != null ? bdBusId.toBigInteger() : null;
                BigDecimal bdRefuellingId = resultSet.getValue(8, BigDecimal.class);
                if (bdRefuellingId != null) {
                    Integer volume = resultSet.getValue(9, Integer.class);
                    assignment = getNewRefuelling(resultId, mechanicId, beginTime, endTime, cost, busId, volume, true);
                }
                BigDecimal bdRepairmentId = resultSet.getValue(10, BigDecimal.class);
                if (bdRepairmentId != null) {
                    BigDecimal price = resultSet.getValue(11, BigDecimal.class);
                    assignment = getNewRepairment(resultId, mechanicId, beginTime, endTime, cost, busId, price, true);
                }
            }
            BigDecimal bdStationServiceId = resultSet.getValue(12, BigDecimal.class);
            if (bdStationServiceId != null) {
                String description = resultSet.getValue(13, String.class);
                assignment = getNewService(resultId, mechanicId, beginTime, endTime, cost, description, true);
            }
            return assignment;
        }
    }

    @Override
    protected BigInteger createEntityManually(TechnicalAssignment assignment) throws Exception {
        DateTime beginTime = assignment.getBeginTime();
        Timestamp beginTimeStamp = beginTime != null ? new Timestamp(beginTime.getMillis()) : null;
        DateTime endTime = assignment.getEndTime();
        Timestamp endTimeStamp = endTime != null ? new Timestamp(endTime.getMillis()) : null;
        Mechanic mechanic = assignment.getMechanic();
        BigInteger mechanicId = mechanic != null ? mechanic.getId() : null;
        BigDecimal cost = assignment.getServiceCost();
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

        if (assignment instanceof BusService) {
            Bus bus = ((BusService) assignment).getBus();
            BigInteger busId = bus != null ? bus.getId() : null;
            String createBsQuery = "insert into bus_services(assignment_id, bus)"
                    + " values(?, ?)";
            List bsParams = new ArrayList(2);
            bsParams.add(new BigDecimal(id));
            bsParams.add(busId != null ? new BigDecimal(busId) : null);
            executor.executeUpdate(createBsQuery, bsParams);
        }

        if (assignment instanceof BusRepairment) {
            BigDecimal price = ((BusRepairment) assignment).getExpendablesPrice();
            String createBrQuery = "insert into bus_repairments(assignment_id, expendables_price)"
                    + " values(?, ?)";
            List brParams = new ArrayList(2);
            brParams.add(new BigDecimal(id));
            brParams.add(price);
            executor.executeUpdate(createBrQuery, brParams);
        }
        if (assignment instanceof BusRefuelling) {
            Integer volume = ((BusRefuelling) assignment).getVolume();
            String createBrQuery = "insert into bus_refuellings(assignment_id, gas_volume)"
                    + " values(?, ?)";
            List brParams = new ArrayList(2);
            brParams.add(new BigDecimal(id));
            brParams.add(volume);
            executor.executeUpdate(createBrQuery, brParams);
        }
        if (assignment instanceof StationService) {
            String description = ((StationService) assignment).getDescription();
            String createSsQuery = "insert into station_services(assignment_id, description)"
                    + " values(?, ?)";
            List ssParams = new ArrayList(2);
            ssParams.add(new BigDecimal(id));
            ssParams.add(description);
            executor.executeUpdate(createSsQuery, ssParams);
        }
        return id;
    }

    @Override
    protected TechnicalAssignment getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select ta.assignment_id, ta.mechanic, ta.begin_time, ta.end_time, ta.service_cost, "
                + "bs.assignment_id, bs.bus, bref.assignment_id, bref.gas_volume, brep.assignment_id, brep.expendables_price, "
                + "ss.assignment_id, ss.description "
                + "from technical_assignments ta left join bus_services bs "
                + " on ta.assignment_id = bs.assignment_id left join bus_refuellings bref"
                + " on bs.assignment_id = bref.assignment_id left join bus_repairments brep "
                + " on ta.assignment_id = brep.assignment_id left join station_services ss" 
                + " on ss.assignment_id = ta.assignment_id where ta.assignment_id = ?";
        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)), new AssignmentSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteAssignmentsQuery = "delete ta, bref, brep, bs, ss "
                + "from technical_assignments ta left join bus_services bs "
                + "on ta.assignment_id = bs.assignment_id left join bus_refuellings bref "
                + "on bref.assignment_id = bs.assignment_id left join bus_repairments brep "
                + "on brep.assignment_id = bs.assignment_id left join station_services ss "
                + "on ss.assignment_id = ta.assignment_id";
        DBHelper.getExecutor().executeUpdate(deleteAssignmentsQuery, null);
    }
}
