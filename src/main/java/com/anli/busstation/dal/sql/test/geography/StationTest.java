package com.anli.busstation.dal.sql.test.geography;

import com.anli.busstation.dal.interfaces.entities.vehicles.Bus;
import com.anli.busstation.dal.interfaces.entities.staff.Employee;
import com.anli.busstation.dal.interfaces.entities.geography.Station;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.busstation.dal.sql.test.IdSelector;
import com.anli.sqlexecution.execution.SqlExecutor;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class StationTest extends com.anli.busstation.dal.test.geography.StationTest {

    protected class StationSelector implements ResultSetHandler<Station> {

        protected final List<BigInteger> busList;
        protected final List<BigInteger> employeeList;

        public StationSelector(List<BigInteger> busList, List<BigInteger> employeeList) {
            this.busList = busList;
            this.employeeList = employeeList;
        }

        @Override
        public Station handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            BigDecimal latitude = resultSet.getValue(2, BigDecimal.class);
            BigDecimal longitude = resultSet.getValue(3, BigDecimal.class);
            String name = resultSet.getValue(4, String.class);
            return getNewStation(resultId, latitude, longitude, name, busList, employeeList, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(Station station) throws Exception {
        List<Bus> busList = station.getBuses();
        List<Employee> employeeList = station.getEmployees();
        BigDecimal latitude = station.getLatitude();
        BigDecimal longitude = station.getLongitude();
        String name = station.getName();
        BigInteger id = generateId();
        SqlExecutor executor = DBHelper.getExecutor();
        String createQuery = "insert into stations (station_id, latitude, longitude, name)"
                + " values(?, ?, ?, ?)";
        List params = new ArrayList(4);
        params.add(new BigDecimal(id));
        params.add(latitude);
        params.add(longitude);
        params.add(name);
        executor.executeUpdate(createQuery, params);

        String linkBusesQuery = "update buses set station = ?, station_order = ? where bus_id = ?";
        int index = 0;
        for (Bus bus : busList) {
            index++;
            executor.executeUpdate(linkBusesQuery, Arrays.asList(new BigDecimal(id), index, new BigDecimal(bus.getId())));
        }

        String linkEmployeesQuery = "update employees set station = ?, station_order = ? where employee_id = ?";
        index = 0;
        for (Employee employee : employeeList) {
            index++;
            executor.executeUpdate(linkEmployeesQuery, Arrays.asList(new BigDecimal(id), index, new BigDecimal(employee.getId())));
        }
        return id;
    }

    @Override
    protected Station getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select station_id, latitude, longitude, name"
                + " from stations where station_id = ?";
        String selectBusesQuery = "select bus_id from buses where station = ? order by station_order";
        String selectEmployeesQuery = "select employee_id from employees where station = ? order by station_order";
        SqlExecutor executor = DBHelper.getExecutor();
        List<BigInteger> busIds = executor.executeSelect(selectBusesQuery, Arrays.asList(new BigDecimal(id)),
                new IdSelector());
        List<BigInteger> employeeIds = executor.executeSelect(selectEmployeesQuery, Arrays.asList(new BigDecimal(id)),
                new IdSelector());
        return executor.executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)),
                new StationSelector(busIds, employeeIds));
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteStationsQuery = "delete from stations";
        DBHelper.getExecutor().executeUpdate(deleteStationsQuery, null);
    }
}
