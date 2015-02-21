package com.anli.busstation.dal.sql.test.staff;

import com.anli.busstation.dal.interfaces.entities.staff.Driver;
import com.anli.busstation.dal.interfaces.entities.staff.DriverSkill;
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

public abstract class DriverTest extends com.anli.busstation.dal.test.staff.DriverTest {

    protected class DriverSelector implements ResultSetHandler<Driver> {

        @Override
        public Driver handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = BigInteger.valueOf(resultSet.getValue(1, Long.class));
            String name = resultSet.getValue(2, String.class);
            BigDecimal salary = resultSet.getValue(3, BigDecimal.class);
            Timestamp sqlHiringDate = resultSet.getValue(4, Timestamp.class);
            DateTime hiringDate = sqlHiringDate == null ? null : new DateTime(sqlHiringDate.getTime());
            Long longSkillId = resultSet.getValue(5, Long.class);
            BigInteger skillId = longSkillId != null ? BigInteger.valueOf(longSkillId) : null;
            return getNewDriver(resultId, name, salary, hiringDate, skillId, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(Driver driver) throws Exception {
        DateTime hiringDate = driver.getHiringDate();
        Timestamp sqlHiringDate = hiringDate == null ? null : new Timestamp(hiringDate.getMillis());
        String name = driver.getName();
        BigDecimal salary = driver.getSalary();
        DriverSkill skill = driver.getSkill();
        BigInteger skillId = skill == null ? null : skill.getId();
        BigInteger id = generateId();

        SqlExecutor executor = DBHelper.getExecutor();

        String createEmployeeQuery = "insert into employees (employee_id, name, salary, hiring_date)"
                + " values (?, ?, ?, ?)";
        String createDriverQuery = "insert into drivers (employee_id, skill)"
                + " values (?, ?)";

        List employeeParams = new ArrayList(4);
        employeeParams.add(id.longValue());
        employeeParams.add(name);
        employeeParams.add(salary);
        employeeParams.add(sqlHiringDate);

        List driverParams = new ArrayList(2);
        driverParams.add(id.longValue());
        driverParams.add(skillId != null ? skillId.longValue() : null);

        executor.executeUpdate(createEmployeeQuery, employeeParams);
        executor.executeUpdate(createDriverQuery, driverParams);

        return id;
    }

    @Override
    protected Driver getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select e.employee_id, e.name, e.salary, e.hiring_date, d.skill"
                + " from employees e join drivers d on e.employee_id = d.employee_id where d.employee_id = ?";
        return DBHelper.getExecutor().executeSelect(selectQuery, Arrays.asList(id.longValue()), new DriverSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteDriversQuery = "delete d, e from drivers d join employees e on d.employee_id = e.employee_id";
        DBHelper.getExecutor().executeUpdate(deleteDriversQuery, null);
    }
}
