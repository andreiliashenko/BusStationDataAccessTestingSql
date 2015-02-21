package com.anli.busstation.dal.sql.test.staff;

import com.anli.busstation.dal.interfaces.entities.staff.Driver;
import com.anli.busstation.dal.interfaces.entities.staff.DriverSkill;
import com.anli.busstation.dal.interfaces.entities.staff.Employee;
import com.anli.busstation.dal.interfaces.entities.staff.Mechanic;
import com.anli.busstation.dal.interfaces.entities.staff.MechanicSkill;
import com.anli.busstation.dal.interfaces.entities.staff.Salesman;
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

public abstract class EmployeeTest extends com.anli.busstation.dal.test.staff.EmployeeTest {

    protected class EmployeeSelector implements ResultSetHandler<Employee> {

        @Override
        public Employee handle(TransformingResultSet resultSet) throws SQLException {

            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            String name = resultSet.getValue(2, String.class);
            BigDecimal salary = resultSet.getValue(3, BigDecimal.class);
            Timestamp sqlHiringDate = resultSet.getValue(4, Timestamp.class);
            DateTime hiringDate = sqlHiringDate == null ? null : new DateTime(sqlHiringDate.getTime());
            BigDecimal driverId = resultSet.getValue(5, BigDecimal.class);
            Employee employee = null;
            if (driverId != null) {
                BigDecimal bdDsId = resultSet.getValue(8, BigDecimal.class);
                BigInteger driverSkillId = bdDsId != null ? bdDsId.toBigInteger() : null;
                employee = getNewDriver(resultId, name, salary, hiringDate, driverSkillId, true);
            }
            BigDecimal mechanicId = resultSet.getValue(6, BigDecimal.class);
            if (mechanicId != null) {
                BigDecimal bdMsId = resultSet.getValue(9, BigDecimal.class);
                BigInteger mechanicSkillId = bdMsId != null ? bdMsId.toBigInteger() : null;
                employee = getNewMechanic(resultId, name, salary, hiringDate, mechanicSkillId, true);
            }
            BigDecimal salesmanId = resultSet.getValue(7, BigDecimal.class);
            if (salesmanId != null) {
                Integer totalSales = resultSet.getValue(10, Integer.class);
                employee = getNewSalesman(resultId, name, salary, hiringDate, totalSales);
            }
            return employee;
        }
    }

    @Override
    protected BigInteger createEntityManually(Employee employee) throws Exception {
        DateTime hiringDate = employee.getHiringDate();
        Timestamp sqlHiringDate = hiringDate == null ? null : new Timestamp(hiringDate.getMillis());
        String name = employee.getName();
        BigDecimal salary = employee.getSalary();
        BigInteger id = generateId();

        SqlExecutor executor = DBHelper.getExecutor();
        String createEmployeeQuery = "insert into employees (employee_id, name, salary, hiring_date)"
                + " values (?, ?, ?, ?)";
        List employeeParams = new ArrayList(4);
        employeeParams.add(new BigDecimal(id));
        employeeParams.add(name);
        employeeParams.add(salary);
        employeeParams.add(sqlHiringDate);
        executor.executeUpdate(createEmployeeQuery, employeeParams);

        if (employee instanceof Driver) {
            DriverSkill skill = ((Driver) employee).getSkill();
            BigInteger skillId = skill == null ? null : skill.getId();
            String createDriverQuery = "insert into drivers (employee_id, skill)"
                    + " values (?, ?)";
            List driverParams = new ArrayList(2);
            driverParams.add(new BigDecimal(id));
            driverParams.add(skillId != null ? new BigDecimal(skillId) : null);
            executor.executeUpdate(createDriverQuery, driverParams);
        }
        if (employee instanceof Mechanic) {
            MechanicSkill skill = ((Mechanic) employee).getSkill();
            BigInteger skillId = skill == null ? null : skill.getId();
            String createMechanicQuery = "insert into mechanics (employee_id, skill)"
                    + " values (?, ?)";
            List mechanicParams = new ArrayList(2);
            mechanicParams.add(new BigDecimal(id));
            mechanicParams.add(skillId != null ? new BigDecimal(skillId) : null);
            executor.executeUpdate(createMechanicQuery, mechanicParams);
        }
        if (employee instanceof Salesman) {
            Integer totalSales = ((Salesman) employee).getTotalSales();
            String createSalesmanQuery = "insert into salesmen (employee_id, total_sales)"
                    + " values (?, ?)";
            List salesmanParams = new ArrayList(2);
            salesmanParams.add(new BigDecimal(id));
            salesmanParams.add(totalSales);
            executor.executeUpdate(createSalesmanQuery, salesmanParams);
        }
        return id;
    }

    @Override
    protected Employee getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select e.employee_id, e.name, e.salary, e.hiring_date,"
                + " d.employee_id, m.employee_id, s.employee_id, "
                + " d.skill, m.skill, s.total_sales"
                + " from employees e left join drivers d on e.employee_id = d.employee_id"
                + " left join mechanics m on e.employee_id = m.employee_id"
                + " left join salesmen s on e.employee_id = s.employee_id"
                + " where e.employee_id = ?";
        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)), new EmployeeSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        SqlExecutor executor = DBHelper.getExecutor();
        String deleteEmployeesQuery = "delete e, s, m, d from employees e left join mechanics m"
                + " on e.employee_id = m.employee_id left join drivers d on d.employee_id = e.employee_id"
                + " left join salesmen s on s.employee_id = e.employee_id";
        executor.executeUpdate(deleteEmployeesQuery, null);
    }
}
