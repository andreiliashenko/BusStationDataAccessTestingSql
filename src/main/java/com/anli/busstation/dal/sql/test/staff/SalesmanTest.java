package com.anli.busstation.dal.sql.test.staff;

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

public abstract class SalesmanTest extends com.anli.busstation.dal.test.staff.SalesmanTest {

    protected class SalesmanSelector implements ResultSetHandler<Salesman> {

        @Override
        public Salesman handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            String name = resultSet.getValue(2, String.class);
            BigDecimal salary = resultSet.getValue(3, BigDecimal.class);
            Timestamp sqlHiringDate = resultSet.getValue(4, Timestamp.class);
            DateTime hiringDate = sqlHiringDate == null ? null : new DateTime(sqlHiringDate.getTime());
            Integer totalSales = resultSet.getValue(5, Integer.class);
            return getNewSalesman(resultId, name, salary, hiringDate, totalSales);
        }
    }

    @Override
    protected BigInteger createEntityManually(Salesman salesman) throws Exception {
        DateTime hiringDate = salesman.getHiringDate();
        Timestamp sqlHiringDate = hiringDate == null ? null : new Timestamp(hiringDate.getMillis());
        String name = salesman.getName();
        BigDecimal salary = salesman.getSalary();
        Integer totalSales = salesman.getTotalSales();
        BigInteger id = generateId();
        String createEmployeeQuery = "insert into employees (employee_id, name, salary, hiring_date)"
                + " values (?, ?, ?, ?)";
        String createSalesmanQuery = "insert into salesmen (employee_id, total_sales)"
                + " values (?, ?)";
        List employeeParams = new ArrayList(4);
        employeeParams.add(new BigDecimal(id));
        employeeParams.add(name);
        employeeParams.add(salary);
        employeeParams.add(sqlHiringDate);
        List salesmanParams = new ArrayList(2);
        salesmanParams.add(new BigDecimal(id));
        salesmanParams.add(totalSales);
        SqlExecutor executor = DBHelper.getExecutor();
        executor.executeUpdate(createEmployeeQuery, employeeParams);
        executor.executeUpdate(createSalesmanQuery, salesmanParams);
        return id;
    }

    @Override
    protected Salesman getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select e.employee_id, e.name, e.salary, e.hiring_date, s.total_sales"
                + " from employees e join salesmen s on e.employee_id = s.employee_id "
                + "where s.employee_id = ?";
        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)), new SalesmanSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteSalesmenQuery = "delete s, e from salesmen s join employees e "
                + "on s.employee_id = e.employee_id";
        DBHelper.getExecutor().executeUpdate(deleteSalesmenQuery, null);
    }
}
