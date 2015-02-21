package com.anli.busstation.dal.sql.test.staff;

import com.anli.busstation.dal.interfaces.entities.staff.Mechanic;
import com.anli.busstation.dal.interfaces.entities.staff.MechanicSkill;
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

public abstract class MechanicTest extends com.anli.busstation.dal.test.staff.MechanicTest {

    protected class MechanicSelector implements ResultSetHandler<Mechanic> {

        @Override
        public Mechanic handle(TransformingResultSet resultSet) throws SQLException {
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
            return getNewMechanic(resultId, name, salary, hiringDate, skillId, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(Mechanic mechanic) throws Exception {
        DateTime hiringDate = mechanic.getHiringDate();
        Timestamp sqlHiringDate = hiringDate == null ? null : new Timestamp(hiringDate.getMillis());
        String name = mechanic.getName();
        BigDecimal salary = mechanic.getSalary();
        MechanicSkill skill = mechanic.getSkill();
        BigInteger skillId = skill == null ? null : skill.getId();
        BigInteger id = generateId();

        SqlExecutor executor = DBHelper.getExecutor();

        String createEmployeeQuery = "insert into employees (employee_id, name, salary, hiring_date)"
                + " values (?, ?, ?, ?)";
        String createMechanicQuery = "insert into mechanics (employee_id, skill)"
                + " values (?, ?)";

        List employeeParams = new ArrayList(4);
        employeeParams.add(id.longValue());
        employeeParams.add(name);
        employeeParams.add(salary);
        employeeParams.add(sqlHiringDate);

        List mechanicParams = new ArrayList(2);
        mechanicParams.add(id.longValue());
        mechanicParams.add(skillId != null ? skillId.longValue() : null);

        executor.executeUpdate(createEmployeeQuery, employeeParams);
        executor.executeUpdate(createMechanicQuery, mechanicParams);
        return id;
    }

    @Override
    protected Mechanic getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select e.employee_id, e.name, e.salary, e.hiring_date, m.skill"
                + " from employees e join mechanics m on e.employee_id = m.employee_id where m.employee_id = ?";
        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(id.longValue()), new MechanicSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteMechanicsQuery = "delete m, e from mechanics m join employees e on e.employee_id = m.employee_id";
        DBHelper.getExecutor().executeUpdate(deleteMechanicsQuery, null);
    }
}
