package com.anli.busstation.dal.sql.test.staff;

import com.anli.busstation.dal.interfaces.entities.staff.MechanicSkill;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class MechanicSkillTest extends com.anli.busstation.dal.test.staff.MechanicSkillTest {

    protected class MechanicSkillSelector implements ResultSetHandler<MechanicSkill> {

        @Override
        public MechanicSkill handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger id = BigInteger.valueOf(resultSet.getValue(1, Long.class));
            String name = resultSet.getValue(2, String.class);
            Integer maxDiffLevel = resultSet.getValue(3, Integer.class);

            return getNewMechanicSkill(id, name, maxDiffLevel);
        }
    }

    @Override
    protected BigInteger createEntityManually(MechanicSkill skill) throws Exception {
        String name = skill.getName();
        Integer maxDiffLevel = skill.getMaxDiffLevel();
        BigInteger id = generateId();

        String createQuery = "insert into mechanic_skills (skill_id, name, max_diff_level) values(?, ?, ?)";
        List createParams = new ArrayList(3);
        createParams.add(id.longValue());
        createParams.add(name);
        createParams.add(maxDiffLevel);

        DBHelper.getExecutor().executeUpdate(createQuery, createParams);
        return id;
    }

    @Override
    protected MechanicSkill getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select skill_id, name, max_diff_level from mechanic_skills where skill_id = ?";
        return DBHelper.getExecutor()
                .executeSelect(selectQuery, Arrays.asList(id.longValue()), new MechanicSkillSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteQuery = "delete from mechanic_skills";
        DBHelper.getExecutor().executeUpdate(deleteQuery, null);
    }
}
