package com.anli.busstation.dal.sql.test.geography;

import com.anli.busstation.dal.interfaces.entities.geography.Region;
import com.anli.busstation.dal.interfaces.entities.geography.Station;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.busstation.dal.sql.test.IdSelector;
import com.anli.sqlexecution.execution.SqlExecutor;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class RegionTest extends com.anli.busstation.dal.test.geography.RegionTest {

    protected class RegionSelector implements ResultSetHandler<Region> {

        protected final List<BigInteger> stationList;

        RegionSelector(List<BigInteger> stations) {
            this.stationList = stations;
        }

        @Override
        public Region handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = BigInteger.valueOf(resultSet.getValue(1, Long.class));
            Integer code = resultSet.getValue(2, Integer.class);
            String name = resultSet.getValue(3, String.class);
            return getNewRegion(resultId, code, name, stationList, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(Region region) throws Exception {
        List<Station> stationList = region.getStations();
        Integer code = region.getCode();
        String name = region.getName();
        BigInteger id = generateId();

        SqlExecutor executor = DBHelper.getExecutor();

        String createQuery = "insert into regions (region_id, num_code, name)"
                + " values(?, ?, ?)";
        List createParams = new ArrayList(3);
        createParams.add(id.longValue());
        createParams.add(code);
        createParams.add(name);
        executor.executeUpdate(createQuery, createParams);

        String linkStationQuery = "update stations set region = ?, region_order = ? where station_id = ?";
        int index = 0;
        for (Station station : stationList) {
            index++;
            executor.executeUpdate(linkStationQuery, Arrays.asList(id.longValue(), index, station.getId().longValue()));
        }
        return id;
    }

    @Override
    protected Region getEntityManually(BigInteger id) throws Exception {
        String selectStationsQuery = "select station_id from stations where region = ? order by region_order";
        String selectQuery = "select region_id, num_code, name"
                + " from regions where region_id = ?";
        SqlExecutor executor = DBHelper.getExecutor();
        List<BigInteger> stationList = executor.executeSelect(selectStationsQuery,
                Arrays.asList(id.longValue()), new IdSelector());
        return executor.executeSelect(selectQuery, Arrays.asList(id.longValue()), new RegionSelector(stationList));
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteRegionsQuery = "delete from regions";
        DBHelper.getExecutor().executeUpdate(deleteRegionsQuery, null);
    }
}
