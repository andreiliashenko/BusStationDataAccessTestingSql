package com.anli.busstation.dal.sql.test.geography;

import com.anli.busstation.dal.interfaces.entities.geography.Road;
import com.anli.busstation.dal.interfaces.entities.geography.Station;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.execution.SqlExecutor;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class RoadTest extends com.anli.busstation.dal.test.geography.RoadTest {

    protected class RoadSelector implements ResultSetHandler<Road> {

        @Override
        public Road handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            BigDecimal bdAStationId = resultSet.getValue(2, BigDecimal.class);
            BigInteger aStationId = bdAStationId != null ? bdAStationId.toBigInteger() : null;
            Integer length = resultSet.getValue(3, Integer.class);
            BigDecimal ridePrice = resultSet.getValue(4, BigDecimal.class);
            BigDecimal bdZStationId = resultSet.getValue(5, BigDecimal.class);
            BigInteger zStationId = bdZStationId != null ? bdZStationId.toBigInteger() : null;
            return getNewRoad(resultId, aStationId, length, ridePrice, zStationId, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(Road road) throws Exception {
        Station aStation = road.getAStation();
        BigDecimal aStationId = aStation != null ? new BigDecimal(aStation.getId()) : null;
        Integer length = road.getLength();
        BigDecimal ridePrice = road.getRidePrice();
        Station zStation = road.getZStation();
        BigDecimal zStationId = zStation != null ? new BigDecimal(zStation.getId()) : null;
        BigInteger id = generateId();
        SqlExecutor executor = DBHelper.getExecutor();
        String createQuery = "insert into roads (road_id, a_station, length, ride_price, z_station)"
                + " values(?, ?, ?, ?, ?)";
        List createParams = new ArrayList(5);
        createParams.add(new BigDecimal(id));
        createParams.add(aStationId);
        createParams.add(length);
        createParams.add(ridePrice);
        createParams.add(zStationId);
        executor.executeUpdate(createQuery, createParams);
        return id;
    }

    @Override
    protected Road getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select road_id, a_station, length, ride_price, z_station"
                + " from roads where road_id = ?";
        SqlExecutor executor = DBHelper.getExecutor();
        return executor.executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)), new RoadSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteRoadsQuery = "delete from roads";
        DBHelper.getExecutor().executeUpdate(deleteRoadsQuery, null);
    }

}
