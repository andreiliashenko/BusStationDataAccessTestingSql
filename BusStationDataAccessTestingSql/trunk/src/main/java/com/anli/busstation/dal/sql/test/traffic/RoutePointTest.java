package com.anli.busstation.dal.sql.test.traffic;

import com.anli.busstation.dal.interfaces.entities.geography.Station;
import com.anli.busstation.dal.interfaces.entities.traffic.RoutePoint;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class RoutePointTest extends com.anli.busstation.dal.test.traffic.RoutePointTest {

    protected class RoutePointSelector implements ResultSetHandler<RoutePoint> {

        @Override
        public RoutePoint handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            BigDecimal bdStationId = resultSet.getValue(2, BigDecimal.class);
            BigInteger stationId = bdStationId != null ? bdStationId.toBigInteger() : null;
            return getNewRoutePoint(resultId, stationId, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(RoutePoint routePoint) throws Exception {
        Station station = routePoint.getStation();
        BigDecimal stationId = station != null ? new BigDecimal(station.getId()) : null;
        BigInteger id = generateId();
        String createQuery = "insert into route_points (route_point_id, station) values(?, ?)";
        List createParams = new ArrayList(2);
        createParams.add(new BigDecimal(id));
        createParams.add(stationId);
        DBHelper.getExecutor().executeUpdate(createQuery, createParams);
        return id;
    }

    @Override
    protected RoutePoint getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select route_point_id, station "
                + "from route_points where route_point_id = ?";
        return DBHelper.getExecutor().executeSelect(selectQuery,
                Collections.singletonList(new BigDecimal(id)), new RoutePointSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteRoutePointsQuery = "delete from route_points";
        DBHelper.getExecutor().executeUpdate(deleteRoutePointsQuery, null);
    }
}
