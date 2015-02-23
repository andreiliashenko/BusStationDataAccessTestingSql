package com.anli.busstation.dal.sql.test.traffic;

import com.anli.busstation.dal.interfaces.entities.traffic.RidePoint;
import com.anli.busstation.dal.interfaces.entities.traffic.RoutePoint;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.joda.time.DateTime;

public abstract class RidePointTest extends com.anli.busstation.dal.test.traffic.RidePointTest {

    protected class RidePointSelector implements ResultSetHandler<RidePoint> {

        @Override
        public RidePoint handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            Timestamp sqlArrivalTime = resultSet.getValue(2, Timestamp.class);
            DateTime arrivalTime = sqlArrivalTime != null ? new DateTime(sqlArrivalTime.getTime()) : null;
            Timestamp sqlDepartureTime = resultSet.getValue(3, Timestamp.class);
            DateTime departureTime = sqlDepartureTime != null ? new DateTime(sqlDepartureTime.getTime()) : null;
            BigDecimal bdRoutePointId = resultSet.getValue(4, BigDecimal.class);
            BigInteger routePointId = bdRoutePointId != null ? bdRoutePointId.toBigInteger() : null;
            return getNewRidePoint(resultId, arrivalTime, departureTime, routePointId, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(RidePoint ridePoint) throws Exception {
        RoutePoint routePoint = ridePoint.getRoutePoint();
        BigDecimal routePointId = routePoint != null ? new BigDecimal(routePoint.getId()) : null;
        DateTime dtArrivalTime = ridePoint.getArrivalTime();
        Timestamp arrivalTime = dtArrivalTime != null ? new Timestamp(dtArrivalTime.getMillis()) : null;
        DateTime dtDepartureTime = ridePoint.getDepartureTime();
        Timestamp departureTime = dtDepartureTime != null ? new Timestamp(dtDepartureTime.getMillis()) : null;
        BigInteger id = generateId();
        String createQuery = "insert into ride_points (ride_point_id, arrival_time, departure_time, route_point) "
                + "values(?, ?, ?, ?)";
        List createParams = new ArrayList(4);
        createParams.add(new BigDecimal(id));
        createParams.add(arrivalTime);
        createParams.add(departureTime);
        createParams.add(routePointId);
        DBHelper.getExecutor().executeUpdate(createQuery, createParams);
        return id;
    }

    @Override
    protected RidePoint getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select ride_point_id, arrival_time, departure_time, route_point"
                + " from ride_points where ride_point_id = ?";
        return DBHelper.getExecutor().executeSelect(selectQuery,
                Collections.singletonList(new BigDecimal(id)), new RidePointSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteRidePointsQuery = "delete from ride_points";
        DBHelper.getExecutor().executeUpdate(deleteRidePointsQuery, null);
    }
}
