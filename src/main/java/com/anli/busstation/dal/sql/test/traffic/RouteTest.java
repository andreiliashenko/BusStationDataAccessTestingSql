package com.anli.busstation.dal.sql.test.traffic;

import com.anli.busstation.dal.interfaces.entities.traffic.Ride;
import com.anli.busstation.dal.interfaces.entities.traffic.Route;
import com.anli.busstation.dal.interfaces.entities.traffic.RoutePoint;
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

public abstract class RouteTest extends com.anli.busstation.dal.test.traffic.RouteTest {

    protected class RouteSelector implements ResultSetHandler<Route> {

        protected final List<BigInteger> routePoints;
        protected final List<BigInteger> rides;

        public RouteSelector(List<BigInteger> routePoints, List<BigInteger> rides) {
            this.routePoints = routePoints;
            this.rides = rides;
        }

        @Override
        public Route handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            String numCode = resultSet.getValue(2, String.class);
            BigDecimal ticketPrice = resultSet.getValue(3, BigDecimal.class);
            return getNewRoute(resultId, numCode, ticketPrice, routePoints, rides, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(Route route) throws Exception {
        String numCode = route.getNumCode();
        BigDecimal ticketPrice = route.getTicketPrice();
        List<RoutePoint> routePointList = route.getRoutePoints();
        List<Ride> rideList = route.getRides();
        BigInteger id = generateId();
        SqlExecutor executor = DBHelper.getExecutor();
        String createQuery = "insert into routes (route_id, num_code, ticket_price)"
                + " values(?, ?, ?)";
        List params = new ArrayList(3);
        params.add(new BigDecimal(id));
        params.add(numCode);
        params.add(ticketPrice);
        executor.executeUpdate(createQuery, params);

        String linkRoutePointsQuery = "update route_points set route = ?, route_order = ? "
                + "where route_point_id = ?";
        int index = 0;
        for (RoutePoint routePoint : routePointList) {
            index++;
            executor.executeUpdate(linkRoutePointsQuery, Arrays.asList(new BigDecimal(id), index,
                    new BigDecimal(routePoint.getId())));
        }

        String linkRidesQuery = "update rides set route = ?, route_order = ? where ride_id = ?";
        index = 0;
        for (Ride ride : rideList) {
            index++;
            executor.executeUpdate(linkRidesQuery, Arrays.asList(new BigDecimal(id), index,
                    new BigDecimal(ride.getId())));
        }
        return id;
    }

    @Override
    protected Route getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select route_id, num_code, ticket_price"
                + " from routes where route_id = ?";
        String selectRoutePointsQuery = "select route_point_id from route_points "
                + "where route = ? order by route_order";
        String selectRidesQuery = "select ride_id from rides where route = ? order by route_order";
        SqlExecutor executor = DBHelper.getExecutor();
        List<BigInteger> routePointIds = executor.executeSelect(selectRoutePointsQuery,
                Arrays.asList(new BigDecimal(id)), new IdSelector());
        List<BigInteger> rideIds = executor.executeSelect(selectRidesQuery, Arrays.asList(new BigDecimal(id)),
                new IdSelector());
        return executor.executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)),
                new RouteSelector(routePointIds, rideIds));
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteQuery = "delete from routes";
        DBHelper.getExecutor().executeUpdate(deleteQuery, null);
    }
}
