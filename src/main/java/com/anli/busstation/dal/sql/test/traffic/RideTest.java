package com.anli.busstation.dal.sql.test.traffic;

import com.anli.busstation.dal.interfaces.entities.traffic.Ride;
import com.anli.busstation.dal.interfaces.entities.traffic.RidePoint;
import com.anli.busstation.dal.interfaces.entities.traffic.RideRoad;
import com.anli.busstation.dal.interfaces.entities.traffic.Ticket;
import com.anli.busstation.dal.interfaces.entities.vehicles.Bus;
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

public abstract class RideTest extends com.anli.busstation.dal.test.traffic.RideTest {

    protected class RideSelector implements ResultSetHandler<Ride> {

        protected final List<BigInteger> ridePoints;
        protected final List<BigInteger> rideRoads;
        protected final List<BigInteger> tickets;

        public RideSelector(List<BigInteger> ridePoints, List<BigInteger> rideRoads,
                List<BigInteger> tickets) {
            this.ridePoints = ridePoints;
            this.rideRoads = rideRoads;
            this.tickets = tickets;
        }

        @Override
        public Ride handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            BigDecimal bdBusId = resultSet.getValue(2, BigDecimal.class);
            BigInteger busId = bdBusId != null ? bdBusId.toBigInteger() : null;
            return getNewRide(resultId, busId, ridePoints, rideRoads, tickets, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(Ride ride) throws Exception {
        Bus bus = ride.getBus();
        BigDecimal busId = bus != null ? new BigDecimal(bus.getId()) : null;
        List<RidePoint> ridePointList = ride.getRidePoints();
        List<RideRoad> rideRoadList = ride.getRideRoads();
        List<Ticket> ticketList = ride.getTickets();
        BigInteger id = generateId();
        SqlExecutor executor = DBHelper.getExecutor();
        String createQuery = "insert into rides (ride_id, bus)"
                + " values(?, ?)";
        List params = new ArrayList(2);
        params.add(new BigDecimal(id));
        params.add(busId);
        executor.executeUpdate(createQuery, params);

        String linkRidePointsQuery = "update ride_points set ride = ?, ride_order = ? "
                + "where ride_point_id = ?";
        int index = 0;
        for (RidePoint ridePoint : ridePointList) {
            index++;
            executor.executeUpdate(linkRidePointsQuery, Arrays.asList(new BigDecimal(id), index,
                    new BigDecimal(ridePoint.getId())));
        }

        String linkTicketsQuery = "update tickets set ride = ?, ride_order = ? where ticket_id = ?";
        index = 0;
        for (Ticket ticket : ticketList) {
            index++;
            executor.executeUpdate(linkTicketsQuery, Arrays.asList(new BigDecimal(id), index,
                    new BigDecimal(ticket.getId())));
        }

        String linkRideRoadsQuery = "update ride_roads set ride = ?, ride_order = ? "
                + "where ride_road_id = ?";
        index = 0;
        for (RideRoad rideRoad : rideRoadList) {
            index++;
            executor.executeUpdate(linkRideRoadsQuery, Arrays.asList(new BigDecimal(id), index,
                    new BigDecimal(rideRoad.getId())));
        }
        return id;
    }

    @Override
    protected Ride getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select ride_id, bus"
                + " from rides where ride_id = ?";
        String selectRidePointsQuery = "select ride_point_id from ride_points "
                + "where ride = ? order by ride_order";
        String selectRideRoadsQuery = "select ride_road_id from ride_roads "
                + "where ride = ? order by ride_order";
        String selectTicketsQuery = "select ticket_id from tickets "
                + "where ride = ? order by ride_order";
        SqlExecutor executor = DBHelper.getExecutor();
        List<BigInteger> ridePointIds = executor.executeSelect(selectRidePointsQuery,
                Arrays.asList(new BigDecimal(id)), new IdSelector());
        List<BigInteger> rideRoadIds = executor.executeSelect(selectRideRoadsQuery,
                Arrays.asList(new BigDecimal(id)), new IdSelector());
        List<BigInteger> ticketIds = executor.executeSelect(selectTicketsQuery,
                Arrays.asList(new BigDecimal(id)), new IdSelector());
        return executor.executeSelect(selectQuery, Arrays.asList(new BigDecimal(id)),
                new RideSelector(ridePointIds, rideRoadIds, ticketIds));
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteQuery = "delete from rides";
        DBHelper.getExecutor().executeUpdate(deleteQuery, null);
    }
}
