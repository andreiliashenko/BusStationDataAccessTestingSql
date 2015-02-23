package com.anli.busstation.dal.sql.test.traffic;

import com.anli.busstation.dal.interfaces.entities.geography.Road;
import com.anli.busstation.dal.interfaces.entities.staff.Driver;
import com.anli.busstation.dal.interfaces.entities.traffic.RideRoad;
import com.anli.busstation.dal.sql.test.DBHelper;
import com.anli.sqlexecution.handling.ResultSetHandler;
import com.anli.sqlexecution.handling.TransformingResultSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class RideRoadTest extends com.anli.busstation.dal.test.traffic.RideRoadTest {

    protected class RideRoadSelector implements ResultSetHandler<RideRoad> {

        @Override
        public RideRoad handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            BigDecimal bdDriverId = resultSet.getValue(2, BigDecimal.class);
            BigInteger driverId = bdDriverId != null ? bdDriverId.toBigInteger() : null;
            BigDecimal bdRoadId = resultSet.getValue(3, BigDecimal.class);
            BigInteger roadId = bdRoadId != null ? bdRoadId.toBigInteger() : null;
            return getNewRideRoad(resultId, driverId, roadId, true);
        }
    }

    @Override
    protected BigInteger createEntityManually(RideRoad rideRoad) throws Exception {
        Driver driver = rideRoad.getDriver();
        BigDecimal driverId = driver != null ? new BigDecimal(driver.getId()) : null;
        Road road = rideRoad.getRoad();
        BigDecimal roadId = road != null ? new BigDecimal(road.getId()) : null;
        BigInteger id = generateId();
        String createQuery = "insert into ride_roads (ride_road_id, driver, road) "
                + "values(?, ?, ?)";
        List createParams = new ArrayList(3);
        createParams.add(new BigDecimal(id));
        createParams.add(driverId);
        createParams.add(roadId);
        DBHelper.getExecutor().executeUpdate(createQuery, createParams);
        return id;
    }

    @Override
    protected RideRoad getEntityManually(BigInteger id) throws Exception {
        String selectQuery = "select ride_road_id, driver, road "
                + " from ride_roads where ride_road_id = ?";
        return DBHelper.getExecutor().executeSelect(selectQuery,
                Collections.singletonList(new BigDecimal(id)), new RideRoadSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteRideRoadsQuery = "delete from ride_roads";
        DBHelper.getExecutor().executeUpdate(deleteRideRoadsQuery, null);
    }
}
