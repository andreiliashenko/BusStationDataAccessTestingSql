package com.anli.busstation.dal.sql.test;

import com.anli.busstation.dal.interfaces.entities.BSEntity;
import com.anli.busstation.dal.interfaces.entities.vehicles.Bus;
import com.anli.busstation.dal.interfaces.entities.staff.DriverSkill;
import com.anli.busstation.dal.interfaces.entities.staff.Employee;
import com.anli.busstation.dal.interfaces.entities.vehicles.GasLabel;
import com.anli.busstation.dal.interfaces.entities.vehicles.Model;
import com.anli.busstation.dal.interfaces.entities.geography.Region;
import com.anli.busstation.dal.interfaces.entities.traffic.Ride;
import com.anli.busstation.dal.interfaces.entities.traffic.RidePoint;
import com.anli.busstation.dal.interfaces.entities.traffic.RideRoad;
import com.anli.busstation.dal.interfaces.entities.geography.Road;
import com.anli.busstation.dal.interfaces.entities.traffic.Route;
import com.anli.busstation.dal.interfaces.entities.traffic.RoutePoint;
import com.anli.busstation.dal.interfaces.entities.geography.Station;
import com.anli.busstation.dal.interfaces.entities.maintenance.TechnicalAssignment;
import com.anli.busstation.dal.interfaces.entities.staff.MechanicSkill;
import com.anli.busstation.dal.interfaces.entities.vehicles.TechnicalState;
import com.anli.busstation.dal.interfaces.entities.traffic.Ticket;
import com.anli.busstation.dal.test.FixtureCreator;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

public abstract class SqlFixtureCreator extends FixtureCreator {

    protected abstract void setObjectId(BSEntity entity, BigInteger id);

    @Override
    protected void setIdManually(BSEntity entity, BigInteger id) {
        String table = getTable(entity);
        String idColumn = getIdColumn(entity);
        String updateQuery = "update " + table + " set " + idColumn + " = ? where " + idColumn + " = ?";
        DBHelper.getExecutor().executeUpdate(updateQuery, Arrays.asList(new BigDecimal(id), new BigDecimal(entity.getId())));
        setObjectId(entity, id);
    }

    protected String getTable(BSEntity entity) {
        if (entity instanceof Bus) {
            return "buses";
        }
        if (entity instanceof Employee) {
            return "employees";
        }
        if (entity instanceof DriverSkill) {
            return "driver_skills";
        }
        if (entity instanceof GasLabel) {
            return "gas_labels";
        }
        if (entity instanceof MechanicSkill) {
            return "mechanic_skills";
        }
        if (entity instanceof Model) {
            return "models";
        }
        if (entity instanceof Region) {
            return "regions";
        }
        if (entity instanceof Ride) {
            return "rides";
        }
        if (entity instanceof RidePoint) {
            return "ride_points";
        }
        if (entity instanceof RideRoad) {
            return "ride_roads";
        }
        if (entity instanceof Road) {
            return "roads";
        }
        if (entity instanceof Route) {
            return "routes";
        }
        if (entity instanceof RoutePoint) {
            return "route_point";
        }
        if (entity instanceof Station) {
            return "stations";
        }
        if (entity instanceof TechnicalAssignment) {
            return "technical_assignments";
        }
        if (entity instanceof TechnicalState) {
            return "technical_states";
        }
        if (entity instanceof Ticket) {
            return "tickets";
        }
        throw new RuntimeException(entity.getClass().getCanonicalName());
    }

    protected String getIdColumn(BSEntity entity) {
        if (entity instanceof Bus) {
            return "bus_id";
        }
        if (entity instanceof DriverSkill) {
            return "skill_id";
        }
        if (entity instanceof Employee) {
            return "employee_id";
        }
        if (entity instanceof GasLabel) {
            return "label_id";
        }
        if (entity instanceof MechanicSkill) {
            return "skill_id";
        }
        if (entity instanceof Model) {
            return "model_id";
        }
        if (entity instanceof Region) {
            return "region_id";
        }
        if (entity instanceof Ride) {
            return "ride_id";
        }
        if (entity instanceof RidePoint) {
            return "ride_point_id";
        }
        if (entity instanceof RideRoad) {
            return "ride_road_id";
        }
        if (entity instanceof Road) {
            return "road_id";
        }
        if (entity instanceof Route) {
            return "route_id";
        }
        if (entity instanceof RoutePoint) {
            return "route_point_id";
        }
        if (entity instanceof Station) {
            return "station_id";
        }
        if (entity instanceof TechnicalAssignment) {
            return "assignment_id";
        }
        if (entity instanceof TechnicalState) {
            return "state_id";
        }
        if (entity instanceof Ticket) {
            return "ticket_id";
        }
        throw new RuntimeException(entity.getClass().getCanonicalName());
    }
}
