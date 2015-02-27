package com.anli.busstation.dal.sql.test.traffic;

import com.anli.busstation.dal.interfaces.entities.staff.Salesman;
import com.anli.busstation.dal.interfaces.entities.traffic.RidePoint;
import com.anli.busstation.dal.interfaces.entities.traffic.Ticket;
import com.anli.busstation.dal.sql.test.DBHelper;
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

public abstract class TicketTest extends com.anli.busstation.dal.test.traffic.TicketTest {

    protected class TicketSelector implements ResultSetHandler<Ticket> {

        @Override
        public Ticket handle(TransformingResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            BigInteger resultId = resultSet.getValue(1, BigDecimal.class).toBigInteger();
            BigDecimal bdArrPointId = resultSet.getValue(2, BigDecimal.class);
            BigInteger arrPointId = bdArrPointId != null ? bdArrPointId.toBigInteger() : null;
            String customerName = resultSet.getValue(3, String.class);
            BigDecimal bdDepPointId = resultSet.getValue(4, BigDecimal.class);
            BigInteger depPointId = bdDepPointId != null ? bdDepPointId.toBigInteger() : null;
            BigDecimal price = resultSet.getValue(5, BigDecimal.class);
            Timestamp sqlSaleDate = resultSet.getValue(6, Timestamp.class);
            DateTime saleDate = sqlSaleDate != null ? new DateTime(sqlSaleDate.getTime()) : null;
            BigDecimal bdSalesmanId = resultSet.getValue(7, BigDecimal.class);
            BigInteger salesmanId = bdSalesmanId != null ? bdSalesmanId.toBigInteger() : null;
            Integer seat = resultSet.getValue(8, Integer.class);
            Integer intSold = resultSet.getValue(9, Integer.class);
            boolean sold = Integer.valueOf(1).equals(intSold);
            return getNewTicket(resultId, arrPointId, customerName, depPointId, price,
                    saleDate, salesmanId, seat, sold, true);
        }

    }

    @Override
    protected BigInteger createEntityManually(Ticket ticket) throws Exception {
        RidePoint arrivalPoint = ticket.getArrivalPoint();
        BigDecimal arrPointId = arrivalPoint != null ? new BigDecimal(arrivalPoint.getId()) : null;
        String customerName = ticket.getCustomerName();
        RidePoint departurePoint = ticket.getDeparturePoint();
        BigDecimal depPointId = departurePoint != null ? new BigDecimal(departurePoint.getId()) : null;
        BigDecimal price = ticket.getPrice();
        DateTime saleDate = ticket.getSaleDate();
        Timestamp sqlSaleDate = saleDate != null ? new Timestamp(saleDate.getMillis()) : null;
        Salesman salesman = ticket.getSalesman();
        BigDecimal salesmanId = salesman != null ? new BigDecimal(salesman.getId()) : null;
        Integer seat = ticket.getSeat();
        boolean sold = ticket.isSold();
        Integer intSold = sold ? 1 : 0;
        BigInteger id = generateId();
        String createQuery = "insert into tickets(ticket_id, arrival_point, customer_name, departure_point, "
                + "price, sale_date, salesman, seat, is_sold) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        List createParams = new ArrayList(9);
        createParams.add(new BigDecimal(id));
        createParams.add(arrPointId);
        createParams.add(customerName);
        createParams.add(depPointId);
        createParams.add(price);
        createParams.add(sqlSaleDate);
        createParams.add(salesmanId);
        createParams.add(seat);
        createParams.add(intSold);
        DBHelper.getExecutor().executeUpdate(createQuery, createParams);
        return id;
    }

    @Override
    protected Ticket getEntityManually(BigInteger id) throws Exception {
        String createQuery = "select ticket_id, arrival_point, customer_name, departure_point, "
                + "price, sale_date, salesman, seat, is_sold from tickets where ticket_id = ?";
        return DBHelper.getExecutor().executeSelect(createQuery,
                Arrays.asList(new BigDecimal(id)), new TicketSelector());
    }

    @Override
    protected void clearStorageSpace() throws Exception {
        String deleteQuery = "delete from tickets";
        DBHelper.getExecutor().executeUpdate(deleteQuery, null);
    }
}
