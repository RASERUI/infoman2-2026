
CREATE OR REPLACE FUNCTION get_flight_duration(p_flight_id INT)
RETURNS INTERVAL AS $$
DECLARE
    v_departure TIMESTAMP;
    v_arrival TIMESTAMP;
BEGIN
    SELECT departure_time, arrival_time
    INTO v_departure, v_arrival
    FROM flights
    WHERE flight_id = p_flight_id;

    RETURN v_arrival - v_departure;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_price_category(p_flight_id INT)
RETURNS TEXT AS $$
DECLARE
    v_price NUMERIC;
BEGIN
    SELECT base_price INTO v_price
    FROM flights
    WHERE flight_id = p_flight_id;

    IF v_price < 300 THEN
        RETURN 'Budget';
    ELSIF v_price <= 800 THEN
        RETURN 'Standard';
    ELSE
        RETURN 'Premium';
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE book_flight(
    p_passenger_id INT,
    p_flight_id INT,
    p_seat_number VARCHAR
)
AS $$
BEGIN
    INSERT INTO bookings(flight_id, passenger_id, booking_date, seat_number, status)
    VALUES (p_flight_id, p_passenger_id, CURRENT_DATE, p_seat_number, 'Confirmed');

    RAISE NOTICE 'Booking created for passenger % on flight %', p_passenger_id, p_flight_id;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE increase_prices_for_airline(
    p_airline_id INT,
    p_percentage_increase NUMERIC
)
AS $$
DECLARE
    rec_flight RECORD;
BEGIN
    FOR rec_flight IN
        SELECT flight_id, base_price
        FROM flights
        WHERE airline_id = p_airline_id
    LOOP
        UPDATE flights
        SET base_price = base_price * (1 + p_percentage_increase / 100)
        WHERE flight_id = rec_flight.flight_id;
    END LOOP;

    RAISE NOTICE 'Prices updated for airline %', p_airline_id;
END;
$$ LANGUAGE plpgsql;

