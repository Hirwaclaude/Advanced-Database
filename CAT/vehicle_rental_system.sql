
-- VEHICLE RENTAL & MAINTENANCE TRACKING SYSTEM

BEGIN;

-- 1. Create tables
CREATE TABLE Customer (
    CustomerID   SERIAL PRIMARY KEY,
    FullName     VARCHAR(200) NOT NULL,
    LicenseNo    VARCHAR(50) UNIQUE NOT NULL,
    Contact      VARCHAR(50),
    Address      TEXT
);

CREATE TABLE Vehicle (
    VehicleID    SERIAL PRIMARY KEY,
    Model        VARCHAR(100) NOT NULL,
    Make         VARCHAR(100),
    Type         VARCHAR(50) NOT NULL,                    -- e.g. Sedan, SUV
    DailyRate    NUMERIC(10,2) NOT NULL CHECK (DailyRate >= 0),
    Status       VARCHAR(20) NOT NULL DEFAULT 'Available' -- Available, Unavailable, Maintenance, Rented
);

CREATE TABLE Staff (
    StaffID      SERIAL PRIMARY KEY,
    FullName     VARCHAR(200) NOT NULL,
    Role         VARCHAR(100),
    Phone        VARCHAR(50)
);

CREATE TABLE Rental (
    RentalID     SERIAL PRIMARY KEY,
    CustomerID   INT NOT NULL REFERENCES Customer(CustomerID) ON DELETE RESTRICT,
    VehicleID    INT NOT NULL REFERENCES Vehicle(VehicleID) ON DELETE RESTRICT,
    StaffID      INT NOT NULL REFERENCES Staff(StaffID) ON DELETE RESTRICT,
    StartDate    DATE NOT NULL,
    EndDate      DATE NOT NULL CHECK (EndDate >= StartDate),
    TotalCost    NUMERIC(12,2) NOT NULL CHECK (TotalCost >= 0)
);

CREATE TABLE Payment (
    PaymentID    SERIAL PRIMARY KEY,
    RentalID     INT NOT NULL UNIQUE REFERENCES Rental(RentalID) ON DELETE CASCADE, -- 1:1 and cascade delete
    Amount       NUMERIC(12,2) NOT NULL CHECK (Amount >= 0),
    PaymentDate  TIMESTAMP NOT NULL DEFAULT now(),
    Method       VARCHAR(50) NOT NULL
);

CREATE TABLE Maintenance (
    MaintenanceID SERIAL PRIMARY KEY,
    VehicleID     INT NOT NULL REFERENCES Vehicle(VehicleID) ON DELETE CASCADE, -- cascade delete when vehicle removed
    StaffID       INT NOT NULL REFERENCES Staff(StaffID) ON DELETE RESTRICT,
    Date          DATE NOT NULL,
    Cost          NUMERIC(12,2) NOT NULL CHECK (Cost >= 0),
    Description   TEXT
);

-- Indexes to help queries (optional but recommended)
CREATE INDEX idx_rental_vehicle ON Rental(VehicleID);
CREATE INDEX idx_rental_dates ON Rental(StartDate, EndDate);
CREATE INDEX idx_maintenance_vehicle ON Maintenance(VehicleID);

-- 2. (CASCADE DELETE already applied above: Payment -> Rental ON DELETE CASCADE, Maintenance -> Vehicle ON DELETE CASCADE)

-- 3. Populate sample data: vehicles, customers, staff, rentals (10), payments, maintenance

-- Staff: 3 staff members
INSERT INTO Staff (FullName, Role, Phone) VALUES
('Alice Mukamana', 'Manager', '0788123456'),
('Ben Nshimiyimana', 'Mechanic', '0788012345'),
('Charles Uwizeyimana', 'Clerk', '0788222333');

-- Vehicles (a variety of types)
INSERT INTO Vehicle (Model, Make, Type, DailyRate, Status) VALUES
('Corolla', 'Toyota', 'Sedan', 30.00, 'Available'),
('Rav4', 'Toyota', 'SUV', 50.00, 'Available'),
('Hiace', 'Toyota', 'Van', 60.00, 'Available'),
('Civic', 'Honda', 'Sedan', 35.00, 'Available'),
('CRV', 'Honda', 'SUV', 55.00, 'Available');

-- Customers (8 customers)
INSERT INTO Customer (FullName, LicenseNo, Contact, Address) VALUES
('John Doe', 'L-10001', '0788000001', 'Kigali'),
('Jane Smith', 'L-10002', '0788000002', 'Kigali'),
('Eric K.', 'L-10003', '0788000003', 'Musanze'),
('Gloria T.', 'L-10004', '0788000004', 'Huye'),
('Sam P.', 'L-10005', '0788000005', 'Kigali'),
('Martha B.', 'L-10006', '0788000006', 'Rwamagana'),
('Kevin R.', 'L-10007', '0788000007', 'Kigali'),
('Alice G.', 'L-10008', '0788000008', 'Rubavu');

-- Rentals (10) - careful distribution; set different dates
-- We'll compute TotalCost = (EndDate - StartDate + 1) * DailyRate for realism
INSERT INTO Rental (CustomerID, VehicleID, StaffID, StartDate, EndDate, TotalCost) VALUES
(1, 1, 3, '2025-09-01', '2025-09-05', 5 * 30.00), -- John - Corolla 5 days
(2, 2, 1, '2025-09-03', '2025-09-04', 2 * 50.00), -- Jane - Rav4 2 days
(3, 3, 2, '2025-08-20', '2025-08-25', 6 * 60.00), -- Eric - Hiace 6 days
(4, 4, 3, '2025-09-10', '2025-09-12', 3 * 35.00), -- Gloria - Civic 3 days
(5, 2, 1, '2025-09-15', '2025-09-18', 4 * 50.00), -- Sam - Rav4 4 days
(6, 5, 2, '2025-09-01', '2025-09-03', 3 * 55.00), -- Martha - CRV 3 days
(7, 1, 3, '2025-10-01', '2025-10-07', 7 * 30.00), -- Kevin - Corolla 7 days (active/near future)
(8, 3, 1, '2025-10-04', '2025-10-06', 3 * 60.00), -- Alice G - Hiace 3 days
(1, 4, 3, '2025-10-10', '2025-10-10', 1 * 35.00), -- John day rental Civic
(2, 5, 2, '2025-10-05', '2025-10-12', 8 * 55.00); -- Jane - CRV 8 days

-- Payments: one per rental (Payment.RentalID unique). Link each to the rental above.

INSERT INTO Payment (RentalID, Amount, PaymentDate, Method) VALUES
(1, 150.00, '2025-08-31 09:00', 'Card'),
(2, 100.00, '2025-09-03 10:00', 'Cash'),
(3, 360.00, '2025-08-20 08:00', 'Card'),
(4, 105.00, '2025-09-10 09:10', 'MobileMoney'),
(5, 200.00, '2025-09-15 14:00', 'Card'),
(6, 165.00, '2025-09-01 16:00', 'Cash'),
(7, 210.00, '2025-09-30 12:00', 'Card'),
(8, 180.00, '2025-10-03 11:00', 'Card'),
(9, 35.00,  '2025-10-10 09:00', 'Cash'),
(10, 440.00,'2025-10-04 13:00', 'Card');

-- Maintenance entries (some vehicles)
INSERT INTO Maintenance (VehicleID, StaffID, Date, Cost, Description) VALUES
(1, 2, '2025-08-15', 120.00, 'Oil change & brakes check'),
(2, 2, '2025-09-20', 300.00, 'Transmission service'),
(3, 2, '2025-09-05', 200.00, 'Tire replacement'),
(4, 2, '2025-10-01', 150.00, 'Battery and service'),
(5, 2, '2025-09-25', 250.00, 'Full service');

COMMIT;

-- 4. Retrieve vehicle utilization rates and total revenue by vehicle type.

-- 4A: Compute window (single values)
WITH window AS (
    SELECT MIN(StartDate) AS min_start, MAX(EndDate) AS max_end
    FROM Rental
), rented_days AS (
    SELECT
      r.VehicleID,
      SUM((r.EndDate - r.StartDate) + 1) AS total_rented_days
    FROM Rental r
    GROUP BY r.VehicleID
)
SELECT
  v.VehicleID,
  v.Make || ' ' || v.Model AS vehicle,
  v.Type,
  COALESCE(rd.total_rented_days, 0) AS total_rented_days,
  ( (COALESCE(rd.total_rented_days,0))::numeric /
    ( (window.max_end - window.min_start) + 1 ) * 100
  )::numeric(5,2) AS utilization_pct
FROM Vehicle v
LEFT JOIN rented_days rd ON rd.VehicleID = v.VehicleID
CROSS JOIN window
ORDER BY v.VehicleID;

-- 4B: Total revenue by vehicle type (sum of payments related to rentals)
SELECT
  v.Type,
  COUNT(r.RentalID) AS rentals_count,
  SUM(p.Amount) AS total_revenue
FROM Vehicle v
JOIN Rental r ON r.VehicleID = v.VehicleID
JOIN Payment p ON p.RentalID = r.RentalID
GROUP BY v.Type
ORDER BY total_revenue DESC;

-- 5. Update a maintenance record and check its effect on vehicle availability.

BEGIN;
-- Update maintenance record
UPDATE Maintenance
SET Date = CURRENT_DATE, Cost = Cost + 20, Description = Description || ' (updated)'
WHERE MaintenanceID = 3
RETURNING *;

-- Optionally mark vehicle status to 'Maintenance' because maintenance scheduled today.
UPDATE Vehicle
SET Status = 'Maintenance'
WHERE VehicleID = (SELECT VehicleID FROM Maintenance WHERE MaintenanceID = 3)
AND (SELECT Date FROM Maintenance WHERE MaintenanceID = 3) = CURRENT_DATE;

COMMIT;

-- Check vehicle availability (status)
SELECT VehicleID, Make, Model, Type, Status FROM Vehicle WHERE VehicleID = (SELECT VehicleID FROM Maintenance WHERE MaintenanceID = 3);

-- 6. Identify the staff who managed the highest total maintenance cost.
SELECT
  s.StaffID,
  s.FullName,
  SUM(m.Cost) AS total_maintenance_cost
FROM Staff s
JOIN Maintenance m ON m.StaffID = s.StaffID
GROUP BY s.StaffID, s.FullName
ORDER BY total_maintenance_cost DESC
LIMIT 1;

-- 7. Create a view summarizing revenue and maintenance costs per vehicle.

CREATE OR REPLACE VIEW vehicle_financials AS
SELECT
  v.VehicleID,
  v.Make,
  v.Model,
  v.Type,
  COALESCE(SUM(p.Amount),0) AS total_revenue,
  COALESCE((SELECT SUM(m2.Cost) FROM Maintenance m2 WHERE m2.VehicleID = v.VehicleID),0) AS total_maintenance_cost,
  COALESCE(SUM(p.Amount),0) - COALESCE((SELECT SUM(m2.Cost) FROM Maintenance m2 WHERE m2.VehicleID = v.VehicleID),0) AS net
FROM Vehicle v
LEFT JOIN Rental r ON r.VehicleID = v.VehicleID
LEFT JOIN Payment p ON p.RentalID = r.RentalID
GROUP BY v.VehicleID, v.Make, v.Model, v.Type
ORDER BY total_revenue DESC;

-- 8. Implement trigger that flags vehicles as "Unavailable" during active rentals.

-- Create trigger function
CREATE OR REPLACE FUNCTION trg_rental_vehicle_status()
RETURNS TRIGGER AS $$
DECLARE
    v_id INT;
    has_active INT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        -- Mark vehicle as Unavailable (rented)
        v_id := NEW.VehicleID;
        UPDATE Vehicle SET Status = 'Unavailable' WHERE VehicleID = v_id;
        RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        -- If vehicle changed, clear old vehicle if no other active rental exists
        IF OLD.VehicleID IS DISTINCT FROM NEW.VehicleID THEN
            -- For OLD vehicle: if no active rentals exist for that vehicle, mark available
            SELECT COUNT(*) INTO has_active FROM Rental
            WHERE VehicleID = OLD.VehicleID
              AND (CURRENT_DATE BETWEEN StartDate AND EndDate)
              AND RentalID <> OLD.RentalID;
            IF has_active = 0 THEN
                UPDATE Vehicle SET Status = 'Available' WHERE VehicleID = OLD.VehicleID;
            END IF;
            -- For NEW vehicle: mark unavailable
            UPDATE Vehicle SET Status = 'Unavailable' WHERE VehicleID = NEW.VehicleID;
        ELSE
            -- Same vehicle but dates might have changed: if NEW dates cover today mark Unavailable else if no active rentals mark Available
            IF (CURRENT_DATE BETWEEN NEW.StartDate AND NEW.EndDate) THEN
                UPDATE Vehicle SET Status = 'Unavailable' WHERE VehicleID = NEW.VehicleID;
            ELSE
                SELECT COUNT(*) INTO has_active FROM Rental
                WHERE VehicleID = NEW.VehicleID
                  AND (CURRENT_DATE BETWEEN StartDate AND EndDate)
                  AND RentalID <> NEW.RentalID;
                IF has_active = 0 THEN
                    UPDATE Vehicle SET Status = 'Available' WHERE VehicleID = NEW.VehicleID;
                END IF;
            END IF;
        END IF;
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        -- For deleted rental, check if vehicle still has active rentals covering today
        v_id := OLD.VehicleID;
        SELECT COUNT(*) INTO has_active FROM Rental
        WHERE VehicleID = v_id
          AND (CURRENT_DATE BETWEEN StartDate AND EndDate)
          AND RentalID <> OLD.RentalID;
        IF has_active = 0 THEN
            UPDATE Vehicle SET Status = 'Available' WHERE VehicleID = v_id;
        END IF;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on Rental
DROP TRIGGER IF EXISTS rental_vehicle_status_trg ON Rental;
CREATE TRIGGER rental_vehicle_status_trg
AFTER INSERT OR UPDATE OR DELETE ON Rental
FOR EACH ROW EXECUTE FUNCTION trg_rental_vehicle_status();

-- Demonstration queries 

-- Show vehicles and statuses
SELECT VehicleID, Make, Model, Type, DailyRate, Status FROM Vehicle ORDER BY VehicleID;

-- Show vehicle_financials view
SELECT * FROM vehicle_financials;

-- End of script
