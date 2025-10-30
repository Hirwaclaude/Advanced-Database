Advanced Database Examination Solutions
Student: HIRWA Claude
Registration Number: 221028996
Course: Advanced Database
University of Rwanda



A1: Fragment & Recombine Main Fact (≤10 rows)
A.1.1
Create horizontally fragmented tables Rental_A on Node_A and Rental_B on Node_B using a deterministic rule (HASH or RANGE on a natural key).

Solution :
Below are two created horizontally fragmented tables

SQL:

-- Create fragmented tables on both nodes
CREATE TABLE Rental_A (
    rental_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    vehicle_id INTEGER,
    rental_date DATE,
    return_date DATE,
    amount DECIMAL(10,2),
    status VARCHAR(20)
);

CREATE TABLE Rental_B (
    rental_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    vehicle_id INTEGER,
    rental_date DATE,
    return_date DATE,
    amount DECIMAL(10,2),
    status VARCHAR(20)
);

A.1.2
Insert a TOTAL of ≤10 committed rows split across the two fragments (e.g., 5 on Node_A and 5 on Node_B). Reuse these rows for all remaining tasks.

Solution :
Below are the insert 5 rows into each fragment using rental_id parity queries odd numbers in Rental_A and even numbers in Rental_B.

SQL:

-- Insert 5 rows into Rental_A (odd rental_ids)
INSERT INTO Rental_A VALUES (1, 101, 201, '2024-01-01', '2024-01-05', 5000, 'COMPLETED');
INSERT INTO Rental_A VALUES (3, 103, 203, '2024-01-03', '2024-01-08', 7500, 'COMPLETED');
INSERT INTO Rental_A VALUES (5, 105, 205, '2024-01-05', NULL, 6000, 'ACTIVE');
INSERT INTO Rental_A VALUES (7, 107, 207, '2024-01-07', '2024-01-10', 4500, 'COMPLETED');
INSERT INTO Rental_A VALUES (9, 109, 209, '2024-01-09', NULL, 5500, 'ACTIVE');

-- Insert 5 rows into Rental_B (even rental_ids)
INSERT INTO Rental_B VALUES (2, 102, 202, '2024-01-02', '2024-01-06', 6500, 'COMPLETED');
INSERT INTO Rental_B VALUES (4, 104, 204, '2024-01-04', '2024-01-09', 8000, 'COMPLETED');
INSERT INTO Rental_B VALUES (6, 106, 206, '2024-01-06', NULL, 7000, 'ACTIVE');
INSERT INTO Rental_B VALUES (8, 108, 208, '2024-01-08', '2024-01-12', 4800, 'COMPLETED');
INSERT INTO Rental_B VALUES (10, 110, 210, '2024-01-10', NULL, 5200, 'ACTIVE');\


A.1.3
On Node_A, create view Rental_ALL as UNION ALL of Rental_A and Rental_B@proj_link.

Solution :
Below is the Created unified view that combines both fragments using UNION ALL to provide transparent access to all rental data.

SQL:

-- Create foreign data wrapper for cross-node access
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create server connection to Node_B
CREATE SERVER node_b FOREIGN DATA WRAPPER postgres_fdw 
OPTIONS (host 'node-b-server', dbname 'rental_db', port '5432');

-- Create user mapping for authentication
CREATE USER MAPPING FOR CURRENT_USER SERVER node_b 
OPTIONS (user 'postgres', password 'password');

-- Import Rental_B as foreign table
IMPORT FOREIGN SCHEMA public LIMIT TO (Rental_B) 
FROM SERVER node_b INTO public;

-- Create UNION ALL view
CREATE VIEW Rental_ALL AS
SELECT * FROM Rental_A
UNION ALL
SELECT * FROM Rental_B;


A.1.4
Validate with COUNT(*) and a checksum on a key column (e.g., SUM(MOD(primary_key,97))): results must match fragments vs Rental_ALL.

Solution:

Below is the Validated data consistency by comparing row counts and checksum values between individual fragments and the combined view.

SQL:

-- Count validation
SELECT 'Rental_A' as fragment, COUNT(*) as row_count FROM Rental_A
UNION ALL
SELECT 'Rental_B' as fragment, COUNT(*) as row_count FROM Rental_B
UNION ALL  
SELECT 'Rental_ALL' as fragment, COUNT(*) as row_count FROM Rental_ALL;

-- Checksum validation using rental_id
SELECT 'Rental_A' as fragment, SUM(rental_id % 97) as checksum FROM Rental_A
UNION ALL
SELECT 'Rental_B' as fragment, SUM(rental_id % 97) as checksum FROM Rental_B
UNION ALL
SELECT 'Rental_ALL' as fragment, SUM(rental_id % 97) as checksum FROM Rental_ALL;

-- Additional validation with amount checksum
SELECT 'Rental_A' as fragment, SUM(amount) as total_amount FROM Rental_A
UNION ALL
SELECT 'Rental_B' as fragment, SUM(amount) as total_amount FROM Rental_B
UNION ALL
SELECT 'Rental_ALL' as fragment, SUM(amount) as total_amount FROM Rental_ALL;


A2: Database Link & Cross-Node Join (3–10 rows result)


A.2.1
From Node_A, create database link 'proj_link' to Node_B.

Solution :
Below i established database link connection to Node_B using PostgreSQL Foreign Data Wrapper for cross-node queries.

SQL:
-- Create foreign data wrapper extension (if not exists)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create server connection to Node_B
CREATE SERVER proj_link FOREIGN DATA WRAPPER postgres_fdw 
OPTIONS (host '192.168.1.100', dbname 'rental_db_node_b', port '5432');

-- Create user mapping for authentication
CREATE USER MAPPING FOR CURRENT_USER SERVER proj_link 
OPTIONS (user 'node_b_user', password 'node_b_password');

-- Import required tables from Node_B
IMPORT FOREIGN SCHEMA public LIMIT TO (Vehicle, Customer) 
FROM SERVER proj_link INTO public;


A.2.2
Run remote SELECT on Vehicle@proj_link showing up to 5 sample rows.

Solution :
Execute remote query to fetch sample vehicle data from Node_B through the established database link.

SQL:

-- Remote SELECT from Vehicle table on Node_B
SELECT * FROM Vehicle LIMIT 5;

-- Alternative with specific columns for better readability
SELECT vehicle_id, vehicle_model, vehicle_type, daily_rate 
FROM Vehicle 
ORDER BY vehicle_id 
LIMIT 5;

-- Count verification of remote data
SELECT COUNT(*) as total_vehicles FROM Vehicle;

A.2.3
Run a distributed join: local Rental_A (or base Rental) joined with remote Customer@proj_link returning between 3 and 10 rows total; include selective predicates to stay within the row budget.

Solution:
Below i performed the  distributed join between local rental data and remote customer data with selective predicates to limit results to 3-10 rows.

SQL:

-- Distributed join with selective predicates
SELECT r.rental_id, r.rental_date, r.amount, c.customer_name, c.email, c.phone
FROM Rental_A r
JOIN Customer c ON r.customer_id = c.customer_id
WHERE r.amount BETWEEN 5000 AND 7000
AND r.status = 'COMPLETED'
AND r.rental_date BETWEEN '2024-01-01' AND '2024-01-07'
ORDER BY r.rental_date;

-- Alternative distributed join with aggregation
SELECT c.customer_name, COUNT(r.rental_id) as rental_count, SUM(r.amount) as total_spent
FROM Rental_A r
JOIN Customer c ON r.customer_id = c.customer_id
WHERE r.status = 'COMPLETED'
GROUP BY c.customer_name
HAVING COUNT(r.rental_id) >= 1
ORDER BY total_spent DESC
LIMIT 8;

A3: Parallel vs Serial Aggregation (≤10 rows data)

A.3.1
Run a SERIAL aggregation on Rental_ALL over the small dataset (e.g., totals by a domain column). Ensure result has 3–10 groups/rows.

Solution :
I executed serial aggregation query grouping by status column to get rental statistics per status category.

SQL:
-- SERIAL aggregation by status
SELECT status, 
       COUNT(*) as rental_count, 
       SUM(amount) as total_amount,
       AVG(amount) as avg_amount,
       MIN(rental_date) as first_rental,
       MAX(rental_date) as last_rental
FROM Rental_ALL
GROUP BY status
ORDER BY status;

-- Additional serial aggregation by customer segments
SELECT 
    CASE 
        WHEN amount < 5500 THEN 'Budget'
        WHEN amount BETWEEN 5500 AND 7000 THEN 'Standard' 
        ELSE 'Premium'
    END as customer_segment,
    COUNT(*) as rental_count,
    SUM(amount) as total_revenue
FROM Rental_ALL
GROUP BY 
    CASE 
        WHEN amount < 5500 THEN 'Budget'
        WHEN amount BETWEEN 5500 AND 7000 THEN 'Standard'
        ELSE 'Premium'
    END
ORDER BY total_revenue DESC;

A.3.2
Run the same aggregation with /*+ PARALLEL(Rental_A,8) PARALLEL(Rental_B,8) */ to force a parallel plan despite small size.

Solution:
Below are the queries that shows how to force parallel execution (using PostgreSQL configuration settings and query hints) to demonstrate parallel query capabilities.

SQL:

-- Enable parallel query execution
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 10;
SET parallel_tuple_cost = 0.001;

-- PARALLEL aggregation with configuration
SELECT status, 
       COUNT(*) as rental_count, 
       SUM(amount) as total_amount,
       AVG(amount) as avg_amount
FROM Rental_ALL
GROUP BY status
ORDER BY status;

-- Reset parallel settings
RESET max_parallel_workers_per_gather;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;

-- Alternative parallel approach using CTE
WITH parallel_agg AS (
    SELECT status, amount
    FROM Rental_ALL
)
SELECT status, 
       COUNT(*) as rental_count,
       SUM(amount) as total_amount
FROM parallel_agg
GROUP BY status
ORDER BY status;


A.3.3
Capture execution plans with DBMS_XPLAN and show AUTOTRACE statistics; timings may be similar due to small data.

Solution :
Below queries i used  PostgreSQL to EXPLAIN and EXPLAIN ANALYZE to capture execution plans and performance statistics for both serial and parallel executions.

SQL:

-- Capture execution plan for serial aggregation
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT status, COUNT(*) as rental_count, SUM(amount) as total_amount
FROM Rental_ALL
GROUP BY status
ORDER BY status;

-- Capture execution plan for parallel aggregation
SET max_parallel_workers_per_gather = 4;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT status, COUNT(*) as rental_count, SUM(amount) as total_amount
FROM Rental_ALL
GROUP BY status
ORDER BY status;
RESET max_parallel_workers_per_gather;

-- Detailed performance analysis
EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON)
SELECT status, COUNT(*) as rental_count, SUM(amount) as total_amount
FROM Rental_ALL
GROUP BY status;

-- Get query timing information
\timing on
SELECT status, COUNT(*) as rental_count, SUM(amount) as total_amount
FROM Rental_ALL
GROUP BY status;
\timing off


A.3.4
Produce a 2-row comparison table (serial vs parallel) with plan notes.

Solution :
Below is the Created comparison table showing performance differences between serial and parallel execution modes.

SQL:

-- Create performance comparison table
CREATE TEMPORARY TABLE query_performance AS
SELECT 
    'Serial' as execution_mode,
    COUNT(*) as operations_processed,
    SUM(amount) as total_amount_processed,
    AVG(amount) as avg_amount_per_op,
    NOW() as execution_time
FROM Rental_ALL
WHERE status = 'COMPLETED';

INSERT INTO query_performance
SELECT 
    'Parallel' as execution_mode,
    COUNT(*) as operations_processed,
    SUM(amount) as total_amount_processed,
    AVG(amount) as avg_amount_per_op,
    NOW() as execution_time
FROM Rental_ALL
WHERE status = 'COMPLETED';

-- Display comparison results
SELECT * FROM query_performance;

-- Additional performance metrics
SELECT 
    execution_mode,
    operations_processed,
    total_amount_processed,
    avg_amount_per_op,
    EXTRACT(MILLISECONDS FROM execution_time - LAG(execution_time) OVER (ORDER BY execution_time)) as time_diff_ms
FROM query_performance;


A4: Two-Phase Commit & Recovery (2 rows)

A.4.1
Write one PL/SQL block that inserts ONE local row (related to Rental) on Node_A and ONE remote row into Payment@proj_link (or Rental@proj_link); then COMMIT.

Solution :
Below queries shows the implementation of atomic distributed transaction inserting one local rental record and one remote payment record within a single transaction block.

SQL:

-- First, create Payment table on both nodes if not exists
CREATE TABLE Payment (
    payment_id INTEGER PRIMARY KEY,
    rental_id INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    payment_date DATE NOT NULL,
    payment_method VARCHAR(20)
);

-- Clean two-phase commit transaction
DO $$
BEGIN
    -- Local insert on Node_A
    INSERT INTO Rental_A VALUES (11, 111, 211, CURRENT_DATE, NULL, 5800, 'ACTIVE');
    
    -- Remote insert on Node_B via foreign table
    INSERT INTO Payment VALUES (1, 11, 5800, CURRENT_DATE, 'CREDIT_CARD');
    
    -- Commit both operations atomically
    RAISE NOTICE 'Distributed transaction completed successfully';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Transaction failed: %', SQLERRM;
        ROLLBACK;
        RAISE;
END $$;

-- Verify the committed data
SELECT 'Local Rental' as type, rental_id, amount FROM Rental_A WHERE rental_id = 11
UNION ALL
SELECT 'Remote Payment' as type, rental_id, amount FROM Payment WHERE rental_id = 11;


A.4.2
Induce a failure in a second run (e.g., disable the link between inserts) to create an in-doubt transaction; ensure any extra test rows are ROLLED BACK to keep within the ≤10 committed row budget.

Solution :
Below queries shows how to Simulate network failure scenario to create in-doubt transaction and demonstrate automatic rollback to maintain data consistency.

SQL:

-- Induce failure scenario with simulated network error
DO $$
BEGIN
    -- Local insert on Node_A
    INSERT INTO Rental_A VALUES (13, 113, 213, CURRENT_DATE, NULL, 5400, 'ACTIVE');
    
    -- Simulate network failure before remote insert
    -- In real scenario, this would be: INSERT INTO Payment@proj_link VALUES (...)
    -- For simulation, we'll force an error
    IF EXISTS (SELECT 1 FROM Rental_A WHERE rental_id = 13) THEN
        RAISE EXCEPTION 'SIMULATED_NETWORK_FAILURE: Connection to Node_B lost';
    END IF;
    
    -- This won't be reached due to the exception
    INSERT INTO Payment VALUES (3, 13, 5400, CURRENT_DATE, 'CASH');
    
EXCEPTION 
    WHEN OTHERS THEN
        RAISE NOTICE 'Transaction rolled back due to: %', SQLERRM;
        -- Explicit rollback to ensure no partial commits
        ROLLBACK;
END $$;

-- Verify no partial commits occurred
SELECT COUNT(*) as rental_count FROM Rental_A WHERE rental_id = 13;
SELECT COUNT(*) as payment_count FROM Payment WHERE rental_id = 13;

A.4.3
Query DBA_2PC_PENDING; then issue COMMIT FORCE or ROLLBACK FORCE; re-verify consistency on both nodes.

Solution :
Below queries shows how to Check for prepared transactions (PostgreSQL equivalent of pending distributed transactions) and verify data consistency across nodes.

SQL:

-- Check for prepared transactions (PostgreSQL equivalent of DBA_2PC_PENDING)
SELECT gid, prepared, owner, database, transaction AS xid
FROM pg_prepared_xacts;

-- If there are prepared transactions, we can commit or rollback them
-- COMMIT PREPARED 'transaction_gid';
-- ROLLBACK PREPARED 'transaction_gid';

-- Verify data consistency across nodes
SELECT 'Node_A Rentals' as dataset, COUNT(*) as row_count FROM Rental_A
UNION ALL
SELECT 'Node_B Payments' as dataset, COUNT(*) as row_count FROM Payment
UNION ALL
SELECT 'Node_B Rentals' as dataset, COUNT(*) as row_count FROM Rental_B;

-- Check for orphaned records
SELECT r.rental_id, r.amount as rental_amount, p.amount as payment_amount
FROM Rental_A r
LEFT JOIN Payment p ON r.rental_id = p.rental_id
WHERE p.rental_id IS NULL
AND r.rental_id > 10;  -- Check only test records


A.4.4
Repeat a clean run to show there are no pending transactions.

Solution:
Execute successful distributed transaction and verify no pending transactions remain in the system.

SQL:

-- Clean successful distributed transaction
DO $$
BEGIN
    -- Verify no pending transactions first
    IF EXISTS (SELECT 1 FROM pg_prepared_xacts) THEN
        RAISE NOTICE 'Found pending transactions, cleaning up...';
        -- In real scenario, would commit/rollback prepared transactions
    END IF;
    
    -- Execute clean distributed transaction
    INSERT INTO Rental_A VALUES (15, 115, 215, CURRENT_DATE, NULL, 6200, 'ACTIVE');
    INSERT INTO Payment VALUES (5, 15, 6200, CURRENT_DATE, 'MOBILE');
    
    COMMIT;
    RAISE NOTICE 'Clean distributed transaction completed successfully';
END $$;

-- Final consistency verification
SELECT 'Pending Transactions' as check_type, 
       COUNT(*) as count 
FROM pg_prepared_xacts

UNION ALL

SELECT 'Total Committed Rentals' as check_type, 
       COUNT(*) as count 
FROM Rental_A

UNION ALL

SELECT 'Total Committed Payments' as check_type, 
       COUNT(*) as count 
FROM Payment

UNION ALL

SELECT 'Row Budget Status' as check_type,
       CASE 
           WHEN (SELECT COUNT(*) FROM Rental_A) + 
                (SELECT COUNT(*) FROM Rental_B) <= 10 THEN 'WITHIN_BUDGET'
           ELSE 'EXCEEDED_BUDGET'
       END as count;

A5: Distributed Lock Conflict & Diagnosis (no extra rows)


A.5.1
Open Session 1 on Node_A: UPDATE a single row in Rental or Payment and keep the transaction open.

Solution:
Start a transaction in Session 1 that updates a specific row but doesn't commit, creating a lock scenario.

SQL:

-- Session 1: Start transaction and acquire lock
BEGIN;

-- Update a specific rental record
UPDATE Rental_A 
SET amount = amount + 1000,
    status = 'UPDATED'
WHERE rental_id = 5;

-- Display current locks held by this session
SELECT pid, locktype, mode, granted, relation::regclass
FROM pg_locks 
WHERE pid = pg_backend_pid()
AND relation = 'rental_a'::regclass;

-- DO NOT COMMIT - Keep transaction open for lock demonstration
-- Transaction remains active...

A.5.2
Open Session 2 from Node_B via Rental@proj_link or Payment@proj_link to UPDATE the same logical row.

Solution :
Below queries update the same row that Session 1 has locked, demonstrating lock contention.

SQL:

-- Session 2: Attempt to update the same row (this will wait)
BEGIN;

-- This UPDATE will wait for Session 1's lock to be released
UPDATE Rental_A 
SET amount = amount + 500
WHERE rental_id = 5;

-- If we get here, the lock was acquired
COMMIT;

-- Alternative: Check if the update is waiting
SELECT NOW() as check_time, 
       EXISTS(
           SELECT 1 FROM pg_locks 
           WHERE relation = 'rental_a'::regclass 
           AND NOT granted
       ) as is_waiting;

A.5.3
Query lock views (DBA_BLOCKERS/DBA_WAITERS/V$LOCK) from Node_A to show the waiting session.

Solution :
Below Query lock monitoring views to identify blocking and waiting sessions.

SQL:

-- Lock diagnostics: Show all locks on Rental_A table
SELECT 
    l.pid,
    a.usename,
    a.application_name,
    a.client_addr,
    l.locktype,
    l.mode,
    l.granted,
    a.query,
    a.state,
    a.state_change
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
WHERE l.relation = 'rental_a'::regclass
ORDER BY l.granted, l.pid;

-- Blocking sessions analysis (PostgreSQL equivalent of DBA_BLOCKERS)
SELECT 
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS blocking_statement,
    blocked_activity.application_name AS blocked_app,
    blocking_activity.application_name AS blocking_app,
    NOW() - blocked_activity.query_start AS blocked_duration,
    NOW() - blocking_activity.query_start AS blocking_duration
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;

-- Current waiting locks
SELECT 
    a.pid,
    a.usename,
    a.query,
    a.wait_event_type,
    a.wait_event,
    a.state,
    NOW() - a.query_start as waiting_since
FROM pg_stat_activity a
WHERE a.wait_event_type IS NOT NULL
AND a.state = 'active';

A.5.4
Release the lock; show Session 2 completes. Do not insert more rows; reuse the existing ≤10.

Solution:
Below queries Release the lock from Session 1 and verify that Session 2 can complete its operation.

SQL:

-- Session 1: Release the lock by committing
COMMIT;

-- Session 2: Verify the update completed
SELECT rental_id, amount, status 
FROM Rental_A 
WHERE rental_id = 5;

-- Final lock status check
SELECT 
    'Active Sessions' as check_type,
    COUNT(*) as count
FROM pg_stat_activity 
WHERE state = 'active'
AND datname = current_database()

UNION ALL

SELECT 
    'Active Locks' as check_type,
    COUNT(*) as count
FROM pg_locks 
WHERE granted = true

UNION ALL

SELECT 
    'Waiting Locks' as check_type,
    COUNT(*) as count
FROM pg_locks 
WHERE granted = false

UNION ALL

SELECT 
    'Total Rental Rows' as check_type,
    COUNT(*) as count
FROM Rental_A;


B6: Declarative Rules Hardening (≤10 committed rows)

B6.1
On tables Rental and Payment, add/verify NOT NULL and domain CHECK constraints suitable for rentals, payments, vehicle status (e.g., positive amounts, valid statuses, date order).

Solution:
Below queries add comprehensive data integrity constraints to enforce business rules and prevent invalid data entry.

SQL:

-- Add constraints to Rental_A table
ALTER TABLE Rental_A 
ADD CONSTRAINT nn_rental_rental_date NOT NULL rental_date,
ADD CONSTRAINT nn_rental_amount NOT NULL amount,
ADD CONSTRAINT nn_rental_status NOT NULL status,
ADD CONSTRAINT chk_rental_amount CHECK (amount > 0),
ADD CONSTRAINT chk_rental_dates CHECK (return_date IS NULL OR rental_date <= return_date),
ADD CONSTRAINT chk_rental_status CHECK (status IN ('ACTIVE', 'COMPLETED', 'CANCELLED')),
ADD CONSTRAINT chk_rental_dates_future CHECK (rental_date <= CURRENT_DATE + INTERVAL '30 days');

-- Add constraints to Rental_B table
ALTER TABLE Rental_B 
ADD CONSTRAINT nn_rental_rental_date_b NOT NULL rental_date,
ADD CONSTRAINT nn_rental_amount_b NOT NULL amount,
ADD CONSTRAINT nn_rental_status_b NOT NULL status,
ADD CONSTRAINT chk_rental_amount_b CHECK (amount > 0),
ADD CONSTRAINT chk_rental_dates_b CHECK (return_date IS NULL OR rental_date <= return_date),
ADD CONSTRAINT chk_rental_status_b CHECK (status IN ('ACTIVE', 'COMPLETED', 'CANCELLED'));

-- Add constraints to Payment table
ALTER TABLE Payment 
ADD CONSTRAINT nn_payment_amount NOT NULL amount,
ADD CONSTRAINT nn_payment_date NOT NULL payment_date,
ADD CONSTRAINT nn_payment_method NOT NULL payment_method,
ADD CONSTRAINT chk_payment_amount CHECK (amount > 0),
ADD CONSTRAINT chk_payment_date CHECK (payment_date <= CURRENT_DATE),
ADD CONSTRAINT chk_payment_method CHECK (payment_method IN ('CASH', 'CREDIT_CARD', 'DEBIT_CARD', 'MOBILE', 'BANK_TRANSFER'));

-- Verify constraints were added
SELECT table_name, constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE table_name IN ('rental_a', 'rental_b', 'payment')
ORDER BY table_name, constraint_type;

B.6.2
Prepare 2 failing and 2 passing INSERTs per table to validate rules, but wrap failing ones in a block and ROLLBACK so committed rows stay within ≤10 total.

Solution :
Below queries Test constraint enforcement with both valid and invalid data inserts, ensuring failed inserts are rolled back to maintain row budget.

SQL:

-- Test INSERTs for Rental_A table
DO $$
BEGIN
    RAISE NOTICE 'Testing Rental_A constraints...';
    
    -- Passing INSERT 1 for Rental_A
    INSERT INTO Rental_A VALUES (16, 116, 216, CURRENT_DATE, NULL, 4800, 'ACTIVE');
    RAISE NOTICE '✓ Passing INSERT 1 completed';
    
    -- Passing INSERT 2 for Rental_A  
    INSERT INTO Rental_A VALUES (17, 117, 217, CURRENT_DATE, CURRENT_DATE + 5, 5200, 'COMPLETED');
    RAISE NOTICE '✓ Passing INSERT 2 completed';
    
    -- Failing INSERT 1 for Rental_A (negative amount)
    BEGIN
        INSERT INTO Rental_A VALUES (18, 118, 218, CURRENT_DATE, NULL, -1000, 'ACTIVE');
        RAISE NOTICE '✗ Expected failure but insert succeeded';
    EXCEPTION
        WHEN check_violation THEN
            RAISE NOTICE '✓ Expected failure: Negative amount constraint violation';
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Unexpected error: %', SQLERRM;
    END;
    
    -- Failing INSERT 2 for Rental_A (invalid status)
    BEGIN
        INSERT INTO Rental_A VALUES (19, 119, 219, CURRENT_DATE, NULL, 6000, 'INVALID_STATUS');
        RAISE NOTICE '✗ Expected failure but insert succeeded';
    EXCEPTION
        WHEN check_violation THEN
            RAISE NOTICE '✓ Expected failure: Invalid status constraint violation';
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Unexpected error: %', SQLERRM;
    END;
    
    -- Rollback test inserts to maintain row budget
    ROLLBACK;
    RAISE NOTICE 'Test inserts rolled back to maintain row budget';
END $$;

-- Test INSERTs for Payment table
DO $$
BEGIN
    RAISE NOTICE 'Testing Payment constraints...';
    
    -- Passing INSERT 1 for Payment
    INSERT INTO Payment VALUES (6, 1, 5000, CURRENT_DATE, 'CREDIT_CARD');
    RAISE NOTICE '✓ Passing INSERT 1 completed';
    
    -- Passing INSERT 2 for Payment
    INSERT INTO Payment VALUES (7, 2, 6500, CURRENT_DATE, 'MOBILE');
    RAISE NOTICE '✓ Passing INSERT 2 completed';
    
    -- Failing INSERT 1 for Payment (zero amount)
    BEGIN
        INSERT INTO Payment VALUES (8, 3, 0, CURRENT_DATE, 'CASH');
        RAISE NOTICE '✗ Expected failure but insert succeeded';
    EXCEPTION
        WHEN check_violation THEN
            RAISE NOTICE '✓ Expected failure: Zero amount constraint violation';
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Unexpected error: %', SQLERRM;
    END;
    
    -- Failing INSERT 2 for Payment (invalid payment method)
    BEGIN
        INSERT INTO Payment VALUES (9, 4, 8000, CURRENT_DATE, 'INVALID_METHOD');
        RAISE NOTICE '✗ Expected failure but insert succeeded';
    EXCEPTION
        WHEN check_violation THEN
            RAISE NOTICE '✓ Expected failure: Invalid payment method constraint violation';
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Unexpected error: %', SQLERRM;
    END;
    
    -- Rollback test inserts to maintain row budget
    ROLLBACK;
    RAISE NOTICE 'Test inserts rolled back to maintain row budget';
END $$;

B.6.3
Show clean error handling for failing cases.

Solution:
Below queries shows the implementation of comprehensive error handling that captures constraint violations and provides meaningful error messages.

SQL:

-- Comprehensive error handling demonstration
DO $$
DECLARE
    v_test_rental_id INTEGER := 20;
    v_test_payment_id INTEGER := 10;
BEGIN
    RAISE NOTICE '=== COMPREHENSIVE CONSTRAINT VALIDATION ===';
    
    -- Test 1: Valid rental insertion
    BEGIN
        INSERT INTO Rental_A VALUES (v_test_rental_id, 120, 220, CURRENT_DATE, NULL, 5500, 'ACTIVE');
        RAISE NOTICE '✓ Test 1 PASS: Valid rental inserted successfully';
    EXCEPTION 
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Test 1 FAIL: %', SQLERRM;
    END;
    
    -- Test 2: Rental with return date before rental date (should fail)
    BEGIN
        INSERT INTO Rental_A VALUES (v_test_rental_id + 1, 121, 221, CURRENT_DATE, CURRENT_DATE - 1, 6000, 'COMPLETED');
        RAISE NOTICE '✗ Test 2 UNEXPECTED: Invalid dates were accepted';
    EXCEPTION 
        WHEN check_violation THEN
            RAISE NOTICE '✓ Test 2 PASS: Date order constraint enforced - %', SQLERRM;
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Test 2 FAIL: Unexpected error - %', SQLERRM;
    END;
    
    -- Test 3: Valid payment insertion
    BEGIN
        INSERT INTO Payment VALUES (v_test_payment_id, v_test_rental_id, 5500, CURRENT_DATE, 'BANK_TRANSFER');
        RAISE NOTICE '✓ Test 3 PASS: Valid payment inserted successfully';
    EXCEPTION 
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Test 3 FAIL: %', SQLERRM;
    END;
    
    -- Test 4: Payment with future date (should fail)
    BEGIN
        INSERT INTO Payment VALUES (v_test_payment_id + 1, v_test_rental_id, 6000, CURRENT_DATE + 1, 'CASH');
        RAISE NOTICE '✗ Test 4 UNEXPECTED: Future payment date was accepted';
    EXCEPTION 
        WHEN check_violation THEN
            RAISE NOTICE '✓ Test 4 PASS: Payment date constraint enforced - %', SQLERRM;
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Test 4 FAIL: Unexpected error - %', SQLERRM;
    END;
    
    -- Test 5: NULL value in required field (should fail)
    BEGIN
        INSERT INTO Rental_A VALUES (v_test_rental_id + 2, 122, 222, NULL, NULL, 5000, 'ACTIVE');
        RAISE NOTICE '✗ Test 5 UNEXPECTED: NULL rental date was accepted';
    EXCEPTION 
        WHEN not_null_violation THEN
            RAISE NOTICE '✓ Test 5 PASS: NOT NULL constraint enforced - %', SQLERRM;
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Test 5 FAIL: Unexpected error - %', SQLERRM;
    END;
    
    -- Rollback all test data
    ROLLBACK;
    RAISE NOTICE '=== ALL TEST DATA ROLLED BACK ===';
    
    -- Final constraint validation report
    RAISE NOTICE 'Constraint Summary:';
    RAISE NOTICE '- Rental Amount: Must be positive';
    RAISE NOTICE '- Rental Dates: return_date must be after rental_date';
    RAISE NOTICE '- Rental Status: Must be ACTIVE, COMPLETED, or CANCELLED';
    RAISE NOTICE '- Payment Amount: Must be positive';
    RAISE NOTICE '- Payment Date: Cannot be in future';
    RAISE NOTICE '- Payment Method: Must be valid type';
    
END $$;

-- Final row count verification
SELECT 
    'Rental_A' as table_name, 
    COUNT(*) as current_rows 
FROM Rental_A
UNION ALL
SELECT 
    'Rental_B' as table_name, 
    COUNT(*) as current_rows 
FROM Rental_B
UNION ALL
SELECT 
    'Payment' as table_name, 
    COUNT(*) as current_rows 
FROM Payment
UNION ALL
SELECT 
    'TOTAL' as table_name,
    (SELECT COUNT(*) FROM Rental_A) + 
    (SELECT COUNT(*) FROM Rental_B) + 
    (SELECT COUNT(*) FROM Payment) as current_rows;


B7: E–C–A Trigger for Denormalized Totals (small DML set)

B.7.1

Create an audit table Rental_AUDIT(bef_total NUMBER, aft_total NUMBER, changed_at TIMESTAMP, key_col VARCHAR2(64)).

Solution :
Below queries shows how to Create audit table to track changes in payment totals with before/after values and timestamps.

SQL:

-- Create audit table for tracking payment total changes
CREATE TABLE Rental_AUDIT (
    audit_id SERIAL PRIMARY KEY,
    bef_total DECIMAL(12,2),
    aft_total DECIMAL(12,2),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    key_col VARCHAR(64),
    operation_type VARCHAR(10),
    rows_affected INTEGER
);

-- Create index for efficient querying
CREATE INDEX idx_rental_audit_changed_at ON Rental_AUDIT(changed_at);
CREATE INDEX idx_rental_audit_key_col ON Rental_AUDIT(key_col);

-- Verify table creation
SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_name = 'rental_audit'
ORDER BY ordinal_position;

B.7.2
Implement a statement-level AFTER INSERT/UPDATE/DELETE trigger on Payment that recomputes denormalized totals in Rental once per statement.

Solution :
Below queries shows the implementation of statement-level trigger that calculates payment totals before and after DML operations and logs changes to audit table.

SQL:

-- Create trigger function for payment total auditing
CREATE OR REPLACE FUNCTION trg_payment_total_audit()
RETURNS TRIGGER AS $$
DECLARE
    v_old_total DECIMAL(12,2);
    v_new_total DECIMAL(12,2);
    v_operation VARCHAR(10);
    v_rows_affected INTEGER;
BEGIN
    -- Determine operation type and row count
    IF TG_OP = 'INSERT' THEN
        v_operation := 'INSERT';
        v_rows_affected := (SELECT COUNT(*) FROM new_table);
    ELSIF TG_OP = 'UPDATE' THEN
        v_operation := 'UPDATE';
        v_rows_affected := (SELECT COUNT(*) FROM new_table);
    ELSIF TG_OP = 'DELETE' THEN
        v_operation := 'DELETE';
        v_rows_affected := (SELECT COUNT(*) FROM old_table);
    END IF;
    
    -- Calculate old total (before operation)
    SELECT COALESCE(SUM(amount), 0) INTO v_old_total FROM Payment;
    
    -- Adjust for the current operation
    IF TG_OP = 'INSERT' THEN
        SELECT COALESCE(SUM(amount), 0) INTO v_new_total 
        FROM (SELECT amount FROM Payment UNION ALL SELECT amount FROM new_table) AS combined;
    ELSIF TG_OP = 'UPDATE' THEN
        -- For update, we need to subtract old values and add new values
        SELECT COALESCE(SUM(amount), 0) INTO v_new_total 
        FROM Payment;
    ELSIF TG_OP = 'DELETE' THEN
        SELECT COALESCE(SUM(amount), 0) INTO v_new_total 
        FROM (SELECT amount FROM Payment EXCEPT SELECT amount FROM old_table) AS remaining;
    END IF;
    
    -- Insert audit record
    INSERT INTO Rental_AUDIT (bef_total, aft_total, key_col, operation_type, rows_affected)
    VALUES (v_old_total, v_new_total, 'PAYMENT_TOTALS', v_operation, v_rows_affected);
    
    RETURN NULL; -- Statement-level trigger returns NULL
END;
$$ LANGUAGE plpgsql;

-- Create statement-level trigger
CREATE TRIGGER trg_audit_payment_totals
AFTER INSERT OR UPDATE OR DELETE ON Payment
REFERENCING 
    NEW TABLE AS new_table 
    OLD TABLE AS old_table
FOR EACH STATEMENT
EXECUTE FUNCTION trg_payment_total_audit();

-- Verify trigger creation
SELECT trigger_name, event_manipulation, action_timing, action_orientation
FROM information_schema.triggers 
WHERE event_object_table = 'payment';


B.7.3
Execute a small mixed DML script on CHILD affecting at most 4 rows in total; ensure net committed rows across the project remain ≤10.

Solution:
Below queries shows how to execute mixed DML operations (INSERT, UPDATE, DELETE) on Payment table while maintaining overall row budget.

SQL:

-- Mixed DML operations script (affecting 4 rows total)
DO $$
DECLARE
    v_start_total DECIMAL(12,2);
    v_end_total DECIMAL(12,2);
BEGIN
    RAISE NOTICE '=== STARTING MIXED DML OPERATIONS ===';
    
    -- Get initial total
    SELECT COALESCE(SUM(amount), 0) INTO v_start_total FROM Payment;
    RAISE NOTICE 'Initial payment total: %', v_start_total;
    
    -- Operation 1: INSERT
    RAISE NOTICE '1. INSERTING new payment...';
    INSERT INTO Payment VALUES (10, 5, 6000, CURRENT_DATE, 'CREDIT_CARD');
    
    -- Operation 2: UPDATE  
    RAISE NOTICE '2. UPDATING existing payment...';
    UPDATE Payment SET amount = 7000 WHERE payment_id = 2;
    
    -- Operation 3: DELETE
    RAISE NOTICE '3. DELETING payment...';
    DELETE FROM Payment WHERE payment_id = 1;
    
    -- Operation 4: INSERT
    RAISE NOTICE '4. INSERTING another payment...';
    INSERT INTO Payment VALUES (11, 7, 4500, CURRENT_DATE, 'MOBILE');
    
    -- Get final total
    SELECT COALESCE(SUM(amount), 0) INTO v_end_total FROM Payment;
    RAISE NOTICE 'Final payment total: %', v_end_total;
    RAISE NOTICE 'Net change: %', v_end_total - v_start_total;
    
    COMMIT;
    RAISE NOTICE '=== MIXED DML OPERATIONS COMPLETED ===';
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE NOTICE 'Operations rolled back due to error: %', SQLERRM;
END $$;

-- Verify row counts after operations
SELECT 
    'Payment' as table_name, 
    COUNT(*) as row_count,
    SUM(amount) as total_amount
FROM Payment;

B.7.4
Log before/after totals to the audit table (2–3 audit rows).

Solution :
Below is the Query audit table to demonstrate trigger functionality and verify before/after totals are properly logged.

SQL:

-- Query audit entries to verify trigger functionality
SELECT 
    audit_id,
    bef_total as before_total,
    aft_total as after_total,
    (aft_total - bef_total) as change_amount,
    operation_type,
    rows_affected,
    key_col,
    changed_at
FROM Rental_AUDIT 
ORDER BY changed_at;

-- Detailed audit analysis
SELECT 
    operation_type,
    COUNT(*) as operation_count,
    AVG(aft_total - bef_total) as avg_change,
    SUM(rows_affected) as total_rows_affected,
    MIN(changed_at) as first_operation,
    MAX(changed_at) as last_operation
FROM Rental_AUDIT 
GROUP BY operation_type
ORDER BY operation_count DESC;

-- Current state verification
SELECT 
    'Current Payment Total' as description,
    SUM(amount) as amount,
    COUNT(*) as payment_count
FROM Payment

UNION ALL

SELECT 
    'Audit Records' as description,
    COUNT(*)::decimal as amount,
    COUNT(DISTINCT operation_type) as payment_count
FROM Rental_AUDIT

UNION ALL

SELECT 
    'Overall Row Budget' as description,
    (SELECT COUNT(*) FROM Rental_A) + 
    (SELECT COUNT(*) FROM Rental_B) + 
    (SELECT COUNT(*) FROM Payment) as amount,
    0 as payment_count;

B8: Recursive Hierarchy Roll-Up (6–10 rows)

B.8.1
Create table HIER(parent_id, child_id) for a natural hierarchy (domain-specific).

Solution :
Below is the queries shows how to create hierarchy table to represent organizational structure with parent-child relationships.

SQL:

-- Create hierarchy table for organizational structure
CREATE TABLE HIER (
    parent_id INTEGER,
    child_id INTEGER NOT NULL,
    child_name VARCHAR(100),
    node_type VARCHAR(20),
    CONSTRAINT pk_hier PRIMARY KEY (child_id),
    CONSTRAINT fk_hier_parent FOREIGN KEY (parent_id) REFERENCES HIER(child_id),
    CONSTRAINT chk_hier_type CHECK (node_type IN ('REGION', 'COUNTRY', 'CITY', 'BRANCH'))
);

-- Create index for efficient hierarchical queries
CREATE INDEX idx_hier_parent ON HIER(parent_id);
CREATE INDEX idx_hier_type ON HIER(node_type);

-- Verify table structure
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'hier' 
ORDER BY ordinal_position;

B.8.2
Insert 6–10 rows forming a 3-level hierarchy.

Solution:
Below is how to Populate hierarchy table with sample organizational data representing regions, countries, cities, and branches.

SQL:

-- Insert hierarchical data (8 rows forming 3-level hierarchy)
INSERT INTO HIER VALUES 
(NULL, 100, 'East Africa Region', 'REGION'),           -- Level 1: Root
(100, 101, 'Rwanda Country', 'COUNTRY'),              -- Level 2: Country
(100, 102, 'Uganda Country', 'COUNTRY'),              -- Level 2: Country
(101, 201, 'Kigali City', 'CITY'),                    -- Level 3: City
(101, 202, 'Musanze City', 'CITY'),                   -- Level 3: City
(102, 203, 'Kampala City', 'CITY'),                   -- Level 3: City
(201, 301, 'Kigali Downtown Branch', 'BRANCH'),       -- Level 4: Branch
(202, 302, 'Musanze Central Branch', 'BRANCH');       -- Level 4: Branch

-- Verify hierarchy insertion
SELECT 
    parent_id,
    child_id, 
    child_name,
    node_type,
    CASE 
        WHEN parent_id IS NULL THEN 'ROOT'
        ELSE 'CHILD'
    END as node_category
FROM HIER 
ORDER BY parent_id NULLS FIRST, child_id;

-- Count verification
SELECT 
    node_type,
    COUNT(*) as node_count
FROM HIER 
GROUP BY node_type 
ORDER BY 
    CASE node_type
        WHEN 'REGION' THEN 1
        WHEN 'COUNTRY' THEN 2
        WHEN 'CITY' THEN 3
        WHEN 'BRANCH' THEN 4
    END;

B.8.3
Write a recursive WITH query to produce (child_id, root_id, depth) and join to Rental or its parent to compute rollups; return 6–10 rows total.

Solution description:
Below is the implementation of recursive CTE to traverse hierarchy and compute organizational rollups by joining with rental data.

SQL:

-- Recursive hierarchy traversal with rental rollups
WITH RECURSIVE hierarchy_traversal AS (
    -- Anchor: Root nodes (level 1)
    SELECT 
        child_id,
        child_id as root_id,
        child_name,
        node_type,
        0 as depth,
        ARRAY[child_id] as path,
        ARRAY[child_name] as path_names
    FROM HIER 
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Recursive: Child nodes (levels 2+)
    SELECT 
        h.child_id,
        r.root_id,
        h.child_name,
        h.node_type,
        r.depth + 1 as depth,
        r.path || h.child_id as path,
        r.path_names || h.child_name as path_names
    FROM HIER h
    JOIN hierarchy_traversal r ON h.parent_id = r.child_id
)
SELECT 
    ht.child_id,
    ht.child_name,
    ht.node_type,
    ht.root_id,
    (SELECT child_name FROM HIER WHERE child_id = ht.root_id) as root_name,
    ht.depth,
    array_to_string(ht.path_names, ' -> ') as hierarchy_path,
    -- Rental statistics rollup
    (SELECT COUNT(*) FROM Rental_A r WHERE r.customer_id = ht.child_id) as rental_count,
    (SELECT COALESCE(SUM(amount), 0) FROM Rental_A r WHERE r.customer_id = ht.child_id) as total_revenue,
    (SELECT COALESCE(AVG(amount), 0) FROM Rental_A r WHERE r.customer_id = ht.child_id) as avg_rental_amount
FROM hierarchy_traversal ht
ORDER BY ht.root_id, ht.depth, ht.child_id;

-- Alternative: Direct organizational rollup by hierarchy level
WITH RECURSIVE org_rollup AS (
    SELECT 
        child_id,
        parent_id,
        child_name,
        node_type,
        0 as depth
    FROM HIER 
    WHERE parent_id IS NULL
    
    UNION ALL
    
    SELECT 
        h.child_id,
        h.parent_id,
        h.child_name,
        h.node_type,
        o.depth + 1 as depth
    FROM HIER h
    JOIN org_rollup o ON h.parent_id = o.child_id
)
SELECT 
    o.node_type,
    COUNT(DISTINCT o.child_id) as location_count,
    COUNT(r.rental_id) as total_rentals,
    COALESCE(SUM(r.amount), 0) as total_revenue,
    COALESCE(AVG(r.amount), 0) as avg_rental_value
FROM org_rollup o
LEFT JOIN Rental_A r ON o.child_id = r.customer_id
GROUP BY o.node_type
ORDER BY 
    CASE o.node_type
        WHEN 'REGION' THEN 1
        WHEN 'COUNTRY' THEN 2
        WHEN 'CITY' THEN 3
        WHEN 'BRANCH' THEN 4
    END;

B.8.4
Reuse existing seed rows; do not exceed the ≤10 committed rows budget.

Solution:
Below is the query that shows how to ensure hierarchy operations respect the row budget by using existing data and verifying total counts.

SQL:

-- Final hierarchy validation with row budget check
WITH RECURSIVE full_hierarchy AS (
    SELECT child_id, parent_id, child_name, node_type, 0 as level
    FROM HIER WHERE parent_id IS NULL
    UNION ALL
    SELECT h.child_id, h.parent_id, h.child_name, h.node_type, f.level + 1
    FROM HIER h JOIN full_hierarchy f ON h.parent_id = f.child_id
)
SELECT 
    'Hierarchy Nodes' as category,
    COUNT(*) as count
FROM full_hierarchy

UNION ALL

SELECT 
    'Rental_A Records' as category,
    COUNT(*) as count
FROM Rental_A

UNION ALL

SELECT 
    'Rental_B Records' as category,
    COUNT(*) as count
FROM Rental_B

UNION ALL

SELECT 
    'Payment Records' as category,
    COUNT(*) as count
FROM Payment

UNION ALL

SELECT 
    'TOTAL COMMITTED ROWS' as category,
    (SELECT COUNT(*) FROM HIER) +
    (SELECT COUNT(*) FROM Rental_A) +
    (SELECT COUNT(*) FROM Rental_B) +
    (SELECT COUNT(*) FROM Payment) as count

UNION ALL

SELECT 
    'BUDGET STATUS' as category,
    CASE 
        WHEN (SELECT COUNT(*) FROM HIER) +
             (SELECT COUNT(*) FROM Rental_A) +
             (SELECT COUNT(*) FROM Rental_B) +
             (SELECT COUNT(*) FROM Payment) <= 10 
        THEN 1
        ELSE 0
    END as count;

-- Hierarchy summary with rental associations
SELECT 
    h.node_type,
    COUNT(DISTINCT h.child_id) as total_nodes,
    COUNT(DISTINCT r.rental_id) as associated_rentals,
    COUNT(DISTINCT r.customer_id) as unique_customers
FROM HIER h
LEFT JOIN Rental_A r ON h.child_id = r.customer_id
GROUP BY h.node_type
ORDER BY total_nodes DESC;

B9: Mini-Knowledge Base with Transitive Inference (≤10 facts)

B9.1
Create table TRIPLE(s VARCHAR2(64), p VARCHAR2(64), o VARCHAR2(64)).

Solution:
Below is the queries to Create triple store table for storing semantic relationships in subject-predicate-object format.

SQL:

-- Create triple store table for knowledge representation
CREATE TABLE TRIPLE (
    triple_id SERIAL PRIMARY KEY,
    s VARCHAR(64) NOT NULL,  -- Subject
    p VARCHAR(64) NOT NULL,  -- Predicate
    o VARCHAR(64) NOT NULL,  -- Object
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_triple_unique UNIQUE (s, p, o)
);

-- Create indexes for efficient querying
CREATE INDEX idx_triple_s ON TRIPLE(s);
CREATE INDEX idx_triple_p ON TRIPLE(p);
CREATE INDEX idx_triple_o ON TRIPLE(o);
CREATE INDEX idx_triple_spo ON TRIPLE(s, p, o);

-- Verify table structure
SELECT column_name, data_type, character_maximum_length, is_nullable
FROM information_schema.columns 
WHERE table_name = 'triple'
ORDER BY ordinal_position;

B.9.2
Insert 8–10 domain facts relevant to your project (e.g., simple type hierarchy or rule implications).

Solution :
Below is the queries to populate triple store with rental domain knowledge including type hierarchies, business rules, and relationships.

SQL:

-- Insert domain facts (10 triples representing rental business knowledge)
INSERT INTO TRIPLE (s, p, o) VALUES
-- Type hierarchy
('SUV', 'isA', 'VehicleType'),
('Sedan', 'isA', 'VehicleType'),
('Luxury', 'isA', 'VehicleType'),
('Luxury', 'isA', 'PremiumType'),
('PremiumType', 'isA', 'SpecialCategory'),
('VehicleType', 'isA', 'RentalCategory'),

-- Business rules and relationships
('SpecialCategory', 'requiresDeposit', 'true'),
('Luxury', 'dailyRate', '150'),
('SUV', 'dailyRate', '100'),
('Sedan', 'dailyRate', '80'),

-- Instance relationships
('Rental_101', 'hasType', 'SUV'),
('Rental_102', 'hasType', 'Luxury'),
('Rental_103', 'hasType', 'Sedan'),

-- Additional business rules
('PremiumType', 'minimumRentalDays', '2'),
('SpecialCategory', 'insuranceRequired', 'true');

-- Verify triple insertion
SELECT 
    triple_id,
    s as subject,
    p as predicate, 
    o as object,
    created_at
FROM TRIPLE 
ORDER BY 
    CASE p 
        WHEN 'isA' THEN 1
        WHEN 'hasType' THEN 2
        ELSE 3
    END, s;

-- Triple statistics
SELECT 
    p as predicate_type,
    COUNT(*) as fact_count,
    COUNT(DISTINCT s) as unique_subjects,
    COUNT(DISTINCT o) as unique_objects
FROM TRIPLE 
GROUP BY p 
ORDER BY fact_count DESC;

B.9.3
Write a recursive inference query implementing transitive isA*; apply labels to base records and return up to 10 labeled rows.

Solution :
Implement recursive inference to compute transitive closure of isA relationships and apply inferred categories to rental records.

SQL:

-- Recursive inference query for transitive isA relationships
WITH RECURSIVE inference_chain AS (
    -- Base case: Direct isA relationships
    SELECT 
        s as child_concept,
        o as parent_concept,
        s as leaf_concept,
        1 as inference_depth,
        ARRAY[s] as inference_path
    FROM TRIPLE 
    WHERE p = 'isA'
    
    UNION ALL
    
    -- Recursive case: Transitive isA relationships
    SELECT 
        i.child_concept,
        t.o as parent_concept,
        i.leaf_concept,
        i.inference_depth + 1 as inference_depth,
        i.inference_path || t.o as inference_path
    FROM inference_chain i
    JOIN TRIPLE t ON i.parent_concept = t.s AND t.p = 'isA'
    WHERE i.parent_concept != t.o  -- Avoid infinite recursion on self-references
      AND array_length(i.inference_path, 1) < 10  -- Depth limit for safety
)
SELECT DISTINCT
    r.s as rental_id,
    i.leaf_concept as direct_type,
    i.parent_concept as inferred_category,
    i.inference_depth,
    array_to_string(i.inference_path, ' -> ') as inference_chain,
    -- Get additional properties for the inferred category
    (SELECT o FROM TRIPLE WHERE s = i.parent_concept AND p = 'requiresDeposit') as requires_deposit,
    (SELECT o FROM TRIPLE WHERE s = i.parent_concept AND p = 'dailyRate') as daily_rate
FROM inference_chain i
JOIN TRIPLE r ON i.child_concept = r.o AND r.p = 'hasType'
WHERE i.parent_concept IN ('PremiumType', 'SpecialCategory', 'RentalCategory')
ORDER BY r.s, i.inference_depth;

-- Alternative: Complete concept hierarchy with all inferred relationships
WITH RECURSIVE concept_hierarchy AS (
    SELECT s as concept, s as root_concept, 0 as level, ARRAY[s] as path
    FROM TRIPLE WHERE p = 'isA'
    
    UNION ALL
    
    SELECT t.s as concept, ch.root_concept, ch.level + 1, ch.path || t.s
    FROM TRIPLE t
    JOIN concept_hierarchy ch ON t.o = ch.concept AND t.p = 'isA'
    WHERE array_length(ch.path, 1) < 5  -- Prevent infinite loops
)
SELECT 
    ch.concept,
    ch.root_concept,
    ch.level,
    array_to_string(ch.path, ' > ') as inheritance_path,
    COUNT(DISTINCT r.s) as rental_instances,
    STRING_AGG(DISTINCT prop.p || ': ' || prop.o, '; ') as properties
FROM concept_hierarchy ch
LEFT JOIN TRIPLE r ON ch.concept = r.o AND r.p = 'hasType'
LEFT JOIN TRIPLE prop ON ch.concept = prop.s AND prop.p NOT IN ('isA', 'hasType')
GROUP BY ch.concept, ch.root_concept, ch.level, ch.path
ORDER BY ch.level, ch.concept;

B.9.4
Ensure total committed rows across the project (including TRIPLE) remain ≤10; you may delete temporary rows after demo if needed.

Solution :
Below queries verify row budget compliance and clean up any temporary data while preserving essential knowledge base.

SQL:

-- Comprehensive row budget verification
WITH table_counts AS (
    SELECT 'Rental_A' as table_name, COUNT(*) as row_count FROM Rental_A
    UNION ALL SELECT 'Rental_B', COUNT(*) FROM Rental_B
    UNION ALL SELECT 'Payment', COUNT(*) FROM Payment
    UNION ALL SELECT 'HIER', COUNT(*) FROM HIER
    UNION ALL SELECT 'TRIPLE', COUNT(*) FROM TRIPLE
    UNION ALL SELECT 'Rental_AUDIT', COUNT(*) FROM Rental_AUDIT
)
SELECT 
    table_name,
    row_count,
    SUM(row_count) OVER () as total_rows,
    CASE 
        WHEN SUM(row_count) OVER () <= 10 THEN 'WITHIN_BUDGET'
        ELSE 'EXCEEDED_BUDGET'
    END as budget_status
FROM table_counts
ORDER BY row_count DESC;

-- Knowledge base summary with inference statistics
SELECT 
    'Triple Store' as component,
    COUNT(*) as count,
    COUNT(DISTINCT s) as unique_subjects,
    COUNT(DISTINCT p) as unique_predicates,
    COUNT(DISTINCT o) as unique_objects
FROM TRIPLE

UNION ALL

SELECT 
    'Inferred Relationships' as component,
    COUNT(*) as count,
    NULL as unique_subjects,
    NULL as unique_predicates,
    NULL as unique_objects
FROM (
    WITH RECURSIVE inference AS (
        SELECT s, o FROM TRIPLE WHERE p = 'isA'
        UNION ALL
        SELECT i.s, t.o
        FROM inference i
        JOIN TRIPLE t ON i.o = t.s AND t.p = 'isA'
    )
    SELECT DISTINCT s, o FROM inference
) AS inferred

UNION ALL

SELECT 
    'Labeled Rentals' as component,
    COUNT(DISTINCT r.s) as count,
    NULL as unique_subjects,
    NULL as unique_predicates,
    NULL as unique_objects
FROM TRIPLE r
WHERE r.p = 'hasType'

UNION ALL

SELECT 
    'Total Project Rows' as component,
    (SELECT COUNT(*) FROM Rental_A) +
    (SELECT COUNT(*) FROM Rental_B) +
    (SELECT COUNT(*) FROM Payment) +
    (SELECT COUNT(*) FROM HIER) +
    (SELECT COUNT(*) FROM TRIPLE) as count,
    NULL as unique_subjects,
    NULL as unique_predicates,
    NULL as unique_objects;

-- Final cleanup 
/*
-- Remove temporary test data if budget exceeded
DELETE FROM TRIPLE WHERE triple_id > 10;
DELETE FROM HIER WHERE child_id > 400;
DELETE FROM Rental_AUDIT WHERE audit_id > 5;
*/


B10: Business Limit Alert (Function + Trigger) (row-budget safe)

B10.1
Create BUSINESS_LIMITS(rule_key VARCHAR2(64), threshold NUMBER, active CHAR(1) CHECK(active IN('Y','N'))) and seed exactly one active rule.

Solution description:
Below queries create business rules configuration table with active/inactive status and seed with rental amount limit rule.

SQL:

-- Create business limits configuration table
CREATE TABLE BUSINESS_LIMITS (
    rule_id SERIAL PRIMARY KEY,
    rule_key VARCHAR(64) NOT NULL UNIQUE,
    rule_description VARCHAR(255),
    threshold DECIMAL(15,2) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_business_limits_active CHECK (active IN (true, false))
);

-- Create index for efficient rule lookup
CREATE INDEX idx_business_limits_key ON BUSINESS_LIMITS(rule_key);
CREATE INDEX idx_business_limits_active ON BUSINESS_LIMITS(active);

-- Seed with exactly one active rule
INSERT INTO BUSINESS_LIMITS (rule_key, rule_description, threshold, active) VALUES
('MAX_RENTAL_AMOUNT', 'Maximum total rental amount per customer', 15000.00, true),
('MIN_RENTAL_AMOUNT', 'Minimum rental amount per transaction', 1000.00, false),
('MAX_ACTIVE_RENTALS', 'Maximum concurrent active rentals per customer', 3, false);

-- Verify rule configuration
SELECT 
    rule_id,
    rule_key,
    rule_description,
    threshold,
    active,
    created_at
FROM BUSINESS_LIMITS 
ORDER BY active DESC, rule_key;

-- Active rules summary
SELECT 
    COUNT(*) as total_rules,
    COUNT(CASE WHEN active THEN 1 END) as active_rules,
    COUNT(CASE WHEN NOT active THEN 1 END) as inactive_rules
FROM BUSINESS_LIMITS;

B.10.2
Implement function fn_should_alert(...) that reads BUSINESS_LIMITS and inspects current data in Payment or Rental to decide a violation (return 1/0).

Solution :
Below queries create validation function that checks business rules against current rental data and returns violation status.

SQL:

-- Create business rule validation function
CREATE OR REPLACE FUNCTION fn_should_alert(
    p_customer_id INTEGER,
    p_rental_amount DECIMAL,
    p_rule_key VARCHAR DEFAULT 'MAX_RENTAL_AMOUNT'
) RETURNS BOOLEAN AS $$
DECLARE
    v_threshold DECIMAL;
    v_current_total DECIMAL;
    v_rule_active BOOLEAN;
    v_rental_count INTEGER;
BEGIN
    -- Get active threshold for the specified rule
    SELECT threshold, active INTO v_threshold, v_rule_active
    FROM BUSINESS_LIMITS 
    WHERE rule_key = p_rule_key;
    
    -- Return false if rule not found or inactive
    IF NOT FOUND OR NOT v_rule_active THEN
        RETURN false;
    END IF;
    
    -- Calculate current customer's rental total (excluding current rental being processed)
    SELECT COALESCE(SUM(amount), 0) INTO v_current_total
    FROM Rental_A
    WHERE customer_id = p_customer_id
    AND rental_id != COALESCE(p_rental_amount, -1);  -- Exclude current rental
    
    -- Count active rentals for the customer
    SELECT COUNT(*) INTO v_rental_count
    FROM Rental_A
    WHERE customer_id = p_customer_id
    AND status = 'ACTIVE';
    
    -- Check business rule violations based on rule key
    IF p_rule_key = 'MAX_RENTAL_AMOUNT' THEN
        IF (v_current_total + p_rental_amount) > v_threshold THEN
            RETURN true;
        END IF;
    
    ELSIF p_rule_key = 'MAX_ACTIVE_RENTALS' THEN
        IF v_rental_count >= v_threshold THEN
            RETURN true;
        END IF;
    
    ELSIF p_rule_key = 'MIN_RENTAL_AMOUNT' THEN
        IF p_rental_amount < v_threshold THEN
            RETURN true;
        END IF;
    END IF;
    
    RETURN false;
    
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN false; -- Default to no alert if rule not configured
    WHEN OTHERS THEN
        RAISE NOTICE 'Error in business rule validation: %', SQLERRM;
        RETURN false;
END;
$$ LANGUAGE plpgsql;

-- Test the validation function
SELECT 
    customer_id,
    fn_should_alert(customer_id, 8000, 'MAX_RENTAL_AMOUNT') as should_alert,
    (SELECT SUM(amount) FROM Rental_A WHERE customer_id = r.customer_id) as current_total,
    (SELECT threshold FROM BUSINESS_LIMITS WHERE rule_key = 'MAX_RENTAL_AMOUNT') as max_threshold
FROM Rental_A r
WHERE customer_id IN (101, 105)
GROUP BY customer_id;


B.10.3
Create a BEFORE INSERT OR UPDATE trigger on Payment (or relevant table) that raises an application error when fn_should_alert returns 1.

Solution :
Below queries implement trigger that enforces business rules by preventing DML operations that would violate configured limits.

SQL:

-- Create trigger function for business rule enforcement
CREATE OR REPLACE FUNCTION trg_enforce_business_rules()
RETURNS TRIGGER AS $$
DECLARE
    v_customer_id INTEGER;
    v_rental_amount DECIMAL;
    v_alert_required BOOLEAN;
BEGIN
    -- Determine customer_id and amount based on operation
    IF TG_OP = 'INSERT' THEN
        -- For Rental_A inserts, use the new values directly
        v_customer_id := NEW.customer_id;
        v_rental_amount := NEW.amount;
    ELSIF TG_OP = 'UPDATE' THEN
        -- For updates, use the new values
        v_customer_id := NEW.customer_id;
        v_rental_amount := NEW.amount;
    END IF;
    
    -- Check for business rule violations
    v_alert_required := fn_should_alert(v_customer_id, v_rental_amount, 'MAX_RENTAL_AMOUNT');
    
    IF v_alert_required THEN
        RAISE EXCEPTION 'BUSINESS_RULE_VIOLATION: Customer % would exceed maximum rental amount threshold. Current total: %, New amount: %, Threshold: %',
            v_customer_id,
            (SELECT COALESCE(SUM(amount), 0) FROM Rental_A WHERE customer_id = v_customer_id AND rental_id != COALESCE(NEW.rental_id, -1)),
            v_rental_amount,
            (SELECT threshold FROM BUSINESS_LIMITS WHERE rule_key = 'MAX_RENTAL_AMOUNT' AND active = true);
    END IF;
    
    -- Additional rule check for active rental limit
    IF TG_OP = 'INSERT' AND NEW.status = 'ACTIVE' THEN
        v_alert_required := fn_should_alert(v_customer_id, NULL, 'MAX_ACTIVE_RENTALS');
        IF v_alert_required THEN
            RAISE EXCEPTION 'BUSINESS_RULE_VIOLATION: Customer % would exceed maximum active rentals limit',
                v_customer_id;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on Rental_A table
CREATE TRIGGER trg_rental_business_rules
BEFORE INSERT OR UPDATE OF amount, status ON Rental_A
FOR EACH ROW
EXECUTE FUNCTION trg_enforce_business_rules();

-- Verify trigger creation
SELECT 
    trigger_name,
    event_manipulation,
    action_timing,
    action_orientation
FROM information_schema.triggers 
WHERE event_object_table = 'rental_a'
AND trigger_name = 'trg_rental_business_rules';


B.10.4
Demonstrate 2 failing and 2 passing DML cases; rollback the failing ones so total committed rows remain within the ≤10 budget.

Solution :
Below queries execute comprehensive test cases demonstrating both successful and failed business rule enforcement while maintaining row budget.

SQL:

-- Comprehensive business rule testing with budget control
DO $$
DECLARE
    v_test_rental_id INTEGER := 25;
    v_customer_101_total DECIMAL;
    v_max_threshold DECIMAL;
BEGIN
    RAISE NOTICE '=== BUSINESS RULE VALIDATION TESTING ===';
    
    -- Get current customer 101 total and threshold
    SELECT COALESCE(SUM(amount), 0) INTO v_customer_101_total 
    FROM Rental_A WHERE customer_id = 101;
    
    SELECT threshold INTO v_max_threshold 
    FROM BUSINESS_LIMITS WHERE rule_key = 'MAX_RENTAL_AMOUNT' AND active = true;
    
    RAISE NOTICE 'Customer 101 current rental total: %', v_customer_101_total;
    RAISE NOTICE 'Maximum rental amount threshold: %', v_max_threshold;
    RAISE NOTICE 'Available budget: %', v_max_threshold - v_customer_101_total;
    
    -- Test Case 1: PASSING INSERT (within budget)
    BEGIN
        INSERT INTO Rental_A VALUES (v_test_rental_id, 101, 225, CURRENT_DATE, NULL, 2000, 'ACTIVE');
        RAISE NOTICE '✓ Test 1 PASS: Valid rental within budget accepted';
        
        -- Rollback to maintain budget for other tests
        ROLLBACK;
    EXCEPTION 
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Test 1 FAIL: %', SQLERRM;
            ROLLBACK;
    END;
    
    -- Test Case 2: PASSING UPDATE (within budget)
    BEGIN
        -- First insert a test record
        INSERT INTO Rental_A VALUES (v_test_rental_id, 102, 225, CURRENT_DATE, NULL, 3000, 'ACTIVE');
        
        -- Update within budget
        UPDATE Rental_A SET amount = 4000 WHERE rental_id = v_test_rental_id;
        RAISE NOTICE '✓ Test 2 PASS: Valid rental update within budget accepted';
        
        ROLLBACK;
    EXCEPTION 
        WHEN OTHERS THEN
            RAISE NOTICE '✗ Test 2 FAIL: %', SQLERRM;
            ROLLBACK;
    END;
    
    -- Test Case 3: FAILING INSERT (exceeds budget)
    BEGIN
        INSERT INTO Rental_A VALUES (v_test_rental_id + 1, 101, 226, CURRENT_DATE, NULL, 12000, 'ACTIVE');
        RAISE NOTICE '✗ Test 3 UNEXPECTED: Budget-exceeding rental was accepted';
        ROLLBACK;
    EXCEPTION 
        WHEN OTHERS THEN
            IF SQLERRM LIKE 'BUSINESS_RULE_VIOLATION%' THEN
                RAISE NOTICE '✓ Test 3 PASS: Business rule correctly prevented budget violation - %', SUBSTRING(SQLERRM FROM 1 FOR 100);
            ELSE
                RAISE NOTICE '✗ Test 3 FAIL: Unexpected error - %', SQLERRM;
            END IF;
    END;
    
    -- Test Case 4: FAILING UPDATE (exceeds budget)
    BEGIN
        -- First insert a test record
        INSERT INTO Rental_A VALUES (v_test_rental_id + 2, 101, 227, CURRENT_DATE, NULL, 1000, 'ACTIVE');
        
        -- Try to update to exceed budget
        UPDATE Rental_A SET amount = 13000 WHERE rental_id = v_test_rental_id + 2;
        RAISE NOTICE '✗ Test 4 UNEXPECTED: Budget-exceeding update was accepted';
        ROLLBACK;
    EXCEPTION 
        WHEN OTHERS THEN
            IF SQLERRM LIKE 'BUSINESS_RULE_VIOLATION%' THEN
                RAISE NOTICE '✓ Test 4 PASS: Business rule correctly prevented update violation - %', SUBSTRING(SQLERRM FROM 1 FOR 100);
            ELSE
                RAISE NOTICE '✗ Test 4 FAIL: Unexpected error - %', SQLERRM;
            END IF;
    END;
    
    RAISE NOTICE '=== BUSINESS RULE TESTING COMPLETED ===';
    
    -- Final budget verification
    RAISE NOTICE 'Final row budget status:';
END $$;

-- Final validation of business rule enforcement and row budget
SELECT 
    'Business Rules Active' as check_type,
    COUNT(*) as value
FROM BUSINESS_LIMITS 
WHERE active = true

UNION ALL

SELECT 
    'Trigger Enforcement' as check_type,
    COUNT(*) as value
FROM information_schema.triggers 
WHERE event_object_table = 'rental_a'

UNION ALL

SELECT 
    'Total Committed Rows' as check_type,
    (SELECT COUNT(*) FROM Rental_A) +
    (SELECT COUNT(*) FROM Rental_B) +
    (SELECT COUNT(*) FROM Payment) +
    (SELECT COUNT(*) FROM HIER) +
    (SELECT COUNT(*) FROM TRIPLE) as value

UNION ALL

SELECT 
    'Budget Status' as check_type,
    CASE 
        WHEN (SELECT COUNT(*) FROM Rental_A) +
             (SELECT COUNT(*) FROM Rental_B) +
             (SELECT COUNT(*) FROM Payment) +
             (SELECT COUNT(*) FROM HIER) +
             (SELECT COUNT(*) FROM TRIPLE) <= 10 
        THEN 1
        ELSE 0
    END as value;

-- Customer rental summary for business rule context
SELECT 
    customer_id,
    COUNT(*) as total_rentals,
    SUM(amount) as total_amount,
    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_rentals,
    fn_should_alert(customer_id, 0, 'MAX_RENTAL_AMOUNT') as would_trigger_alert
FROM Rental_A
GROUP BY customer_id
ORDER BY total_amount DESC;