
# Vehicle Rental & Maintenance Tracking System - Database Project Report

## 1. Introduction
This project presents a Vehicle Rental & Maintenance Tracking System designed to manage vehicle rentals, customer details, staff assignments, payments, and maintenance activities. The system ensures data consistency between rentals, maintenance, and payments, supporting efficient vehicle management and reporting.

## 2. Objectives
- Design and implement a relational database for a vehicle rental company.
- Ensure referential integrity and cascading relationships.
- Track vehicle rentals, maintenance, and payments effectively.
- Generate analytical reports on utilization, revenue, and maintenance costs.

## 3. Database Design
The database consists of six main tables:
1. Customer(CustomerID, FullName, LicenseNo, Contact, Address)
2. Vehicle(VehicleID, Model, Make, Type, DailyRate, Status)
3. Staff(StaffID, FullName, Role, Phone)
4. Rental(RentalID, CustomerID, VehicleID, StaffID, StartDate, EndDate, TotalCost)
5. Payment(PaymentID, RentalID, Amount, PaymentDate, Method)
6. Maintenance(MaintenanceID, VehicleID, StaffID, Date, Cost, Description)

### Key Relationships
- Customer → Rental (1:N)
- Vehicle → Rental (1:N)
- Staff → Rental (1:N)
- Rental → Payment (1:1)
- Vehicle → Maintenance (1:N)
- Staff → Maintenance (1:N)

## 4. Constraints and Integrity Rules
- Primary and foreign key constraints are enforced.
- CASCADE DELETE is applied on Rental → Payment and Vehicle → Maintenance.
- Each payment is uniquely tied to a rental record.
- Vehicles marked as 'Unavailable' cannot be rented again until released.

## 5. Sample Data Population
Sample data includes:
- 10 Rental records linking customers, vehicles, and staff.
- 3 Staff members managing both rentals and maintenance.
- Sample Vehicle data across different types (Sedan, SUV, Van, Truck).

## 6. Key Queries
a) Vehicle utilization and total revenue by vehicle type.  
b) Update maintenance record and observe vehicle availability.  
c) Identify staff with the highest total maintenance cost.  
d) Create a view summarizing revenue and maintenance cost per vehicle.  

## 7. Trigger Implementation
A trigger automatically sets vehicle status to 'Unavailable' during active rentals, ensuring no overlapping rentals occur for the same vehicle.

## 8. View Creation
A SQL view consolidates total revenue and maintenance costs per vehicle, supporting managerial insights into profitability and operational costs.

## 9. Results and Analysis
- Trigger maintains consistency in vehicle availability.
- CASCADE DELETE ensures dependent data is removed automatically.
- Queries reveal top-performing vehicle types and maintenance trends.
- Staff performance is evaluated based on maintenance cost handled.

## 10. Conclusion
The Vehicle Rental & Maintenance Tracking System effectively manages rentals, payments, and maintenance workflows. With enforced integrity, automated triggers, and analytical views, the system ensures reliable data management and operational efficiency.
