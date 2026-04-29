# MongoDB Aggregation Pipeline — Query Output

---

## Task 1: Emergency Fuel Report

**Query:** Vehicles with status `"In Transit"` and `fuelLevel < 50`.

![Task 1 Output - Part 1](task1_1.png)
![Task 1 Output - Part 2](task1_2.png)

---

## Task 2: Maintenance Prioritization

**Query:** Vehicles in `"Maintenance"`, sorted by `lastServiceDate` ascending. `activeAlerts` renamed to `issues`.

![Task 2 Output - Part 1](task2_1.png)
![Task 2 Output - Part 2](task2_2.png)

---

## Task 3: Electric Fleet Geo-Audit

**Query:** All electric vehicles with extracted `lon` and `lat` from `location.coordinates`.

![Task 3 Output - Part 1](task3_1.png)
![Task 3 Output - Part 2](task3_2.png)
![Task 3 Output - Part 3](task3_3.png)
![Task 3 Output - Part 4](task3_4.png)

---

## Task 4: High-Risk Truck Report

**Query:** Top 3 Semi-Trucks by `alertCount` descending, with computed `needsUrgentRefuel` boolean.

![Task 4 Output](task4_1.png)
