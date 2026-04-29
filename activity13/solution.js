// ============================================================
// Lab Activity: MongoDB Aggregation Pipeline
// Scenario: Smart Fleet Management - Nexus Logistics
// ============================================================

// ------------------------------------------------------------
// SETUP: Import sample data into the vehicles collection
// Run this in mongosh to insert the documents before executing
// the queries below.
// ------------------------------------------------------------

// ------------------------------------------------------------
// Task 1: Emergency Fuel Report
// Find all "In Transit" vehicles with fuelLevel below 50%.
// Stage 1: $match  — filter by status and fuelLevel
// Stage 2: $project — show only vin, type, fuelLevel; hide _id
// ------------------------------------------------------------
db.vehicles.aggregate([
  {
    $match: {
      status: "In Transit",
      fuelLevel: { $lt: 50 }
    }
  },
  {
    $project: {
      _id: 0,
      vin: 1,
      type: 1,
      fuelLevel: 1
    }
  }
]);

// ------------------------------------------------------------
// Task 2: Maintenance Prioritization
// Identify vehicles currently in "Maintenance", sorted by
// lastServiceDate ascending (oldest first).
// Stage 1: $match       — filter by status "Maintenance"
// Stage 2: $project     — show vin, rename activeAlerts → issues,
//                         show lastServiceDate; hide _id
// Stage 3: $sort        — oldest lastServiceDate first
// ------------------------------------------------------------
db.vehicles.aggregate([
  {
    $match: {
      status: "Maintenance"
    }
  },
  {
    $project: {
      _id: 0,
      vin: 1,
      issues: "$activeAlerts",
      lastServiceDate: 1
    }
  },
  {
    $sort: {
      lastServiceDate: 1
    }
  }
]);

// ------------------------------------------------------------
// Task 3: Electric Fleet Geo-Audit
// Show location (lon, lat) of all electric vehicles.
// Stage 1: $match   — filter isElectric: true
// Stage 2: $project — extract lon (index 0) and lat (index 1)
//                     from location.coordinates array
// Stage 3: $project — hide location and _id
// (Combined into a single $project for efficiency)
// ------------------------------------------------------------
db.vehicles.aggregate([
  {
    $match: {
      isElectric: true
    }
  },
  {
    $project: {
      _id: 0,
      vin: 1,
      lon: { $arrayElemAt: ["$location.coordinates", 0] },
      lat: { $arrayElemAt: ["$location.coordinates", 1] }
    }
  }
]);

// ------------------------------------------------------------
// Task 4: High-Risk Truck Report (Mastery Challenge)
// Multi-stage pipeline for Semi-Trucks with computed fields.
// Stage 1: $match   — filter type "Semi-Truck"
// Stage 2: $project — compute alertCount (size of activeAlerts),
//                     compute needsUrgentRefuel (fuelLevel < 20),
//                     show only vin, alertCount, needsUrgentRefuel
// Stage 3: $sort    — most alerts first (descending)
// Stage 4: $limit   — top 3 high-risk trucks
// ------------------------------------------------------------
db.vehicles.aggregate([
  {
    $match: {
      type: "Semi-Truck"
    }
  },
  {
    $project: {
      _id: 0,
      vin: 1,
      alertCount: { $size: "$activeAlerts" },
      needsUrgentRefuel: { $lt: ["$fuelLevel", 20] }
    }
  },
  {
    $sort: {
      alertCount: -1
    }
  },
  {
    $limit: 3
  }
]);
