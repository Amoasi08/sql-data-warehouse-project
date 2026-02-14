/*
=======================================================================
Quality Checks
=======================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracym
  and standardization across the 'silver' schemas. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields. 
  - Data standardization and consistency.
  - Invalid date range and orders.
  - Data consistency between related fields.

Usage Notes: 
  - Run these checks agter data loading silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
=====================================================================
*/

-- ======================================================
-- Checking 'silver.crm_cust_info'
-- ======================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
