# License Category and Fee Data Migration Plan

## 1. Overview

This document outlines the plan for migrating license categories and their associated application fees from an Excel spreadsheet into the production database. The process is handled by the Python script `license_categories_import_service.py`, which orchestrates a robust, multi-step data import using a temporary staging table for performance and data integrity.

The primary goal is to populate two main tables:
1.  `public.license_categories`: Stores the names and metadata of different license categories.
2.  `public.application_category_fees`: Stores the detailed fee structures for each license category, including application fees, license fees, and other parameters.

## 2. Data Source and Preparation

### 2.1. Input File Format
The migration process starts with an Excel file. This file must be read and converted into a pandas DataFrame before being passed to the import script.

### 2.2. Required Data Columns
The script is strict about the columns present in the input file. Column names are automatically normalized (stripped of whitespace and converted to lowercase). The presence of the following columns is mandatory:

**Common Columns (Required for all sectors):**
- `categoryorclass`: The name of the license category.
- `appfee`: The application fee.
- `licencefee`: The license fee.
- `prefix`: The prefix for the application code.
- `licenseprefix`: The prefix for the license code.
- `licenseperiod_x` or `licenseperiod`: The duration for which the license is eligible (in months).

**Sector-Specific Columns:**
The column used to determine the `application_type` depends on the sector being processed:
- **Natural Gas / Water & Wastewater**: `applicationtype`
- **Electricity**: No column needed; the script hardcodes the `application_type` to 'NEW'.
- **Other Sectors**: `licencetype`

If any of these required columns are missing, the script will raise an error and halt the process.

## 3. The Migration Process

The migration is executed by the `import_license_categories_and_fees_via_staging_copy` function. The process can be broken down into the following key steps:

### Step 1: Initial Validation and Setup
1.  **Input Check**: The script first checks if the provided DataFrame is empty. If so, it exits immediately.
2.  **Sector Validation**: It verifies that the provided `sector_name` (e.g., 'Electricity', 'Natural Gas') exists in the `public.ca_sectors` table and retrieves its `sector_id`. If the sector is not found, the process stops.

### Step 2: Staging Table Creation
To handle data in bulk efficiently, the script creates a temporary staging table named `public.stage_license_category_fees_raw`.
- If this table already exists, it is **dropped and recreated**.
- The table schema is designed to hold the raw, text-based data from all possible columns in the Excel file.

### Step 3: Schema Synchronization
The script ensures that the destination tables (`public.license_categories` and `public.application_category_fees`) contain all necessary columns for the import. It runs `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` commands to add columns like `sub_sector_type`, `code`, `license_type`, etc. This makes the script resilient and self-contained, even if database migrations have not been run recently.

### Step 4: Bulk Load into Staging Table
- The data from the input DataFrame is prepared and then loaded into the `public.stage_license_category_fees_raw` table using a highly efficient `COPY` command. This is significantly faster than inserting rows one by one.
- After the `COPY` operation, the script checks if any rows were staged. If not, it reports that no data was processed and exits.

### Step 5: Upsert License Categories
This step populates the `public.license_categories` table.
1.  **Identify New Categories**: The script identifies all unique category names from the staging table that do not already exist in the `public.license_categories` table (based on a case-insensitive name match).
2.  **Insert New Categories**: For each new category, a new row is inserted into `public.license_categories`. The script sets default values for several fields:
    - `years_eligible`: 2
    - `sub_sector_type`: 'OPERATIONAL'
    - `category_type`: 'Construction' or 'License' based on the `licencetype` value.
    - `code`: Derived from the license prefix or the category name.
3.  **Deduplication**: The insertion uses an `ON CONFLICT (name) DO NOTHING` clause to prevent creating duplicate categories and avoid errors.

### Step 6: Map Category Names to IDs
To link fees to the correct categories, the script creates another temporary table (`stage_license_category_ids`) to store the mapping between the string name (`key_name`) of a category and its newly created or existing UUID (`category_id`). This mapping is crucial for the next step.

### Step 7: Insert Application Category Fees
This is the final and most complex step, where the `public.application_category_fees` table is populated.
1.  **Data Transformation**: The script reads from the staging table and joins with the ID mapping table. It applies significant business logic and data transformation rules:
    - **Type Conversion**: Fee and capacity values are safely cast to numeric types, with defaults applied if the source data is invalid (e.g., not a number).
    - **Application Type Logic**: The `application_type` is determined based on the sector-specific rules defined in Step 2.2.
    - **License Type Normalization**: For the 'Electricity' sector, `license_type` values are derived from the `licencetype` column and sanitized (converted to uppercase, special characters replaced with underscores). For all other sectors, it is hardcoded to 'OPERATIONAL'.
2.  **Deduplication**: Before inserting, the script identifies unique fee structures based on a combination of keys: `category_id`, `application_type`, capacity range, prefixes, and `months_eligible`. This prevents inserting duplicate fee rows.
3.  **Conflict Check**: The script then checks which of these unique fee structures **do not** already exist in the `public.application_category_fees` table.
4.  **Final Insert**: Only the new, unique fee structures are inserted into the `public.application_category_fees` table.

## 4. Post-Migration and Reporting

After the insertion steps are complete, the script calculates and returns a statistics dictionary containing:
- Total rows processed from the input file.
- Number of rows loaded into the staging table.
- Counts of newly inserted categories and fees.
- Estimated counts of skipped categories and fees that already existed.
- The total count of fees in the table before and after the operation.

This report is essential for verifying the outcome of the migration and diagnosing any issues.
