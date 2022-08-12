
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

<<<<<<< HEAD:databricks_dbt_bid_optimization/models/bids/my_first_dbt_model.sql
{{ config(materialized='table') }}


select *
from rbt_2
=======
{{ config(materialized='table', file_format='delta') }}

select *
from rtb_2
>>>>>>> 99d78d1525a0b1c9c19ead218b09c63d15cb965a:databricks_dbt_bid_optimization/models/bids/bids_bronze.sql

