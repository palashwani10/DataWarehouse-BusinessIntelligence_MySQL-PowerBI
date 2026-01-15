CREATE VIEW `flink_query` AS SELECT
     `fact_sales_order`.`order_id`,
    `fact_sales_order`.`customer_id`,
    `fact_sales_order`.`picker_id`,
    `fact_sales_order`.`rider_id`,
    `fact_sales_order`.`order_date`,
    `fact_sales_order`.`order_quantity`,
    `fact_sales_order`.`order_value`,
    `fact_sales_order`.`order_placed_ts`,
    `fact_sales_order`.`order_assigned_ts`,
    `fact_sales_order`.`picker_start_ts`,
    `fact_sales_order`.`picker_end_ts`,
    `fact_sales_order`.`expected_delivery_ts`,
    `fact_sales_order`.`rider_pickup_ts`,
    `fact_sales_order`.`rider_drop_ts`,
    `fact_sales_order`.`distance_km`,
    `fact_sales_order`.`issue_flag`
FROM `dwbi`.`fact_sales_order`;
FROM fact_sales_order
