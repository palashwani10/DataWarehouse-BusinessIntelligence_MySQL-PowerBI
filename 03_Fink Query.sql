CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `dwbi`.`flink_query1` AS
    SELECT 
        `dwbi`.`fact_sales_order`.`order_id` AS `Order_ID`,
        `dwbi`.`fact_sales_order`.`hub_id` AS `Hub_ID`,
        `dwbi`.`dim_hub`.`hub_name` AS `Hub_Name`,
        `dwbi`.`fact_sales_order`.`customer_id` AS `Customer_ID`,
        `dwbi`.`fact_sales_order`.`picker_id` AS `Picker_Id`,
        `dwbi`.`dim_warehouse_associate`.`picker_name` AS `Picker_Name`,
        `dwbi`.`fact_sales_order`.`rider_id` AS `Rider_Id`,
        `dwbi`.`dim_delivery_associate`.`rider_name` AS `Rider_Name`,
        `dwbi`.`fact_sales_order`.`order_date` AS `Order_Date`,
        `dwbi`.`fact_sales_order`.`order_quantity` AS `Order_Quantity`,
        `dwbi`.`fact_sales_order`.`order_value` AS `Order_Value`,
        `dwbi`.`fact_sales_order`.`order_placed_ts` AS `Order_Placed_TS`,
        `dwbi`.`fact_sales_order`.`order_assigned_ts` AS `Order_Assigned_TS`,
        `dwbi`.`fact_sales_order`.`picker_start_ts` AS `Picker_Start_TS`,
        `dwbi`.`fact_sales_order`.`picker_end_ts` AS `Picker_End_TS`,
        `dwbi`.`fact_sales_order`.`expected_delivery_ts` AS `Expected_Delivery_TS`,
        `dwbi`.`fact_sales_order`.`rider_pickup_ts` AS `Rider_Pickup_TS`,
        `dwbi`.`fact_sales_order`.`rider_drop_ts` AS `Rider_Drop_TS`,
        `dwbi`.`fact_sales_order`.`distance_km` AS `Distance_KMs`,
        `dwbi`.`fact_sales_order`.`issue_flag` AS `Issue_Flag`,
        TIMESTAMPDIFF(MINUTE,
            `dwbi`.`fact_sales_order`.`order_placed_ts`,
            `dwbi`.`fact_sales_order`.`rider_drop_ts`) AS `Deliver_Time`,
        (CASE
            WHEN (`dwbi`.`fact_sales_order`.`rider_drop_ts` <= `dwbi`.`fact_sales_order`.`expected_delivery_ts`) THEN 'Yes'
            ELSE 'No'
        END) AS `On_Time_Delivery_Flag`,
        TIMESTAMPDIFF(MINUTE,
            `dwbi`.`fact_sales_order`.`order_assigned_ts`,
            `dwbi`.`fact_sales_order`.`picker_start_ts`) AS `Picker_Reaction_Time_Min`,
        TIMESTAMPDIFF(MINUTE,
            `dwbi`.`fact_sales_order`.`picker_start_ts`,
            `dwbi`.`fact_sales_order`.`picker_end_ts`) AS `Picking_Speed_Min`
    FROM
        (((`dwbi`.`fact_sales_order`
        LEFT JOIN `dwbi`.`dim_hub` ON ((`dwbi`.`fact_sales_order`.`hub_id` = `dwbi`.`dim_hub`.`hub_id`)))
        LEFT JOIN `dwbi`.`dim_warehouse_associate` ON (((`dwbi`.`fact_sales_order`.`picker_id` = `dwbi`.`dim_warehouse_associate`.`picker_id`)
            AND (`dwbi`.`fact_sales_order`.`hub_id` = `dwbi`.`dim_warehouse_associate`.`hub_id`))))
        LEFT JOIN `dwbi`.`dim_delivery_associate` ON (((`dwbi`.`fact_sales_order`.`rider_id` = `dwbi`.`dim_delivery_associate`.`rider_id`)
            AND (`dwbi`.`fact_sales_order`.`hub_id` = `dwbi`.`dim_delivery_associate`.`hub_id`))))