DEFINE CSVEXCELStorage org.apache.pig.piggybank.storage.CSVEXCELStorage;

orders = LOAD '/user/maria_dev/diplomacy/orders.csv' USING CSVEXCELStorage() AS(
game_id:int,
unit_id:int,
unit_order:chararray,
location:chararray,
target:chararray,
target_dest:chararray,
success:int,
reason:int,
turn_num:int
);

filtered_orders = FILTER orders BY target =='Holland';

grouped_orders = GROUP filtered_orders BY(location, target);

unordered_orders = FOREACH grouped_orders GENERATE group, COUNT(filtered_orders);

ordered_orders = ORDER unordered_orders BY $0 ASC;

DUMP ordered_orders;