--AFTER INSERT TRIGGERS
create trigger new_green_api
	after insert on raw_data_schema.green_taxi_api_tabular
	for each row
	execute procedure filtered_data_schema.add_from_green_api();
	
create trigger new_green_csv
	after insert on raw_data_schema.green_taxi_trip_records_csv
	for each row
	execute procedure filtered_data_schema.add_from_green_csv();
	
create trigger new_yellow_csv
	after insert on raw_data_schema.yellow_taxi_trip_records_csv
	for each row
	execute procedure filtered_data_schema.add_from_yellow_csv();
	


	
	
--TRIGGER FUNCTIONS	
create or replace function filtered_data_schema.add_from_green_api()
returns trigger
as
$$
begin

insert into filtered_data_schema.combined_trip_records
	(pickup_datetime,
	 dropoff_datetime,
	 passenger_count,
	 total_amount,
	 trip_distance)
values
	(new.lpep_pickup_datetime,
	 NEW.lpep_dropoff_datetime,
	 new.passenger_count,
	 new.total_amount,
	 new.trip_distance);

return new;
end;
$$
language plpgsql;



create or replace function filtered_data_schema.add_from_green_csv()
returns trigger
as
$$
begin

insert into filtered_data_schema.combined_trip_records
	(pickup_datetime,
	 dropoff_datetime,
	 passenger_count,
	 total_amount,
	 trip_distance)
values
	(new.lpep_pickup_datetime,
	 NEW.lpep_dropoff_datetime,
	 new.passenger_count,
	 new.total_amount,
	 new.trip_distance);

return new;
end;
$$
language plpgsql;



create or replace function filtered_data_schema.add_from_yellow_csv()
returns trigger
as
$$
begin

insert into filtered_data_schema.combined_trip_records
	(pickup_datetime,
	 dropoff_datetime,
	 passenger_count,
	 total_amount,
	 trip_distance)
values
	(new.tpep_pickup_datetime,
	 NEW.tpep_dropoff_datetime,
	 new.Passenger_count,
	 new.total_amount,
	 new.Trip_distance);

return new;
end;
$$
language plpgsql;
