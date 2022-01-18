create schema raw_data_schema;

create schema filtered_data_schema;


create table raw_data_schema.green_taxi_api_tabular
	(
		vendorid smallint,
		lpep_pickup_datetime timestamp,
		lpep_dropoff_datetime timestamp,
		store_and_fwd_flag varchar(20),
		ratecodeid smallint,
		pickup_longitude float,
		pickup_latitude float,
		dropoff_longitude float,
		dropoff_latitude float,
		passenger_count int,
		trip_distance float,
		fare_amount float,
		extra float,
		mta_tax float,
		tip_amount float,
		tolls_amount float,
		improvement_surcharge float,
		total_amount float,
		payment_type smallint,
		trip_type smallint
	);

create table raw_data_schema.yellow_taxi_trip_records_csv
	(
		VendorID smallint,
		tpep_pickup_datetime timestamp,
		tpep_dropoff_datetime timestamp,
		Passenger_count smallint,
		Trip_distance float,
		RateCodeID smallint,
		store_and_fwd_flag varchar(1),
		PULocationID smallint,
		DOLocationID smallint,
		payment_type smallint,
		fare_amount float,
		extra float,
		mta_tax float,
		tip_amount float,
		tolls_amount float,
		improvement_surcharge float,
		total_amount float,
		congestion_surcharge float
	);

create table raw_data_schema.green_taxi_trip_records_csv
	(
		VendorID smallint,
		lpep_pickup_datetime timestamp,
		lpep_dropoff_datetime timestamp,
		store_and_fwd_flag varchar(1),
		RatecodeID smallint,
		PULocationID smallint,
		DOLocationID smallint,
		passenger_count smallint,
		trip_distance float,
		fare_amount float,
		extra float,
		mta_tax float,
		tip_amount float,
		tolls_amount float,
		ehail_fee float,
		improvement_surcharge float,
		total_amount float,
		payment_type smallint,
		trip_type smallint,
		congestion_surcharge float
	);

create table raw_data_schema.for_hire_vehicle_trip_records_csv
	(
		dispatching_base_num varchar,
		pickup_datetime timestamp,
		dropoff_datetime timestamp,
		PULocationID int,
		DOLocationID int,
		SR_Flag varchar(1),
		Affiliated_base_number varchar
	);

create table raw_data_schema.high_volume_for_hire_vehicle_trip_records_csv
	(
		hvfhs_license_num varchar(6),
		dispatching_base_num varchar(6),
		pickup_datetime timestamp,
		dropoff_datetime timestamp,
		PULocationID smallint,
		DOLocationID smallint,
		SR_Flag smallint
	);

create table filtered_data_schema.combined_trip_records
	(
		pickup_datetime timestamp,
		dropoff_datetime timestamp,
		passenger_count int,
		total_amount float,
		trip_distance float
	);







--INSERT TRIGGERS-------------------------------------------------------------------------
create trigger new_green_api
	after insert on raw_data_schema.green_taxi_api_tabular
	for each row
	execute procedure filtered_data_schema.add_from_green_api()
	
create trigger new_green_csv
	after insert on raw_data_schema.green_taxi_trip_records_csv
	for each row
	execute procedure filtered_data_schema.add_from_green_csv()
	
create trigger new_yellow_csv
	after insert on raw_data_schema.yellow_taxi_trip_records_csv
	for each row
	execute procedure filtered_data_schema.add_from_yellow_csv()
	



	
	
	
	
--INSERT FUNCTIONS	------------------------------------------------------------
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
