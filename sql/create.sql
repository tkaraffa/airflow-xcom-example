CREATE TABLE airflow_example(
	id			INT	GENERATED ALWAYS AS IDENTITY,
	column1 INT,
	column2 INT
);

INSERT INTO airflow_example(column1, column2)
VALUES (1111, 111111);

INSERT INTO airflow_example(column1, column2)
VALUES (2222, 222222);
