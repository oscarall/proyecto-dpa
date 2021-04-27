CREATE SCHEMA IF NOT EXISTS dpa;

DROP TABLE IF EXISTS dpa.task_metadata;
DROP TABLE IF EXISTS dpa.test_metadata;
DROP TABLE IF EXISTS dpa.lu_step;

CREATE TABLE IF NOT EXISTS dpa.lu_step (
	id		SERIAL PRIMARY KEY,
	name	VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dpa.task_metadata (
	id			SERIAL PRIMARY KEY,
	step		INTEGER NOT NULL,
	task_id 	VARCHAR(256) NOT NULL,
	date		TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	metadata	JSON NOT NULL,
	CONSTRAINT fk_step FOREIGN KEY (step) REFERENCES dpa.lu_step(id)
);

CREATE TABLE IF NOT EXISTS dpa.test_metadata (
	id			SERIAL PRIMARY KEY,
	step		INTEGER NOT NULL,
	task_id 	VARCHAR(256) NOT NULL,
	date		TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	tests		INTEGER NOT NULL,
	CONSTRAINT fk_test_step FOREIGN KEY (step) REFERENCES dpa.lu_step(id)
);

INSERT INTO dpa.lu_step (name) 
VALUES 	('ingesta'), 
		('almacenamiento'), 
		('limpieza'), 
		('feature engineering'),
		('entrenamiento'),
		('seleccion de modelo');