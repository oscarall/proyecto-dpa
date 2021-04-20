CREATE SCHEMA IF NOT EXISTS dpa;

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

INSERT INTO dpa.lu_step (name) 
VALUES 	('ingesta'), 
		('almacenamiento'), 
		('limpieza'), 
		('feature engineering');