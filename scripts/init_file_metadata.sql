CREATE TABLE if not exists file_metadata (
    id SERIAL PRIMARY KEY,
    data_path VARCHAR(500) NOT NULL,
    number_of_files INTEGER NOT NULL,
    average_files_size DECIMAL(10,2) NOT NULL,
    dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);