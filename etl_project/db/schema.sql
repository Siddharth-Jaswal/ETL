CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) NOT NULL,
    roll_no VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone_number VARCHAR(20) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_records_roll_no ON records(roll_no);
CREATE INDEX IF NOT EXISTS idx_records_email ON records(email);
