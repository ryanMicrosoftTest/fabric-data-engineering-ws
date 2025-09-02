CREATE TABLE employee (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    social_security_number VARCHAR(11) NOT NULL
);

CREATE TABLE student (
    id INT IDENTITY(1,1) PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    social_security_number VARCHAR(11) NOT NULL
);
