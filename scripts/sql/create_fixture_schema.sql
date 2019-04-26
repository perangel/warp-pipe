-- Create a 'users' table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    UNIQUE (email)
);

CREATE INDEX users_email_idx ON users (email);

-- Create a 'pets' table
CREATE TABLE pets (
    id SERIAL PRIMARY KEY,
    name TEXT,
    owner_id INTEGER REFERENCES users(id) ON DELETE CASCADE
);
