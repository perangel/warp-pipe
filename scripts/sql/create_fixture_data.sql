-- Insert some test users
INSERT INTO users (
    first_name,
    last_name,
    email
) VALUES
    ('Bob', 'Silver', 'bob@test.com'),
    ('Alice', 'Gold', 'alice@test.com'),
    ('Maria', 'Hierro', 'maria@test.com'),
    ('Wendy', 'Steel', 'wendy@test.com'),
    ('Silvio', 'Bronce', 'silvio@test.com');

-- Insert some test pets
INSERT INTO pets (
    name, owner_id
) VALUES
    ('Rex', 1),
    ('Fido', 2),
    ('Pichi', 3),
    ('Sambo', 4),
    ('Vincenzo', 5);
