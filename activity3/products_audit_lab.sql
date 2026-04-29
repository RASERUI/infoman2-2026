-- 1. Drop tables if they exist
DROP TABLE IF EXISTS products_audit;
DROP TABLE IF EXISTS products;

-- 2. Create main products table
CREATE TABLE products (
    product_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    stock_quantity INT NOT NULL CHECK (stock_quantity >= 0),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_modified TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Create audit table
CREATE TABLE products_audit (
    audit_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id INT NOT NULL,
    change_type TEXT NOT NULL, -- INSERT, UPDATE, DELETE
    old_name TEXT,
    new_name TEXT,
    old_price NUMERIC(10, 2),
    new_price NUMERIC(10, 2),
    change_timestamp TIMESTAMPTZ DEFAULT NOW(),
    db_user TEXT DEFAULT current_user
);

-- 4. Insert initial data
INSERT INTO products (name, description, price, stock_quantity) VALUES
('Super Widget', 'A high-quality widget for all your needs.', 29.99, 100),
('Mega Gadget', 'The latest and greatest gadget.', 199.50, 50),
('Basic Gizmo', 'A simple gizmo for everyday tasks.', 9.75, 250);

-- 5. Create audit trigger function
CREATE OR REPLACE FUNCTION log_product_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO products_audit(product_id, change_type, new_name, new_price)
        VALUES (NEW.product_id, 'INSERT', NEW.name, NEW.price);
        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO products_audit(product_id, change_type, old_name, old_price)
        VALUES (OLD.product_id, 'DELETE', OLD.name, OLD.price);
        RETURN OLD;

    ELSIF TG_OP = 'UPDATE' THEN
        IF NEW.name IS DISTINCT FROM OLD.name OR NEW.price IS DISTINCT FROM OLD.price THEN
            INSERT INTO products_audit(
                product_id, change_type, old_name, new_name, old_price, new_price
            ) VALUES (
                OLD.product_id, 'UPDATE', OLD.name, NEW.name, OLD.price, NEW.price
            );
        END IF;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 6. Create audit trigger
CREATE TRIGGER product_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON products
FOR EACH ROW
EXECUTE FUNCTION log_product_changes();

-- 7. Test triggers

-- Test INSERT
INSERT INTO products (name, description, price, stock_quantity)
VALUES ('Miniature Thingamabob', 'A very small thingamabob.', 4.99, 500);

-- Test UPDATE with meaningful change
UPDATE products
SET price = 225.00, name = 'Mega Gadget v2'
WHERE name = 'Mega Gadget';

-- Test UPDATE with no meaningful change (description only)
UPDATE products
SET description = 'An even simpler gizmo for all your daily tasks.'
WHERE name = 'Basic Gizmo';

-- Test DELETE
DELETE FROM products
WHERE name = 'Super Widget';

-- 8. Verify audit table
SELECT * FROM products_audit ORDER BY audit_id;
