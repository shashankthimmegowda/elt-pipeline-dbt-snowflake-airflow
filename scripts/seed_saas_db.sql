-- Seed data for simulated SaaS source database
-- This creates realistic tables that the extractor pulls from

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100) UNIQUE NOT NULL,
    full_name VARCHAR(200),
    plan_type VARCHAR(20) DEFAULT 'free',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    price NUMERIC(10, 2) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    status VARCHAR(20) DEFAULT 'pending',
    total_amount NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    payment_method VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    plan_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    start_date DATE NOT NULL,
    end_date DATE,
    monthly_amount NUMERIC(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    event_type VARCHAR(100) NOT NULL,
    event_properties JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Seed users
INSERT INTO users (email, username, full_name, plan_type, is_active, created_at) VALUES
('alice@example.com', 'alice', 'Alice Johnson', 'pro', true, NOW() - INTERVAL '180 days'),
('bob@example.com', 'bob', 'Bob Smith', 'starter', true, NOW() - INTERVAL '120 days'),
('carol@example.com', 'carol', 'Carol Williams', 'enterprise', true, NOW() - INTERVAL '90 days'),
('dave@example.com', 'dave', 'Dave Brown', 'free', true, NOW() - INTERVAL '60 days'),
('eve@example.com', 'eve', 'Eve Davis', 'pro', true, NOW() - INTERVAL '30 days'),
('frank@example.com', 'frank', 'Frank Miller', 'starter', false, NOW() - INTERVAL '200 days'),
('grace@example.com', 'grace', 'Grace Wilson', 'free', true, NOW() - INTERVAL '15 days'),
('hank@example.com', 'hank', 'Hank Moore', 'enterprise', true, NOW() - INTERVAL '250 days'),
('iris@example.com', 'iris', 'Iris Taylor', 'pro', true, NOW() - INTERVAL '45 days'),
('jack@example.com', 'jack', 'Jack Anderson', 'starter', true, NOW() - INTERVAL '10 days');

-- Seed products
INSERT INTO products (name, category, price, is_active) VALUES
('Basic Widget', 'widgets', 9.99, true),
('Pro Widget', 'widgets', 29.99, true),
('Enterprise Widget', 'widgets', 99.99, true),
('Data Connector', 'integrations', 49.99, true),
('API Access', 'integrations', 19.99, true),
('Premium Support', 'services', 149.99, true),
('Training Course', 'services', 299.99, true),
('Custom Dashboard', 'analytics', 79.99, true),
('Reporting Suite', 'analytics', 59.99, true),
('Storage Addon', 'infrastructure', 14.99, true);

-- Seed orders
INSERT INTO orders (user_id, status, total_amount, currency, payment_method, created_at) VALUES
(1, 'delivered', 59.98, 'USD', 'credit_card', NOW() - INTERVAL '170 days'),
(1, 'delivered', 29.99, 'USD', 'credit_card', NOW() - INTERVAL '140 days'),
(2, 'delivered', 49.99, 'USD', 'paypal', NOW() - INTERVAL '110 days'),
(3, 'delivered', 299.99, 'USD', 'credit_card', NOW() - INTERVAL '85 days'),
(1, 'delivered', 149.99, 'USD', 'credit_card', NOW() - INTERVAL '60 days'),
(4, 'shipped', 9.99, 'USD', 'paypal', NOW() - INTERVAL '30 days'),
(5, 'confirmed', 79.99, 'USD', 'credit_card', NOW() - INTERVAL '15 days'),
(3, 'delivered', 59.99, 'USD', 'credit_card', NOW() - INTERVAL '10 days'),
(2, 'pending', 19.99, 'USD', 'paypal', NOW() - INTERVAL '2 days'),
(8, 'delivered', 399.98, 'USD', 'credit_card', NOW() - INTERVAL '200 days'),
(9, 'confirmed', 129.98, 'USD', 'credit_card', NOW() - INTERVAL '40 days'),
(7, 'pending', 9.99, 'USD', 'paypal', NOW() - INTERVAL '1 day');

-- Seed order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 2, 9.99), (1, 5, 1, 19.99), (1, 2, 1, 29.99),
(2, 2, 1, 29.99),
(3, 4, 1, 49.99),
(4, 7, 1, 299.99),
(5, 6, 1, 149.99),
(6, 1, 1, 9.99),
(7, 8, 1, 79.99),
(8, 9, 1, 59.99),
(9, 5, 1, 19.99),
(10, 7, 1, 299.99), (10, 3, 1, 99.99),
(11, 8, 1, 79.99), (11, 4, 1, 49.99),
(12, 1, 1, 9.99);

-- Seed subscriptions
INSERT INTO subscriptions (user_id, plan_type, status, start_date, monthly_amount) VALUES
(1, 'pro', 'active', CURRENT_DATE - INTERVAL '180 days', 29.99),
(2, 'starter', 'active', CURRENT_DATE - INTERVAL '120 days', 9.99),
(3, 'enterprise', 'active', CURRENT_DATE - INTERVAL '90 days', 99.99),
(5, 'pro', 'active', CURRENT_DATE - INTERVAL '30 days', 29.99),
(6, 'starter', 'cancelled', CURRENT_DATE - INTERVAL '200 days', 9.99),
(8, 'enterprise', 'active', CURRENT_DATE - INTERVAL '250 days', 99.99),
(9, 'pro', 'active', CURRENT_DATE - INTERVAL '45 days', 29.99);

-- Seed events
INSERT INTO events (user_id, event_type, event_properties, created_at) VALUES
(1, 'page_view', '{"page": "/dashboard"}', NOW() - INTERVAL '1 day'),
(1, 'feature_used', '{"feature": "export_csv"}', NOW() - INTERVAL '1 day'),
(2, 'page_view', '{"page": "/settings"}', NOW() - INTERVAL '2 days'),
(3, 'api_call', '{"endpoint": "/v1/data", "method": "GET"}', NOW() - INTERVAL '1 day'),
(5, 'page_view', '{"page": "/dashboard"}', NOW() - INTERVAL '3 hours'),
(7, 'signup_completed', '{"source": "organic"}', NOW() - INTERVAL '15 days'),
(9, 'feature_used', '{"feature": "custom_report"}', NOW() - INTERVAL '6 hours'),
(1, 'page_view', '{"page": "/analytics"}', NOW() - INTERVAL '12 hours'),
(3, 'api_call', '{"endpoint": "/v1/users", "method": "POST"}', NOW() - INTERVAL '5 hours'),
(8, 'page_view', '{"page": "/billing"}', NOW() - INTERVAL '1 day');
