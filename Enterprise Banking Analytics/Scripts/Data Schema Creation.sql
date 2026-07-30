CREATE DATABASE core_banking;
CREATE DATABASE credit_loans;

-- CREATING core_banking tables

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_uuid UUID UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    phone_number VARCHAR(30),
    date_of_birth DATE,
    city VARCHAR(50),
    postcode VARCHAR(15),
    country VARCHAR(50) DEFAULT 'United Kingdom',
    employment_status VARCHAR(30),
    annual_income DECIMAL(12, 2),
    customer_segment VARCHAR(30),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    account_uuid UUID UNIQUE NOT NULL,
    customer_uuid UUID NOT NULL,
    account_type VARCHAR(30) NOT NULL,
    interest_rate_apr DECIMAL(5, 2),
    monthly_maintenance_fee DECIMAL(6, 2),
    current_balance DECIMAL(15, 2),
    currency VARCHAR(3) DEFAULT 'GBP',
    status VARCHAR(20) DEFAULT 'Active',
    opened_date DATE DEFAULT CURRENT_DATE,
    CONSTRAINT fk_accounts_customer FOREIGN KEY (customer_uuid) REFERENCES customers(customer_uuid)
);

CREATE TABLE transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    account_id INT NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    amount DECIMAL(12, 2),
    running_balance DECIMAL(15, 2),
    merchant_name VARCHAR(100),
    spending_sector VARCHAR(50),
    channel VARCHAR(30),
    transaction_city VARCHAR(50),
    transaction_timestamp TIMESTAMP NOT NULL,
    status VARCHAR(20) DEFAULT 'Completed',
    CONSTRAINT fk_transactions_account FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

CREATE TABLE fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    customer_uuid UUID NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    risk_score INT CHECK (risk_score BETWEEN 0 AND 100),
    status VARCHAR(30) DEFAULT 'Under Review',
    resolution_notes TEXT,
    flagged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fraud_transaction FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
    CONSTRAINT fk_fraud_customer FOREIGN KEY (customer_uuid) REFERENCES customers(customer_uuid)
);





-- Create database CREDIT & Loans

CREATE TABLE credit_scores (
    score_id SERIAL PRIMARY KEY,
    customer_uuid UUID UNIQUE NOT NULL,
    bureau_score INT CHECK (bureau_score BETWEEN 300 AND 850),
    risk_rating VARCHAR(20),
    delinquency_history_count INT DEFAULT 0,
    debt_to_income_ratio DECIMAL(5, 2),
    last_updated DATE DEFAULT CURRENT_DATE
);

CREATE TABLE credit_cards (
    card_id SERIAL PRIMARY KEY,
    card_number_masked VARCHAR(20) NOT NULL,
    customer_uuid UUID NOT NULL,
    credit_limit DECIMAL(12, 2),
    current_balance DECIMAL(12, 2),
    apr DECIMAL(5, 2),
    card_status VARCHAR(20) DEFAULT 'Active'
);

CREATE TABLE loan_applications (
    loan_id SERIAL PRIMARY KEY,
    customer_uuid UUID NOT NULL,
    loan_type VARCHAR(40) NOT NULL,
    principal_amount DECIMAL(15, 2),
    interest_rate DECIMAL(5, 2),
    term_months INT NOT NULL,
    start_date DATE DEFAULT CURRENT_DATE,
    loan_status VARCHAR(20) DEFAULT 'Active'
);

CREATE TABLE loan_repayments (
    repayment_id BIGSERIAL PRIMARY KEY,
    loan_id INT NOT NULL,
    payment_date TIMESTAMP NOT NULL,
    amount_paid DECIMAL(12, 2),
    principal_component DECIMAL(12, 2),
    interest_component DECIMAL(12, 2),
    late_fee_applied DECIMAL(8, 2) DEFAULT 0.00,
    CONSTRAINT fk_repayments_loan FOREIGN KEY (loan_id) REFERENCES loan_applications(loan_id)
);



-- Check core_banking counts
SELECT 'customers' AS table_name, COUNT(*) FROM core_banking.public.customers
UNION ALL
SELECT 'accounts', COUNT(*) FROM core_banking.public.accounts
UNION ALL
SELECT 'transactions', COUNT(*) FROM core_banking.public.transactions
UNION ALL
SELECT 'fraud_alerts', COUNT(*) FROM core_banking.public.fraud_alerts;




SELECT * from credit_cards
LIMIT 1000;



SELECT * from loan_repayments
LIMIT 500;


SELECT 'customers' AS table_name, COUNT(*) AS record_count FROM customers
UNION ALL
SELECT 'accounts', COUNT(*) FROM accounts
UNION ALL
SELECT 'transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'fraud_alerts', COUNT(*) FROM fraud_alerts
ORDER BY record_count DESC;






















