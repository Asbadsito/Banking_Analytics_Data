import os
import random
import uuid
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

# Initialize Faker with UK locale
fake = Faker('en_GB')

# --- CONFIGURATION ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASSWORD = *No password here, usually I would use env variables, but to push the script i removed it

# Batch size optimized for maximum PostgreSQL streaming speed
BATCH_SIZE = 10000

def get_connection(dbname):
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=dbname
    )

# --- HELPER UTILITIES FOR INTENTIONAL "DIRTY" RAW DATA ---

def introduce_string_dirt(text):
    """Injects casing inconsistencies or extra whitespace (~5% probability)."""
    if not text or random.random() > 0.05:
        return text
    choice = random.choice(['uppercase', 'lowercase', 'whitespace'])
    if choice == 'uppercase':
        return text.upper()
    elif choice == 'lowercase':
        return text.lower()
    elif choice == 'whitespace':
        return f"  {text}   "

def maybe_null(value, null_prob=0.03):
    """Randomly replaces a value with None (SQL NULL) based on probability."""
    return None if random.random() < null_prob else value


# --- ENTERPRISE DOMAIN REFERENCE ARRAYS ---
SPENDING_SECTORS = [
    'Groceries & Supermarkets', 'Restaurants & Fast Food', 'Utilities & Bills',
    'Public Transport', 'Aviation & Travel', 'Streaming & Subscriptions',
    'E-Commerce & Retail', 'Health & Pharmacy', 'Fuel & Automotive',
    'Home Improvement', 'Gaming & Entertainment', 'Luxury Goods', 'Charity & Donations'
]

TRANSACTION_TYPES = [
    'Debit Card Purchase', 'Faster Payment Out', 'Direct Debit',
    'ATM Cash Withdrawal', 'Bank Transfer In', 'Standing Order'
]

TRANSACTION_CHANNELS = [
    'Mobile Banking App', 'Web Portal', 'Contactless POS', 'Chip & PIN',
    'ATM Cash Withdrawal', 'Recurring Direct Debit', 'Open Banking API', 'Telephone Banking'
]

EMPLOYMENT_STATUSES = [
    'Full-Time Employed', 'Part-Time Employed', 'Self-Employed',
    'Contractor', 'Unemployed', 'Retired', 'Student', 'Freelancer'
]

LOAN_TYPES = [
    'Personal Unsecured Loan', 'Auto Loan', 'Residential Mortgage',
    'Home Improvement Loan', 'Debt Consolidation Loan', 'Green Energy Loan'
]


def populate_core_banking(total_customers=150000):
    """
    Populates core_banking database using fast batch inserts:
    Target: ~150k Customers, ~300k Accounts, ~3.5M+ Transactions, ~140k Fraud Alerts.
    """
    print(f"Connecting to 'core_banking' (Target: {total_customers:,} customers)...")
    conn = get_connection("core_banking")
    cur = conn.cursor()

    customer_uuids = []

    # 1. GENERATE CUSTOMERS
    print(f"Generating {total_customers:,} Customers in batch chunks...")
    cust_batch = []
    for i in range(1, total_customers + 1):
        c_uuid = str(uuid.uuid4())
        customer_uuids.append(c_uuid)

        cust_batch.append((
            c_uuid,
            introduce_string_dirt(fake.first_name()),
            introduce_string_dirt(fake.last_name()),
            introduce_string_dirt(fake.email()),
            maybe_null(fake.phone_number()[:30], null_prob=0.04),
            fake.date_of_birth(minimum_age=18, maximum_age=80),
            maybe_null(introduce_string_dirt(fake.city()), null_prob=0.02),
            fake.postcode(),
            random.choice(EMPLOYMENT_STATUSES),
            round(random.uniform(14000, 160000), 2),
            random.choice(['Standard', 'Mass Affluent', 'High Net Worth', 'Student/Youth'])
        ))

        if len(cust_batch) >= BATCH_SIZE:
            execute_values(cur, """
                INSERT INTO customers 
                (customer_uuid, first_name, last_name, email, phone_number, date_of_birth, city, postcode, employment_status, annual_income, customer_segment)
                VALUES %s
            """, cust_batch)
            conn.commit()
            cust_batch = []

    if cust_batch:
        execute_values(cur, """
            INSERT INTO customers 
            (customer_uuid, first_name, last_name, email, phone_number, date_of_birth, city, postcode, employment_status, annual_income, customer_segment)
            VALUES %s
        """, cust_batch)
        conn.commit()

    print(f"✓ Customers inserted: {len(customer_uuids):,}")

    # 2. GENERATE ACCOUNTS (~300,000 total)
    print("Generating Accounts...")
    raw_acc_data = []
    account_lookup = []  # Holds (account_id, customer_uuid)

    for c_uuid in customer_uuids:
        for _ in range(random.randint(1, 3)):
            a_uuid = str(uuid.uuid4())
            acc_type = random.choice(['Current Account', 'Instant Savings', 'Fixed ISA', 'Student Account'])
            apr = round(random.uniform(0.25, 6.50), 2) if 'Savings' in acc_type or 'ISA' in acc_type else 0.00
            fee = 0.00 if 'Student' in acc_type else float(random.choice([0.00, 3.00, 10.00, 25.00]))
            balance = round(random.uniform(-150.0, 45000.0), 2)
            raw_acc_data.append((a_uuid, c_uuid, acc_type, apr, fee, balance))

    for i in range(0, len(raw_acc_data), BATCH_SIZE):
        chunk = raw_acc_data[i:i + BATCH_SIZE]
        inserted = execute_values(cur, """
            INSERT INTO accounts 
            (account_uuid, customer_uuid, account_type, interest_rate_apr, monthly_maintenance_fee, current_balance)
            VALUES %s
            RETURNING account_id, customer_uuid
        """, chunk, fetch=True)
        account_lookup.extend(inserted)
        conn.commit()

    print(f"✓ Accounts inserted: {len(account_lookup):,}")

    # 3. GENERATE TIME-SERIES TRANSACTIONS & FRAUD ALERTS (~3.5M+ total)
    print("Generating Time-Series Transactions & Fraud Alerts (~3.5M+ rows)...")
    tx_batch = []
    fraud_batch = []

    start_date = datetime.now() - timedelta(days=730) # 2-year rolling window

    for idx, (acc_id, c_uuid) in enumerate(account_lookup, start=1):
        num_tx = random.randint(12, 35)
        running_bal = round(random.uniform(200.0, 15000.0), 2)

        for _ in range(num_tx):
            # Log-normal spend distribution: mostly everyday low spend, occasional large outlier
            amount = round(random.lognormvariate(3.2, 1.1), 2) + 0.50
            if amount > 15000:
                amount = 15000.00

            t_type = random.choice(TRANSACTION_TYPES)
            merchant = maybe_null(introduce_string_dirt(fake.company()), null_prob=0.03)
            sector = introduce_string_dirt(random.choice(SPENDING_SECTORS))
            channel = random.choice(TRANSACTION_CHANNELS)
            t_city = maybe_null(fake.city(), null_prob=0.03)

            random_days = random.uniform(0, 730)
            timestamp = start_date + timedelta(days=random_days)

            tx_batch.append((
                acc_id, t_type, amount, running_bal, merchant,
                sector, channel, t_city, timestamp, c_uuid
            ))

        if len(tx_batch) >= BATCH_SIZE:
            db_tx_data = [item[:-1] for item in tx_batch]

            inserted_txs = execute_values(cur, """
                INSERT INTO transactions 
                (account_id, transaction_type, amount, running_balance, merchant_name, spending_sector, channel, transaction_city, transaction_timestamp)
                VALUES %s
                RETURNING transaction_id
            """, db_tx_data, fetch=True)

            for (tx_id,), tx_item in zip(inserted_txs, tx_batch):
                if random.random() < 0.04:  # 4% fraud rate
                    alert_type = random.choice([
                        'Unusual Foreign Location', 'Velocity Threshold Exceeded',
                        'High-Risk Merchant Sector', 'Impossible Travel Speed', 'Unusually High Amount'
                    ])
                    risk_score = random.randint(55, 99)
                    cust_uuid_ref = tx_item[-1]
                    fraud_batch.append((tx_id, cust_uuid_ref, alert_type, risk_score))

            conn.commit()
            tx_batch = []

            if len(fraud_batch) >= BATCH_SIZE:
                execute_values(cur, """
                    INSERT INTO fraud_alerts 
                    (transaction_id, customer_uuid, alert_type, risk_score)
                    VALUES %s
                """, fraud_batch)
                conn.commit()
                fraud_batch = []

    # Flush remaining transaction queue
    if tx_batch:
        db_tx_data = [item[:-1] for item in tx_batch]
        inserted_txs = execute_values(cur, """
            INSERT INTO transactions 
            (account_id, transaction_type, amount, running_balance, merchant_name, spending_sector, channel, transaction_city, transaction_timestamp)
            VALUES %s
            RETURNING transaction_id
        """, db_tx_data, fetch=True)

        for (tx_id,), tx_item in zip(inserted_txs, tx_batch):
            if random.random() < 0.04:
                alert_type = random.choice([
                    'Unusual Foreign Location', 'Velocity Threshold Exceeded',
                    'High-Risk Merchant Sector', 'Impossible Travel Speed', 'Unusually High Amount'
                ])
                risk_score = random.randint(55, 99)
                cust_uuid_ref = tx_item[-1]
                fraud_batch.append((tx_id, cust_uuid_ref, alert_type, risk_score))

        conn.commit()

    if fraud_batch:
        execute_values(cur, """
            INSERT INTO fraud_alerts 
            (transaction_id, customer_uuid, alert_type, risk_score)
            VALUES %s
        """, fraud_batch)
        conn.commit()

    print("✓ core_banking database successfully populated!")
    cur.close()
    conn.close()
    return customer_uuids


def populate_credit_loans(customer_uuids):
    """
    Populates credit_loans database:
    Credit Scores (150k), Credit Cards (~180k), Loan Applications & Repayments (~450k).
    """
    print(f"Connecting to 'credit_loans' database...")
    conn = get_connection("credit_loans")
    cur = conn.cursor()

    # 1. CREDIT SCORES (150,000 rows)
    print("Generating Credit Scores...")
    score_batch = []
    for c_uuid in customer_uuids:
        score = random.randint(300, 850)
        rating = 'Excellent' if score > 740 else ('Good' if score > 670 else ('Fair' if score > 580 else 'Poor'))
        delinquencies = random.choices([0, 1, 2, 3, 5], weights=[70, 15, 8, 5, 2])[0]
        dti = round(random.uniform(5.0, 58.0), 2)
        score_batch.append((c_uuid, score, rating, delinquencies, dti))

        if len(score_batch) >= BATCH_SIZE:
            execute_values(cur, """
                INSERT INTO credit_scores 
                (customer_uuid, bureau_score, risk_rating, delinquency_history_count, debt_to_income_ratio)
                VALUES %s
            """, score_batch)
            conn.commit()
            score_batch = []

    if score_batch:
        execute_values(cur, """
            INSERT INTO credit_scores 
            (customer_uuid, bureau_score, risk_rating, delinquency_history_count, debt_to_income_ratio)
            VALUES %s
        """, score_batch)
        conn.commit()

    # 2. CREDIT CARDS (~180,000 rows)
    print("Generating Credit Cards...")
    card_batch = []
    for c_uuid in customer_uuids:
        if random.random() < 0.60:
            for _ in range(random.randint(1, 2)):
                card_num = fake.credit_card_number()
                masked_card = f"****-****-****-{card_num[-4:]}"
                limit = float(random.choice([500, 1500, 3000, 7500, 15000]))
                bal = round(random.uniform(0, limit * 0.95), 2)
                apr = round(random.uniform(12.9, 34.9), 2)
                card_batch.append((masked_card, c_uuid, limit, bal, apr))

                if len(card_batch) >= BATCH_SIZE:
                    execute_values(cur, """
                        INSERT INTO credit_cards 
                        (card_number_masked, customer_uuid, credit_limit, current_balance, apr)
                        VALUES %s
                    """, card_batch)
                    conn.commit()
                    card_batch = []

    if card_batch:
        execute_values(cur, """
            INSERT INTO credit_cards 
            (card_number_masked, customer_uuid, credit_limit, current_balance, apr)
            VALUES %s
        """, card_batch)
        conn.commit()

    # 3. LOAN APPLICATIONS & REPAYMENTS (~450,000 rows)
    print("Generating Loan Applications & Repayments...")
    raw_loan_meta = []
    for c_uuid in customer_uuids:
        if random.random() < 0.35:
            loan_type = random.choice(LOAN_TYPES)
            principal = float(random.choice([3000, 8000, 15000, 35000, 220000]))
            interest = round(random.uniform(3.2, 14.5), 2)
            term = random.choice([12, 24, 36, 60, 120, 300])
            raw_loan_meta.append((c_uuid, loan_type, principal, interest, term))

    repayment_batch = []
    for i in range(0, len(raw_loan_meta), BATCH_SIZE):
        chunk = raw_loan_meta[i:i + BATCH_SIZE]
        inserted_loans = execute_values(cur, """
            INSERT INTO loan_applications 
            (customer_uuid, loan_type, principal_amount, interest_rate, term_months)
            VALUES %s
            RETURNING loan_id, principal_amount, interest_rate, term_months
        """, chunk, fetch=True)

        for loan_id, principal, interest, term in inserted_loans:
            for j in range(1, random.randint(3, 12)):
                pay_date = datetime.now() - timedelta(days=30 * j)
                amount = round(principal / term, 2)
                interest_comp = round(amount * (interest / 100 / 12), 2)
                principal_comp = round(amount - interest_comp, 2)
                late_fee = 25.00 if random.random() < 0.05 else 0.00

                repayment_batch.append((loan_id, pay_date, amount, principal_comp, interest_comp, late_fee))

                if len(repayment_batch) >= BATCH_SIZE:
                    execute_values(cur, """
                        INSERT INTO loan_repayments 
                        (loan_id, payment_date, amount_paid, principal_component, interest_component, late_fee_applied)
                        VALUES %s
                    """, repayment_batch)
                    conn.commit()
                    repayment_batch = []

    if repayment_batch:
        execute_values(cur, """
            INSERT INTO loan_repayments 
            (loan_id, payment_date, amount_paid, principal_component, interest_component, late_fee_applied)
            VALUES %s
        """, repayment_batch)
        conn.commit()

    cur.close()
    conn.close()
    print("✓ credit_loans database successfully populated!")


if __name__ == "__main__":
    print("==========================================================")
    print("STARTING LARGE-SCALE ENTERPRISE POPULATION (7M+ RECORDS)")
    print("==========================================================\n")

    # 150,000 customers yields ~7 million total records across all tables
    uuids = populate_core_banking(total_customers=150000)
    populate_credit_loans(uuids)

    print("\nMISSION ACCOMPLISHED! All ~7,000,000 records have been populated.")