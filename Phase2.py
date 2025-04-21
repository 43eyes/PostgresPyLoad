import psycopg2
import sys
from decimal import Decimal # need this for money calcs - floats cause rounding issues

# --- DB connection stuff ---
DB_NAME = "store"
DB_USER = "postgres"
DB_PASS = "admin"

# --- SQL Queries ---

# using left join so we don't miss accounts with no activity
# coalesce deals with null sums for empty accounts
# using dbo schema - change if yours is different

SQL_OVERDRAWN_CHECKING = """
SELECT
    M.FIRST_NAME,
    M.LAST_NAME,
    (C.STARTING_BALANCE + COALESCE(SUM(T.TRANSACTION_AMOUNT), 0)) AS current_balance
FROM
    dbo.MEMBERS M
JOIN
    dbo.ACCOUNTS A ON M.MEMBER_GUID = A.MEMBER_GUID
JOIN
    dbo.CHECKING C ON A.ACCOUNT_GUID = C.ACCOUNT_GUID
LEFT JOIN
    dbo.TRANSACTIONS T ON C.ACCOUNT_GUID = T.ACCOUNT_GUID
GROUP BY
    M.MEMBER_GUID, M.FIRST_NAME, M.LAST_NAME, C.ACCOUNT_GUID, C.STARTING_BALANCE
HAVING
    (C.STARTING_BALANCE + COALESCE(SUM(T.TRANSACTION_AMOUNT), 0)) < 0
ORDER BY
    current_balance ASC, M.LAST_NAME, M.FIRST_NAME;
"""

SQL_OVERPAID_LOANS = """
SELECT
    M.FIRST_NAME,
    M.LAST_NAME,
    -- negative sign here flips it positive for display
    -(L.STARTING_DEBT + COALESCE(SUM(T.TRANSACTION_AMOUNT), 0)) AS overpaid_amount
FROM
    dbo.MEMBERS M
JOIN
    dbo.ACCOUNTS A ON M.MEMBER_GUID = A.MEMBER_GUID
JOIN
    dbo.LOANS L ON A.ACCOUNT_GUID = L.ACCOUNT_GUID
LEFT JOIN
    dbo.TRANSACTIONS T ON L.ACCOUNT_GUID = T.ACCOUNT_GUID
GROUP BY
    M.MEMBER_GUID, M.FIRST_NAME, M.LAST_NAME, L.ACCOUNT_GUID, L.STARTING_DEBT
HAVING
    -- this is how we catch overpayments
    (L.STARTING_DEBT + COALESCE(SUM(T.TRANSACTION_AMOUNT), 0)) < 0
ORDER BY
    overpaid_amount DESC, M.LAST_NAME, M.FIRST_NAME;
"""

# total assets = sum of all outstanding loan balances
SQL_TOTAL_ASSETS = """
WITH LoanBalances AS (
    SELECT
        L.ACCOUNT_GUID,
        (L.STARTING_DEBT + COALESCE(SUM(T.TRANSACTION_AMOUNT), 0)) AS current_balance
    FROM
        dbo.LOANS L
    LEFT JOIN
        dbo.TRANSACTIONS T ON L.ACCOUNT_GUID = T.ACCOUNT_GUID
    GROUP BY
        L.ACCOUNT_GUID, L.STARTING_DEBT
)
SELECT
    SUM(current_balance) AS total_institution_assets
FROM
    LoanBalances
WHERE
    current_balance > 0;
"""

def run_query(conn, sql):
    """Runs a query and gets all results"""
    results = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description: # make sure query actually returned data
                 results = cur.fetchall()
    except psycopg2.Error as e:
        print(f"Database error: {e}")

    return results

def format_currency(value):
    """Makes numbers look like $1,234.56"""
    if value is None:
        return "$0.00"
    # convert to Decimal if it isn't already
    if not isinstance(value, Decimal):
        try:
            value = Decimal(value)
        except:
            return str(value) # just return as-is if conversion fails
    return f"${value:,.2f}"

def main():
    """Main function - connects to DB and runs the reports"""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
        )
        print("Successfully connected to the database.")

        # --- Question 1: Overdrawn Checking Accounts ---
        print("\n——— Overdrawn Checking Accounts ———")
        overdrawn_results = run_query(conn, SQL_OVERDRAWN_CHECKING)
        if overdrawn_results:
            print(f"{'First Name':<15} {'Last Name':<15} {'Overdrawn Balance'}")
            print("—" * 45)
            for first, last, balance in overdrawn_results:
                print(f"{first:<15} {last:<15} {format_currency(balance)}")
        else:
            print("No overdrawn checking accounts found.")

        # --- Question 2: Overpaid Loans ---
        print("\n——— Overpaid Loans ———")
        overpaid_results = run_query(conn, SQL_OVERPAID_LOANS)
        if overpaid_results:
            print(f"{'First Name':<15} {'Last Name':<15} {'Amount Overpaid'}")
            print("—" * 45)
            for first, last, amount in overpaid_results:
                 print(f"{first:<15} {last:<15} {format_currency(amount)}")
        else:
            print("No overpaid loans found.")

        # --- Question 3: Total Asset Size ---
        print("\n——— Total Institution Asset Size (Outstanding Loans) ———")
        asset_results = run_query(conn, SQL_TOTAL_ASSETS)
        if asset_results and asset_results[0][0] is not None:
            total_assets = asset_results[0][0]
            print(f"Total Assets: {format_currency(total_assets)}")
        else:
            print("Could not calculate total assets or total assets are $0.00.")

    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
    input("\nPress Enter to exit...")
