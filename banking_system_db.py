import mysql.connector
from tabulate import tabulate

def connect_db():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="jhames",
            database="banking_system_db"
        )
        return conn
    except mysql.connector.Error as e:
        print(f"Database connection error: {e}")
        return None

def input_data():
    conn = connect_db()
    if not conn:
        return
    cursor = conn.cursor()

    client_names = input("Enter Client Name(s) (separate with ', '): ")
    bank_name = input("Enter Bank Name: ")
    account_type = input("Enter Account Type: ")

    cursor.execute("SELECT COUNT(*) FROM Banking_1NF")
    entry_id = cursor.fetchone()[0] + 1  

    for client_name in client_names.split(", "):
        cursor.execute("""
            INSERT INTO Banking_1NF (Entry_ID, Client_Name, Bank_Name, Account_Type)
            VALUES (%s, %s, %s, %s)
        """, (entry_id, client_name.strip(), bank_name, account_type))
        entry_id += 1

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data inserted successfully.")

def display_raw_data():
    conn = connect_db()
    if not conn:
        return
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY MIN(Entry_ID)) AS Entry_ID,
            Bank_Name, 
            GROUP_CONCAT(DISTINCT Client_Name ORDER BY Client_Name SEPARATOR ', ') AS Client_Names,
            Account_Type
        FROM Banking_1NF
        GROUP BY Bank_Name, Account_Type
        ORDER BY MIN(Entry_ID);
    """)

    rows = cursor.fetchall()
    if rows:
        headers = ["Entry_ID", "Bank_Name", "Client_Name(s)", "Account_Type"]
        print("\n--- Raw Data ---")
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    else:
        print("\n--- Raw Data ---")
        print("No data available.")

    cursor.close()
    conn.close()

def display_table(table_name, headers):
    conn = connect_db()
    if not conn:
        return
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    
    table_labels = {
        "Banking_1NF": "--- Banking_1NF ---",
        "ClientAccounts_2NF": "--- ClientAccounts_2NF ---",
        "Clients": "--- Clients_3NF ---",
        "Banks": "--- Banks_3NF ---",
        "AccountTypes": "--- AccountTypes_3NF ---"
    }

    label = table_labels.get(table_name, f"--- {table_name} ---")

    if rows:
        print(f"\n{label}")
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    else:
        print(f"\n{label}")
        print("No data available.")

    cursor.close()
    conn.close()

def normalize_data():
    conn = connect_db()
    if not conn:
        return
    cursor = conn.cursor()

    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursor.execute("DELETE FROM ClientAccounts_2NF;")
    cursor.execute("DELETE FROM Clients;")
    cursor.execute("DELETE FROM Banks;")
    cursor.execute("DELETE FROM AccountTypes;")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    conn.commit()

    cursor.execute("SELECT DISTINCT Client_Name FROM Banking_1NF")
    client_map = {}
    for idx, (client_name,) in enumerate(cursor.fetchall(), start=1):
        client_map[client_name] = idx
        cursor.execute("INSERT INTO Clients (Client_ID, Client_Name) VALUES (%s, %s)", (idx, client_name))

    cursor.execute("SELECT DISTINCT Bank_Name FROM Banking_1NF")
    bank_map = {}
    for idx, (bank_name,) in enumerate(cursor.fetchall(), start=1):
        bank_map[bank_name] = idx
        cursor.execute("INSERT INTO Banks (Bank_ID, Bank_Name) VALUES (%s, %s)", (idx, bank_name))

    cursor.execute("SELECT DISTINCT Account_Type FROM Banking_1NF")
    account_map = {}
    for idx, (account_type,) in enumerate(cursor.fetchall(), start=1):
        account_map[account_type] = idx
        cursor.execute("INSERT INTO AccountTypes (AccountType_ID, Account_Type) VALUES (%s, %s)", (idx, account_type))

    cursor.execute("SELECT DISTINCT Client_Name, Bank_Name, Account_Type FROM Banking_1NF")
    for idx, (client_name, bank_name, account_type) in enumerate(cursor.fetchall(), start=1):
        client_id = client_map[client_name]
        bank_id = bank_map[bank_name]
        account_type_id = account_map[account_type]
        cursor.execute("""
            INSERT INTO ClientAccounts_2NF (Entry_ID, Client_ID, Bank_ID, AccountType_ID)
            VALUES (%s, %s, %s, %s)
        """, (idx, client_id, bank_id, account_type_id))
    conn.commit()
    cursor.close()
    conn.close()

    display_table("Banking_1NF", ["Entry_ID", "Client_Name", "Bank_Name", "Account_Type"])
    display_table("ClientAccounts_2NF", ["Entry_ID", "Client_ID", "Bank_ID", "AccountType_ID"])
    display_table("Clients", ["Client_ID", "Client_Name"])
    display_table("Banks", ["Bank_ID", "Bank_Name"])
    display_table("AccountTypes", ["AccountType_ID", "Account_Type"])

def main():
    while True:
        print("\n===== Banking System =====")
        print("1. Insert Data")
        print("2. Display Raw Data")
        print("3. Normalize & Display 1NF, 2NF, 3NF")
        print("4. Exit")

        choice = input("Enter choice: ")

        if choice == "1":
            input_data()
        elif choice == "2":
            display_raw_data()
        elif choice == "3":
            normalize_data()
        elif choice == "4":
            print("Exiting program...")
            break
        else:
            print("❌ Invalid choice. Try again.")

if __name__ == "__main__":
    main()
