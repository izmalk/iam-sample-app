from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions
DB_NAME = "iam"
SERVER_ADDR = "127.0.0.1:1729"

print("List")

print("Attempting to connect to a TypeDB Core server: ", SERVER_ADDR)
with TypeDB.core_driver(SERVER_ADDR) as driver:  # Connect to TypeDB Core server
    for db in driver.databases.all():
        print(db.name)
