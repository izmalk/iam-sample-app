from typedb.driver import TypeDB, SessionType, TransactionType

DB_NAME = "iam"
SERVER_ADDR = "127.0.0.1:1729"


def create_new_database(driver, db):
    if driver.databases.contains(db):
        driver.databases.get(DB_NAME).delete()  # Delete the database if it exists already
    driver.databases.create(db)
    if not driver.databases.contains(db):
        print("Database creation failed. Terminating...")
        exit()


def db_schema_setup(schema_session, schema_file='iam-schema.tql'):
    with schema_session.transaction(TransactionType.WRITE) as txn:
        with open(schema_file, 'r') as data:
            define_query = data.read()
        txn.query.define(define_query)
        txn.commit()


def db_dataset_setup(data_session, data_file='iam-data-single-query.tql'):
    with data_session.transaction(TransactionType.WRITE) as txn:
        with open(data_file, 'r') as data:
            insert_query = data.read()
        txn.query.insert(insert_query)
        txn.commit()


def test_initial_database(data_session):
    print("Testing the new database.")
    with data_session.transaction(TransactionType.READ) as txn:  # Re-using a session to open a new transaction
        test_query = "match $u isa user; get $u; count;"
        response = txn.query.get_aggregate(test_query)
        result = response.resolve().as_value().as_long()
        if result == 3:
            print("Test OK. Database setup complete.")
            return True
        else:
            print("Test failed with the following result:", result, " expected result: 3.")
            return False


def db_setup():
    print("Setup")
    with TypeDB.core_driver(SERVER_ADDR) as driver:
        print("Connected to TypeDB Core server: ", SERVER_ADDR)
        create_new_database(driver, DB_NAME)
        with driver.session(DB_NAME, SessionType.SCHEMA) as session:
            db_schema_setup(session)
        with driver.session(DB_NAME, SessionType.DATA) as session:
            db_dataset_setup(session)
            if test_initial_database(session):
                return True
            else:
                return False


if __name__ == "__main__":
    if db_setup():
        print("Setup complete.")
