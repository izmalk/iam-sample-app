from typedb.driver import TypeDB, SessionType, TransactionType

DB_NAME = "sample_app_db"
SERVER_ADDR = "127.0.0.1:1729"


def create_new_database(driver, db_name, db_reset):
    if driver.databases.contains(db_name):
        if db_reset:
            print("Replacing an existing database", end="...")
            driver.databases.get(db_name).delete()  # Delete the database if it exists already
            driver.databases.create(db_name)
            print("OK")
        else:
            print("Reusing an existing database. To reset the database, consider using the --reset argument.")
    else:
        print("Creating a new database", end="...")
        driver.databases.create(db_name)
        print("OK")
    if not driver.databases.contains(db_name):
        print("Database creation failed. Terminating...")
        exit()


def db_schema_setup(schema_session, schema_file='iam-schema.tql'):
    with schema_session.transaction(TransactionType.WRITE) as tx:
        with open(schema_file, 'r') as data:
            define_query = data.read()
        print("Defining schema", end="...")
        tx.query.define(define_query)
        tx.commit()
        print("OK")


def db_dataset_setup(data_session, data_file='iam-data-single-query.tql'):
    with data_session.transaction(TransactionType.WRITE) as tx:
        with open(data_file, 'r') as data:
            insert_query = data.read()
        print("Loading data", end="...")
        tx.query.insert(insert_query)
        tx.commit()
        print("OK")


def test_initial_database(data_session):
    with data_session.transaction(TransactionType.READ) as tx:
        test_query = "match $u isa user; get $u; count;"
        print("Testing the database", end="...")
        response = tx.query.get_aggregate(test_query)
        result = response.resolve().as_value().as_long()
        if result == 3:
            print("Passed")
            return True
        else:
            print("Failed with the result:", result, "\n Expected result: 3.")
            return False


def db_setup(driver, db_name, db_reset):
    print(f"Setting up the database: {db_name}")
    create_new_database(driver, db_name, db_reset)
    if db_reset:
        with driver.session(db_name, SessionType.SCHEMA) as session:
            db_schema_setup(session)
        with driver.session(db_name, SessionType.DATA) as session:
            db_dataset_setup(session)
    with driver.session(db_name, SessionType.DATA) as session:
        if test_initial_database(session):
            print("Database setup complete.")
            return True
        else:
            print("Database setup failed.")
            return False


def main(driver):
    if db_setup(driver, DB_NAME):
        print("Setup complete.")


if __name__ == "__main__":
    with TypeDB.core_driver(SERVER_ADDR) as driver:
        main(driver)
