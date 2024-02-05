from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions
from datetime import datetime

DB_NAME = "iam"
SERVER_ADDR = "127.0.0.1:1729"


def create_new_database(db):
    if driver.databases.contains(DB_NAME):
        driver.databases.get(DB_NAME).delete()  # Delete the database if it exists already
    driver.databases.create(DB_NAME)
    if not driver.databases.contains(DB_NAME):
        print("Database creation failed. Terminating...")
        exit()


def db_schema_setup(schema_session, schema_file='iam-schema.tql'):
    with schema_session.transaction(TransactionType.WRITE) as tx:
        with open(schema_file, 'r') as data:
            define_query = data.read()
        tx.query.define(define_query)
        tx.commit()


def db_dataset_setup(data_session, data_file='iam-data-single-query.tql'):
    with data_session.transaction(TransactionType.WRITE) as tx:
        with open(data_file, 'r') as data:
            insert_query = data.read()
        tx.query.insert(insert_query)
        tx.commit()


def test_initial_database(data_session):
    print("Testing the new database.")
    with data_session.transaction(TransactionType.READ) as tx:  # Re-using a session to open a new transaction
        test_query = "match $u isa user; get $u; count;"
        response = tx.query.get_aggregate(test_query)
        result = response.resolve().as_value().as_long()
        if result == 3:
            print("Test OK. Database setup complete.")
            return True
        else:
            print("Test failed with the following result:", result, " expected result: 3.")
            return False


def fetch_all_users(data_session, fetch_query):
    with data_session.transaction(TransactionType.READ) as tx:
        with open(fetch_query, 'r') as data:
            typeql_fetch_query = data.read()
        iterator = tx.query.fetch(typeql_fetch_query)  # Executing the query
        result = []
        for item in iterator:  # Iterating through results
            result.append(item)
            # print("User #" + str(k) + ": " + item.get("n").as_attribute().get_value() + ", has E-Mail: " + item.get("e").as_attribute().get_value())
        # print("Users found:", k)
        if len(result) > 0:
            return result
        return 0


def get_kevin_files(data_session, get_query, inference=False):
    if inference:
        options = TypeDBOptions(infer=True)
    else:
        options = TypeDBOptions
    with data_session.tx(TransactionType.READ, options) as tx:
        iterator = tx.query.get(get_query)
        result = []
        k = 0
        for i, item in enumerate(iterator):
            result.append("File #" + str(i+1) + ": " + item.get("fp").as_attribute().get_value())
            k = i
    return result, k


print("IAM Sample App")
with TypeDB.core_driver(SERVER_ADDR) as driver:
    print("Connected to TypeDB Core server: ", SERVER_ADDR)
    create_new_database(DB_NAME)
    with driver.session(DB_NAME, SessionType.SCHEMA) as session:
        db_schema_setup(session)
    with driver.session(DB_NAME, SessionType.DATA) as session:
        db_dataset_setup(session)
        if not test_initial_database(session):
            exit()
        print("Commencing sample requests.")

        print("\nRequest #1: User listing")
        users = fetch_all_users(session, 'queries/1-user-listing.tql')
        for i, user in enumerate(users):
            print(f"User # {i+1}: {user}")  #printing every JSON returned

        print("\nRequest #2: Files that Kevin Morrison has access to")
        files, count = get_kevin_files(session, 'queries/2-kevin-files.tql')
        for file in files:
            print(file)
        print("Files found:", count)

        print("\nRequest #3: Files that Kevin Morrison has view access to (with inference)")
        files, count = get_kevin_files(session, 'queries/3-kevin-view-files.tql', inference=True)
        for file in files:
            print(file)
        print("Files found:", count)


    #     print("\nRequest #3: Files that Kevin Morrison has view access to (with inference)")
    #     with session.transaction(TransactionType.READ, TypeDBOptions(infer=True)) as transaction:  # Inference enabled
    #         typeql_read_query = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " \
    #                             "$o isa object, has path $fp; $pa($o, $va) isa access; " \
    #                             "$va isa action, has name 'view_file'; get $fp; sort $fp asc; offset 0; limit 5;"  # Only the first five results
    #         iterator = transaction.query.get(typeql_read_query)
    #         k = 0
    #         for item in iterator:
    #             k += 1
    #             print("File #" + str(k) + ": " + item.get("fp").as_attribute().get_value())
    #
    #         typeql_read_query = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " \
    #                             "$o isa object, has path $fp; $pa($o, $va) isa access; " \
    #                             "$va isa action, has name 'view_file'; get $fp; sort $fp asc; offset 5; limit 5;"  # The next five results
    #         iterator = transaction.query.get(typeql_read_query)
    #         for item in iterator:
    #             k += 1
    #             print("File #" + str(k) + ": " + item.get("fp").as_attribute().get_value())
    #         print("Files found:", k)
    #
    #     print("\nRequest #4: Add a new file and a view access to it")
    #     with session.transaction(TransactionType.WRITE) as transaction:  # Open a transaction to write
    #         filename = "logs/" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + ".log"
    #         typeql_insert_query = "insert $f isa file, has path '" + filename + "';"
    #         transaction.query.insert(typeql_insert_query)  # Executing the query to insert the file
    #         print("Inserted file:", filename)
    #         typeql_insert_query = "match $f isa file, has path '" + filename + "'; " \
    #                               "$vav isa action, has name 'view_file'; " \
    #                               "insert ($vav, $f) isa access;"
    #         transaction.query.insert(typeql_insert_query)  # Executing the second query in the same transaction
    #         print("Added view access to the file.")
    #         transaction.commit()  # commit transaction to persist changes
