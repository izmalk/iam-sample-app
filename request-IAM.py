from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions
from datetime import datetime
db_name = "iam"
server_addr = "127.0.0.1:1729"

print("IAM Sample App")

print("Database setup initiated. Attempting to connect...")
with TypeDB.core_driver(server_addr) as driver:  # Connect to TypeDB Core server
    if driver.databases.contains(db_name):
        print("Found a pre-existing database! Re-creating with the default schema and data...")
        driver.databases.get(db_name).delete()
    driver.databases.create(db_name)
    if driver.databases.contains(db_name):
        print("Empty database ready.")
    print("Opening a Schema session to define schema.")
    with driver.session(db_name, SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.WRITE) as transaction:
            with open('iam-schema.tql', 'r') as data:
                define_query = data.read()
            transaction.query.define(define_query)
            transaction.commit()
    print("Opening a Data session to insert data.")
    with driver.session(db_name, SessionType.DATA) as session:
        with session.transaction(TransactionType.WRITE) as transaction:
            with open('iam-data-single-query.tql', 'r') as data:
                insert_query = data.read()
            transaction.query.insert(insert_query)
            transaction.commit()
        print("Testing a database.")
        with session.transaction(TransactionType.READ) as transaction:  # Re-using a session to open a new transaction
            test_query = "match $u isa user; get $u; count;"
            response = transaction.query.get_aggregate(test_query)
            result = response.resolve().as_value().as_long()
            if result == 3:
                print("Database setup complete. Test passed.")
            else:
                print("Test failed with the following result:", result, " expected result: 3.")
                exit()

    print("Commencing sample requests.")
    with driver.session(db_name, SessionType.DATA) as session:
        print("\nRequest #1: User listing")
        with session.transaction(TransactionType.READ) as transaction:
            typeql_read_query = "match $u isa user, has full-name $n, has email $e; get;"
            iterator = transaction.query.get(typeql_read_query)  # Executing the query
            k = 0  # Reset counter
            for item in iterator:  # Iterating through results
                k += 1
                print("User #" + str(k) + ": " + item.get("n").as_attribute().get_value() + ", has E-Mail: " + item.get("e").as_attribute().get_value())
            print("Users found:", k)  # Print the number of results

        print("\nRequest #2: Files that Kevin Morrison has access to")
        with session.transaction(TransactionType.READ) as transaction:
            typeql_read_query = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " \
                                "$o isa object, has path $fp; $pa($o, $va) isa access; get $fp;"
            iterator = transaction.query.get(typeql_read_query)
            k = 0
            for item in iterator:
                k += 1
                print("File #" + str(k) + ": " + item.get("fp").as_attribute().get_value())
            print("Files found:", k)

        print("\nRequest #3: Files that Kevin Morrison has view access to (with inference)")
        with session.transaction(TransactionType.READ, TypeDBOptions(infer=True)) as transaction:  # Inference enabled
            typeql_read_query = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " \
                                "$o isa object, has path $fp; $pa($o, $va) isa access; " \
                                "$va isa action, has name 'view_file'; get $fp; sort $fp asc; offset 0; limit 5;"  # Only the first five results
            iterator = transaction.query.get(typeql_read_query)
            k = 0
            for item in iterator:
                k += 1
                print("File #" + str(k) + ": " + item.get("fp").as_attribute().get_value())

            typeql_read_query = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " \
                                "$o isa object, has path $fp; $pa($o, $va) isa access; " \
                                "$va isa action, has name 'view_file'; get $fp; sort $fp asc; offset 5; limit 5;"  # The next five results
            iterator = transaction.query.get(typeql_read_query)
            for item in iterator:
                k += 1
                print("File #" + str(k) + ": " + item.get("fp").as_attribute().get_value())
            print("Files found:", k)

        print("\nRequest #4: Add a new file and a view access to it")
        with session.transaction(TransactionType.WRITE) as transaction:  # Open a transaction to write
            filename = "logs/" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + ".log"
            typeql_insert_query = "insert $f isa file, has path '" + filename + "';"
            transaction.query.insert(typeql_insert_query)  # Executing the query to insert the file
            print("Inserting file:", filename)
            typeql_insert_query = "match $f isa file, has path '" + filename + "'; " \
                                  "$vav isa action, has name 'view_file'; " \
                                  "insert ($vav, $f) isa access;"
            print("Adding view access to the file")
            transaction.query.insert(typeql_insert_query)  # Executing the second query in the same transaction
            transaction.commit()  # commit transaction to persist changes
