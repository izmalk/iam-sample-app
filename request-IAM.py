from typedb.client import TypeDB, SessionType, TransactionType, TypeDBOptions
from datetime import datetime

print("IAM Sample App")

print("Connecting to the server")
with TypeDB.core_client("0.0.0.0:1729") as client:  # Connect to TypeDB server
    print("Connecting to the `iam` database")
    with client.session("iam", SessionType.DATA) as session:  # Access data in the `iam` database as Session
        print("Request #1: User listing")
        with session.transaction(TransactionType.READ) as transaction:  # Open transaction to read
            typeql_read_query = "match $u isa user, has full-name $n, has email $e;"
            iterator = transaction.query().match(typeql_read_query)  # Executing query
            k = 0
            for item in iterator:  # Iterating through results
                k += 1  # Counter
                print("User #" + str(k) + ": " + item.get("n").get_value() + ", has E-Mail: " + item.get("e").get_value())
            print("Users found:", k)  # Print number of results

        print("\nRequest #2: Files that Kevin Morrison has access to")
        with session.transaction(TransactionType.READ) as transaction:  # Open transaction to read
            typeql_read_query = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " \
                                "$o isa object, has path $fp; $pa($o, $va) isa access; get $fp;"
            iterator = transaction.query().match(typeql_read_query)  # Executing query
            k = 0
            for item in iterator:  # Iterating through results
                k += 1  # Counter
                print("File #" + str(k) + ": " + item.get("fp").get_value())
            print("Files found:", k)  # Print number of results

        print("\nRequest #3: Files that Kevin Morrison has view access to (with inference)")
        typedb_options = TypeDBOptions.core()  # Initialising a new set of options
        typedb_options.infer = True  # Enabling inference in this new set of options
        with session.transaction(TransactionType.READ, typedb_options) as transaction:  # Open transaction to read with inference
            typeql_read_query = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " \
                                "$o isa object, has path $fp; $pa($o, $va) isa access; " \
                                "$va isa action, has action-name 'view_file'; get $fp; sort $fp asc; offset 0; limit 5;"
            iterator = transaction.query().match(typeql_read_query)  # Executing query
            k = 0
            for item in iterator:  # Iterating through results
                k += 1  # Counter
                print("File #" + str(k) + ": " + item.get("fp").get_value())

            typeql_read_query = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " \
                                "$o isa object, has path $fp; $pa($o, $va) isa access; " \
                                "$va isa action, has action-name 'view_file'; get $fp; sort $fp asc; offset 5; limit 5;"
            iterator = transaction.query().match(typeql_read_query)  # Executing query
            for item in iterator:  # Iterating through results
                k += 1  # Counter
                print("File #" + str(k) + ": " + item.get("fp").get_value())
            print("Files found:", k)  # Print number of results

        print("\nRequest #4: Add a new file and a view access to it")
        with session.transaction(TransactionType.WRITE) as transaction:  # Open transaction to write
            filepath = "logs/" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + ".log"
            typeql_insert_query = "insert $f isa file, has path '" + filepath + "';"
            transaction.query().insert(typeql_insert_query)  # runs the query
            print("Inserting file:", filepath)
            typeql_insert_query = "match $f isa file, has path '" + filepath + "'; " \
                                  "$vav isa action, has action-name 'view_file'; " \
                                  "insert ($vav, $f) isa access;"
            print("Adding view access to the file")
            transaction.query().insert(typeql_insert_query)  # runs the query
            transaction.commit()  # commits the transaction
