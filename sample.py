from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions
from datetime import datetime

import queries
import setup

DB_NAME = "iam"
SERVER_ADDR = "127.0.0.1:1729"


if __name__ == "__main__":
    print("IAM Sample App")
    if setup.db_setup():
        print("Commencing sample requests.")
    else:
        print("Setup failed. Terminating.")

    with TypeDB.core_driver(SERVER_ADDR) as driver:
        with driver.session(DB_NAME,SessionType.DATA) as data_session:
            with data_session.transaction(TransactionType.READ) as read_txn:
                print("\nRequest #1: User listing")
                with open('queries/1-user-listing.tql', 'r') as data:
                    typeql_fetch_query = data.read()
                users = queries.fetch_users(data_session, typeql_fetch_query)
                for i, JSON in enumerate(users):
                    print(f"User # {i+1}: {JSON}")

                print("\nRequest #2: Files that Kevin Morrison has access to")
                count, results = queries.get_query(data_session, 'queries/2-kevin-files.tql', ["fp"])
                for result in results:
                    print(result)
                print("Files found:", count)

                print("\nRequest #3: Files that Kevin Morrison has view access to (with inference)")
                count, results = queries.get_query(data_session, 'queries/3-kevin-view-files.tql', ["fp"], inference=True)
                for result in results:
                    print(result)
                print("Files found:", count)



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
