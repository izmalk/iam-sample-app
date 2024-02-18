from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions
import setup

DB_NAME = "sample_app"
SERVER_ADDR = "127.0.0.1:1729"


def fetch_all_users(driver):
    print("\nRequest #1: Fetching all users as JSON objects with full names and emails")
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.READ) as read_tx:
            users = read_tx.query.fetch("match $u isa user; fetch $u: full-name, email;")
            for i, JSON in enumerate(users):
                print(f"User #{i + 1}: {JSON}")


def insert_new_user(driver):
    print("\nRequest #2: Add new user with full-name and email")
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.WRITE) as write_tx:
            response = write_tx.query.insert(
                "insert $p isa person, has full-name 'Jack Keeper', has email 'jk@vaticle.com';")
            for res in response:
                print(res.get("_0").as_attribute().get_value() + " " + res.get("_1").as_attribute().get_value())


def get_files_by_user(driver, name, inference=False):
    if not inference:
        print("\nRequest #3: Find all files that user with selected name has access to view")
        options = TypeDBOptions()
    else:
        print("\nRequest #4: Find all files that user with selected name has access to view (with inference)")
        options = TypeDBOptions(infer=True)
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.WRITE, options) as read_tx:
            matched_users = list(read_tx.query.get(f"match $p isa person, has full-name '{name}'; get;"))
            if len(matched_users) > 1:
                print("Found more than one user with tha name")
                return False
            elif len(matched_users) == 1:
                response = list(read_tx.query.get(f"""
                                                match
                                                $u isa user, has full-name '{name}';
                                                $p($u, $pa) isa permission;
                                                $o isa object, has path $fp;
                                                $pa($o, $va) isa access;
                                                $va isa action, has name 'view_file';
                                                get $fp; sort $fp asc;
                                                """))
            else:
                response = list(read_tx.query.get(f"""
                                                match
                                                $u isa user, has full-name $fn;
                                                $fn contains '{name}';
                                                $p($u, $pa) isa permission;
                                                $o isa object, has path $fp;
                                                $pa($o, $va) isa access;
                                                $va isa action, has name 'view_file';
                                                get $fp; sort $fp asc;
                                                """))
            for i, res in enumerate(response):
                print(f"File #{i + 1}", res.get("fp").as_attribute().get_value())


def main():
    with TypeDB.core_driver(SERVER_ADDR) as driver:
        setup.db_setup(driver, DB_NAME)

        print("\nDemonstrating CRUD Operations:")
        fetch_all_users(driver)
        insert_new_user(driver)
        get_files_by_user(driver, "Kevin Morrison")
        get_files_by_user(driver, "Kevin Morrison", inference=True)

        # Update
        # Delete
        #
        # print("\nSample Application Complete. Check the documentation for more details.")


if __name__ == "__main__":
    main()

#
# print("\nRequest #2: Files that Kevin Morrison has access to")
# count, results = queries.get_query(data_session, 'queries/2-kevin-files.tql', ["fp"])
# for result in results:
#     print(result)
# print("Files found:", count)
#
# print("\nRequest #3: Files that Kevin Morrison has view access to (with inference)")
# count, results = queries.get_query(data_session, 'queries/3-kevin-view-files.tql', ["fp"], inference=True)
# for result in results:
#     print(result)
# print("Files found:", count)


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
