from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions
import setup

DB_NAME = "sample_app"
SERVER_ADDR = "127.0.0.1:1729"


def fetch_all_users(driver) -> list:
    print("\nRequest #1: Fetch all users as JSON objects with full names and emails")
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.READ) as read_tx:
            return list(read_tx.query.fetch("match $u isa user; fetch $u: full-name, email;"))


def insert_new_user(driver, name, email):
    print(f"\nRequest #2: Add a new user with full-name {name} and email {email}")
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.WRITE) as write_tx:
            response = list(
                write_tx.query.insert(f"insert $p isa person, has full-name '{name}', has email '{email}';"))
            write_tx.commit()
            return response


def get_files_by_user(driver, name, inference=False):
    if not inference:
        print(f"\nRequest #3: Find all files that the user {name} has access to view (no inference)")
        options = TypeDBOptions()
    else:
        print(f"\nRequest #4: Find all files that the user {name} has access to view (with inference)")
        options = TypeDBOptions(infer=True)
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.READ, options) as read_tx:
            matched_users = list(read_tx.query.get(f"match $u isa user, has full-name '{name}'; get;"))
            if len(matched_users) > 1:
                print("WARNING: Found more than one user with tha name.")
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
            if len(response) > 0:
                return response
            else:
                print("No files found. Try enabling inference.")
                return False


def update_filepath(driver, old, new):
    print(f"\nRequest #5: Update the path of a file from {old} to {new}")
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.WRITE) as write_tx:
            response = list(write_tx.query.update(f"""
                                                    match
                                                    $f isa file, has path $old_path;
                                                    $old_path = '{old}';
                                                    delete
                                                    $f has $old_path;
                                                    insert
                                                    $f has path $new_path;
                                                    $new_path = '{new}';
                                                    """))
            if len(response) > 0:
                write_tx.commit()
                return response
            else:
                print("No matched paths: nothing to update.")
                return False


def delete_file(driver, path):
    print(f"\nRequest #6: Delete the file with path {path}")
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.WRITE) as write_tx:
            response = list(write_tx.query.get(f"""
                                                match
                                                $f isa file, has path '{path}';
                                                get;
                                                """))
            if len(response) == 1:
                write_tx.query.delete(f"""
                                        match
                                        $f isa file, has path '{path}';
                                        delete
                                        $f isa file;
                                        """).resolve()
                write_tx.commit()
                return True
            elif len(response) > 1:
                print("Matched more than one file with the same path. Deletion was aborted.")
                return False
            else:
                print("No files matched in the database.")
                return False


def main():
    with TypeDB.core_driver(SERVER_ADDR) as driver:
        setup.db_setup(driver, DB_NAME)

        print("\nCRUD Operations:")
        users = fetch_all_users(driver)
        for i, JSON in enumerate(users, start=1):
            print(f"User #{i}: {JSON}")

        new_user = insert_new_user(driver, 'Jack Keeper', 'jk@vaticle.com')
        for i, concept_map in enumerate(new_user, start=1):
            name = concept_map.get("_0").as_attribute().get_value()
            email = concept_map.get("_1").as_attribute().get_value()
            print("Added new user. Name: " + name + ", E-mail:" + email)

        files = get_files_by_user(driver, "Kevin Morrison")
        if not (files is False):
            for i, file in enumerate(files, start=1):
                print(f"File #{i}:", file.get("fp").as_attribute().get_value())

        files = get_files_by_user(driver, "Kevin Morrison", inference=True)
        if not (files is False):
            for i, file in enumerate(files, start=1):
                print(f"File #{i}:", file.get("fp").as_attribute().get_value())

        updated_files = update_filepath(driver, 'lzfkn.java', 'lzfkn2.java')
        print(f"Total number of paths updated: {len(updated_files)}")

        deleted = delete_file(driver, 'lzfkn2.java')
        if deleted:
            print("The file has been deleted.")
        else:
            print("No files were deleted.")


if __name__ == "__main__":
    main()
