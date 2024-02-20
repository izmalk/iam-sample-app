from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions
import setup

DB_NAME = setup.DB_NAME
SERVER_ADDR = setup.SERVER_ADDR


def fetch_all_users(driver) -> list:
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.READ) as read_tx:
            users = list(read_tx.query.fetch("match $u isa user; fetch $u: full-name, email;"))
            for i, JSON in enumerate(users, start=0):
                print(f"User #{i + 1} — Full-name:", users[i]['u']['full-name'][0]['value'], end="")
                print(f", JSON: {JSON}")
            return users


def insert_new_user(driver, name, email):
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.WRITE) as write_tx:
            response = list(
                write_tx.query.insert(
                    f"insert $p isa person, has full-name $fn, has email $e; $fn == '{name}'; $e == '{email}';"))
            write_tx.commit()
            for i, concept_map in enumerate(response, start=1):
                name = concept_map.get("fn").as_attribute().get_value()
                email = concept_map.get("e").as_attribute().get_value()
                print("Added new user. Name: " + name + ", E-mail:" + email)
            return response


def get_files_by_user(driver, name, inference=False):
    options = TypeDBOptions(infer=inference)
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.READ, options) as read_tx:
            users = list(read_tx.query.get(f"match $u isa user, has full-name '{name}'; get;"))
            if len(users) == 1:
                response = list(read_tx.query.get(f"""
                                                    match
                                                    $fn == '{name}';
                                                    $u isa user, has full-name $fn;
                                                    $p($u, $pa) isa permission;
                                                    $o isa object, has path $fp;
                                                    $pa($o, $va) isa access;
                                                    $va isa action, has name 'view_file';
                                                    get $fp; sort $fp asc;
                                                    """))
            elif len(users) > 1:
                print("Error: Found more than one user with that name.")
                return None
            elif len(users) == 0:
                print("Warning: No users found with that name. Extending search for full-names containing the "
                      "provided search string.")
                response = list(read_tx.query.get(f"""
                                                    match
                                                    $fn contains '{name}';
                                                    $u isa user, has full-name $fn;
                                                    $p($u, $pa) isa permission;
                                                    $o isa object, has path $fp;
                                                    $pa($o, $va) isa access;
                                                    $va isa action, has name 'view_file';
                                                    get $fp; sort $fp asc;
                                                    """))
            for i, file in enumerate(response, start=1):
                print(f"File #{i}:", file.get("fp").as_attribute().get_value())
            if len(response) == 0:
                print("No files found. Try enabling inference.")
            return response


def update_filepath(driver, old, new):
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
                return None


def delete_file(driver, path):
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
                print("Matched more than one file with the same path.")
                return False
            else:
                print("No files matched in the database.")
                return False


def main():
    with TypeDB.core_driver(SERVER_ADDR) as driver:
        if not setup.db_setup(driver, DB_NAME):
            print("Terminating...")
            exit()

        print("\nRequest 1 of 6: Fetch all users as JSON objects with full names and emails")
        users = fetch_all_users(driver)
        assert len(users) == 3

        name = "Jack Keeper"
        email = "jk@vaticle.com"
        print(f"\nRequest 2 of 6: Add a new user with the full-name {name} and email {email}")
        insert_new_user(driver, name, email)

        name = "Kevin Morrison"
        print(f"\nRequest 3 of 6: Find all files that the user {name} has access to view (no inference)")
        files = get_files_by_user(driver, name)
        assert files is not None
        assert len(files) == 0

        print(f"\nRequest 4 of 6: Find all files that the user {name} has access to view (with inference)")
        files = get_files_by_user(driver, name, inference=True)
        assert files is not None
        assert len(files) == 10

        old_path = 'lzfkn.java'
        new_path = 'lzfkn2.java'
        print(f"\nRequest 5 of 6: Update the path of a file from {old_path} to {new_path}")
        updated_files = update_filepath(driver, old_path, new_path)
        if updated_files is not None:
            print(f"Total number of paths updated: {len(updated_files)}")

        path = 'lzfkn2.java'
        print(f"\nRequest 6 of 6: Delete the file with path {path}")
        deleted = delete_file(driver, path)
        if deleted:
            print("The file has been deleted.")
        else:
            print("No files were deleted.")


if __name__ == "__main__":
    main()
