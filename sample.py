from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions
import setup

DB_NAME = "sample_app"
SERVER_ADDR = "127.0.0.1:1729"


def fetch_all_users(driver):
    print("\nRequest #1: Fetching all users as JSON objects with full names and emails")
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.READ) as read_tx:
            users = list(read_tx.query.fetch("match $u isa user; fetch $u: full-name, email;"))
            for i, JSON in enumerate(users):
                print(f"User #{i + 1}: {JSON}")
            return users


def insert_new_user(driver):
    print("\nRequest #2: Add new user with full-name and email")
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.WRITE) as write_tx:
            response = write_tx.query.insert(
                "insert $p isa person, has full-name 'Jack Keeper', has email 'jk@vaticle.com';")
            counter = 0
            for res in response:
                counter += 1
                name = res.get("_0").as_attribute().get_value()
                email = res.get("_1").as_attribute().get_value()
                print("Added new user. Name: " + name + ", E-mail:" + email)
            write_tx.commit()
            return counter


def get_files_by_user(driver, name, inference=False):
    if not inference:
        print("\nRequest #3: Find all files that user with selected name has access to view (no inference)")
        options = TypeDBOptions()
    else:
        print("\nRequest #4: Find all files that user with selected name has access to view (with inference)")
        options = TypeDBOptions(infer=True)
    with driver.session(DB_NAME, SessionType.DATA) as data_session:
        with data_session.transaction(TransactionType.READ, options) as read_tx:
            matched_users = list(read_tx.query.get(f"match $p isa person, has full-name '{name}'; get;"))
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
            files = []
            if len(response) > 0:
                for i, res in enumerate(response):
                    file_path = res.get("fp").as_attribute().get_value()
                    files.append(file_path)
                    print(f"File #{i + 1}:", file_path)
            else:
                print("No files found. Try enabling inference.")
            return files


def update_filepath(driver, old, new):
    print("\nRequest #5: Update path of a file")
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
            files = []
            if len(response) > 1:
                print(f"Warning: more than one file matched and processed. Total count of files: {len(response)}")
            if len(response) > 0:
                for res in response:
                    file_path = res.get("new_path").as_attribute().get_value()
                    files.append(file_path)
                    print(f"File {old} has been renamed to {file_path}")
                write_tx.commit()
            else:
                print("No files found. Try enabling inference.")
            return files


def delete_file(driver, path):
    print("\nRequest #6: Delete a file")
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
                print(f"File {path} has been deleted.")
            elif len(response) > 1:
                print("Matched more than one file with the same path. Deletion was aborted.")
            else:
                print("No files matched in the database.")


def main():
    with TypeDB.core_driver(SERVER_ADDR) as driver:
        setup.db_setup(driver, DB_NAME)

        print("\nDemonstrating CRUD Operations:")
        fetch_all_users(driver)
        insert_new_user(driver)
        get_files_by_user(driver, "Kevin Morrison")
        get_files_by_user(driver, "Kevin Morrison", inference=True)
        update_filepath(driver, 'lzfkn.java', 'lzfkn2.java')
        delete_file(driver, 'lzfkn2.java')


if __name__ == "__main__":
    main()
