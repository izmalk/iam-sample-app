from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions


def fetch_users(session, query_string):
    with session.transaction(TransactionType.READ) as txn:
        response = txn.query.fetch(query_string)
        result = []
        for item in response:
            result.append(item)
        if len(result) > 0:
            return result
        return 0


# todo Implement pagination
def get_query(data_session, query_file, qvars, inference=False):
    options = TypeDBOptions(infer=inference)
    with open(query_file, 'r') as data:
        typeql_get_query = data.read()
    with data_session.transaction(TransactionType.READ, options) as txn:
        iterator = txn.query.get(typeql_get_query)
        result = []
        for counter, item in enumerate(iterator):
            k = counter + 1
            for qvar in qvars:
                string_value = item.get(qvar).as_attribute()
                result.append(string_value.get_type().get_label().name.capitalize() + " #"
                              + str(counter + 1) + ": " + string_value.get_value())
    return k, result
