from bevault_workers.stores.store_utils import get_first


def test_get_first_is_case_insensitive_for_string_keys():
    d = {"Host": "h1", "PORT": "22"}
    assert get_first(d, "host") == "h1"
    assert get_first(d, "port") == "22"


def test_get_first_tries_keys_in_order_for_distinct_names():
    d = {"user": "legacy", "username": "ignored"}
    assert get_first(d, "username", "user") == "ignored"
    d2 = {"username": "u"}
    assert get_first(d2, "user", "username") == "u"
