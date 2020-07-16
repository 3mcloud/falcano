from falcano.util import snake_to_camel_case


def test_convert_snake_to_camel():
    assert snake_to_camel_case("my_name_is_jonas") == "myNameIsJonas"
    assert snake_to_camel_case("Thanks_for_all_ya_shown_us") == "thanksForAllYaShownUs"
