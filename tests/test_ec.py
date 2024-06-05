from indra_cogex.sources.ec import strip_ec_code


def test_strip_ec_code():
    case1 = "3.4.24.-"
    case2 = "3.5.-.-"

    stripped1 = strip_ec_code(case1)
    assert stripped1 == "3.4.24"
    stripped2 = strip_ec_code(case2)
    assert stripped2 == "3.5"
