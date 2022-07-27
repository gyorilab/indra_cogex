from indra_cogex.sources.ec import _strip_ec_code


def test_strip_ec_code():
    case1 = "3.4.24.-"
    case2 = "3.5.-.-"

    stripped1 = _strip_ec_code(case1)
    assert stripped1 == "3.4.24"
    stripped2 = _strip_ec_code(case2)
    assert stripped2 == "3.5"
