import pytest
import chameleon


def test_sum_as_string():
    assert chameleon.sum_as_string(1, 1) == "2"
