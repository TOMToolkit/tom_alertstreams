from django.test import tag, TestCase


class TestDummy(TestCase):
    """
    This is just a dummy test to make sure the testing infrastructure is working.
    """

    def test_dummy(self):
        assert True


@tag('canary')
class TestDummyCanary(TestCase):
    """
    This is just a dummy test to make sure the testing infrastructure is working.
    """

    def test_dummy_canary(self):
        assert True
