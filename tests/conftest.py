import pytest


class FakeResponse:
    """Fake response class for tests"""

    def __init__(self, ok: bool, text: str, status_code: int, content: str = None):
        self.ok = ok
        self.text = text
        self.status_code = status_code
        self.content = content


@pytest.fixture()
def fake_response():
    return FakeResponse


class FakeMessage:
    """Fake response class for tests"""

    def value(self):
        return 'fake_value'.encode('utf-8')


@pytest.fixture()
def fake_message():
    return FakeMessage


@pytest.fixture()
def get_ok_resp(fake_response):
    """Return response with ok status"""
    def inner(url: str):
        return fake_response(ok=True, text=url, status_code=201)
    return inner


@pytest.fixture()
def get_not_ok_resp(fake_response):
    """Return response with ok status"""
    def inner(url: str):
        return fake_response(ok=False, text=url, status_code=401, content='Error')
    return inner
