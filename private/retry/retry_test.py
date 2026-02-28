# Copyright (C) 2025 Akave
# See LICENSE for copying information.

import pytest
import time
from .retry import WithRetry


def test_success_on_first_attempt():
    retry = WithRetry(max_attempts=3, base_delay=0.01)
    call_count = 0
    
    def f():
        nonlocal call_count
        call_count += 1
        return False, None
    
    err = retry.do(f)
    assert err is None
    assert call_count == 1


def test_failure_without_retry():
    retry = WithRetry(max_attempts=3, base_delay=0.01)
    call_count = 0
    test_err = Exception("test error")
    
    def f():
        nonlocal call_count
        call_count += 1
        return False, test_err
    
    err = retry.do(f)
    assert err is not None
    assert str(err) == "test error"
    assert call_count == 1


def test_retry_and_success():
    retry = WithRetry(max_attempts=3, base_delay=0.01)
    call_count = 0
    test_err = Exception("test error")
    
    def f():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return True, test_err
        return False, None
    
    err = retry.do(f)
    assert err is None
    assert call_count == 3


def test_retry_exceeds_max_attempts():
    retry = WithRetry(max_attempts=2, base_delay=0.01)
    call_count = 0
    test_err = Exception("test error")
    
    def f():
        nonlocal call_count
        call_count += 1
        return True, test_err
    
    err = retry.do(f)
    assert err is not None
    assert str(err) == "test error"
    assert call_count == 3


def test_max_attempts_zero():
    retry = WithRetry(max_attempts=0, base_delay=0.01)
    call_count = 0
    test_err = Exception("test error")
    
    def f():
        nonlocal call_count
        call_count += 1
        return True, test_err
    
    err = retry.do(f)
    assert err is not None
    assert str(err) == "test error"
    assert call_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
