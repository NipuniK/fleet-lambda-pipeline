import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
from business_logic import is_idle_threshold_exceeded

def test_is_idle_threshold_exceeded():
    assert is_idle_threshold_exceeded(15.0, 10) is True
    assert is_idle_threshold_exceeded(10.0, 10) is False
    assert is_idle_threshold_exceeded(5.0, 10) is False
