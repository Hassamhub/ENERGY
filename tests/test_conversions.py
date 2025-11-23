from poller.conversions import to_float32_be, to_float64_be

def test_float32():
    assert round(to_float32_be([0x42f6, 0xe979]), 6) == 123.456001

def test_float64():
    assert round(to_float64_be([0x405f, 0x9e37, 0x5b05, 0x3c61]), 12) == 123.456789012346