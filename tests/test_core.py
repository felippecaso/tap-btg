"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os

from singer_sdk.testing import get_tap_test_class

from tap_btg.tap import TapBTG


test_data_dir = os.path.dirname(os.path.abspath(__file__))

SAMPLE_CONFIG = {
    "files": [
        {
            "path": f"{test_data_dir}/data",
            "type": "investments_transactions"
        }
    ]
}

# Run standard built-in tap tests from the SDK:
TestTapBTG = get_tap_test_class(
    tap_class=TapBTG,
    config=SAMPLE_CONFIG
)
