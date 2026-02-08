# -*- coding: utf-8 -*-
import unittest

# Import PixivHelper first to avoid circular import when PixivImage loads PixivHelper.
import common.PixivHelper  # noqa: F401
from model.PixivImage import PixivImage


class TestPixivDateManagement(unittest.TestCase):
    def test_get_date_epoch_seconds_parses_iso8601(self):
        image = PixivImage()
        epoch = image.get_date_epoch_seconds("2023-01-02T03:04:05+00:00")
        self.assertEqual(epoch, 1672628645)


if __name__ == "__main__":
    unittest.main()
