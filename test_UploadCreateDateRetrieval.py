#!C:/Python37-32/python
# -*- coding: UTF-8 -*-

import unittest
import json
import os
import tempfile
import shutil

import PixivConstant
from PixivDBManager import PixivDBManager
from PixivImage import PixivImage

PixivConstant.PIXIVUTIL_LOG_FILE = 'pixivutil.test.log'


class TestUploadCreateDateRetrieval(unittest.TestCase):
    def setUp(self):
        # Create a temporary database for testing
        self.test_db_path = tempfile.mktemp(suffix=".db")
        self.db = PixivDBManager(root_directory=".", target=self.test_db_path)
        self.db.createDatabase()

    def tearDown(self):
        # Clean up the temporary database
        self.db.close()
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)

    def test_date_epoch_conversion_with_timezone(self):
        """Test converting ISO 8601 dates with timezone to epoch seconds"""
        image = PixivImage()
        
        # Test with timezone offset (+00:00)
        test_date = "2023-12-15T08:24:00+00:00"
        epoch_result = image.get_date_epoch_seconds(test_date)
        self.assertIsNotNone(epoch_result)
        self.assertIsInstance(epoch_result, int)
        # 2023-12-15T08:24:00 UTC should be 1702628640
        self.assertEqual(epoch_result, 1702628640)

    def test_date_epoch_conversion_with_japan_timezone(self):
        """Test converting ISO 8601 dates with Japan timezone to epoch seconds"""
        image = PixivImage()
        
        # Test with Japan timezone offset (+09:00)
        test_date = "2022-12-03T17:00:08+09:00"
        epoch_result = image.get_date_epoch_seconds(test_date)
        self.assertIsNotNone(epoch_result)
        self.assertIsInstance(epoch_result, int)

    def test_date_epoch_conversion_invalid_date(self):
        """Test handling of invalid date strings"""
        image = PixivImage()
        
        # Test with invalid date
        invalid_dates = [None, "", "invalid-date", "2023-13-32T25:61:61+00:00"]
        
        for invalid_date in invalid_dates:
            result = image.get_date_epoch_seconds(invalid_date)
            self.assertIsNone(result)

    def test_extract_dates_from_illust_json(self):
        """Test extracting create and upload dates from test-pixiv-illust-114245433.json test data"""
        # Load the test-pixiv-illust-114245433.json test file
        with open('./test/test-pixiv-illust-114245433.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Create a PixivImage instance and manually set the date fields
        # (simulating what would happen during JSON parsing)
        image = PixivImage()
        body_data = data['body']
        image.js_createDate = body_data.get('createDate')
        image.js_uploadDate = body_data.get('uploadDate')
        
        # Test that dates were extracted
        self.assertIsNotNone(image.js_createDate)
        self.assertIsNotNone(image.js_uploadDate)
        self.assertEqual(image.js_createDate, "2023-12-15T08:24:00+00:00")
        self.assertEqual(image.js_uploadDate, "2023-12-15T08:24:00+00:00")
        
        # Test epoch conversion
        created_epoch = image.get_created_date_epoch()
        uploaded_epoch = image.get_uploaded_date_epoch()
        self.assertIsNotNone(created_epoch)
        self.assertIsNotNone(uploaded_epoch)
        self.assertEqual(created_epoch, 1702628640)
        self.assertEqual(uploaded_epoch, 1702628640)

    def test_extract_dates_from_manga_json(self):
        """Test extracting create and upload dates from test-pixiv-manga-103301948.json test data"""
        # Load the test-pixiv-manga-103301948.json test file
        with open('./test/test-pixiv-manga-103301948.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Create a PixivImage instance and manually set the date fields
        # (simulating what would happen during JSON parsing)
        image = PixivImage()
        body_data = data['body']
        image.js_createDate = body_data.get('createDate')
        image.js_uploadDate = body_data.get('uploadDate')
        
        # Test that dates were extracted
        self.assertIsNotNone(image.js_createDate)
        self.assertIsNotNone(image.js_uploadDate)
        self.assertEqual(image.js_createDate, "2022-12-03T08:00:00+00:00")
        self.assertEqual(image.js_uploadDate, "2022-12-03T08:00:00+00:00")
        
        # Test epoch conversion
        created_epoch = image.get_created_date_epoch()
        uploaded_epoch = image.get_uploaded_date_epoch()
        self.assertIsNotNone(created_epoch)
        self.assertIsNotNone(uploaded_epoch)

    def test_database_date_info_crud(self):
        """Test CRUD operations for pixiv_date_info table"""
        image_id = 114245433
        created_epoch = 1702628640
        uploaded_epoch = 1702628640
        
        # Test insert
        self.db.insertDateInfo(image_id, created_epoch, uploaded_epoch)
        
        # Test select
        result = self.db.selectDateInfoByImageId(image_id)
        self.assertIsNotNone(result)
        self.assertEqual(result[0], created_epoch)  # created_date_epoch
        self.assertEqual(result[1], uploaded_epoch)  # uploaded_date_epoch
        
        # Test update (insert with same ID should update)
        new_created_epoch = 1702628700
        new_uploaded_epoch = 1702628700
        self.db.insertDateInfo(image_id, new_created_epoch, new_uploaded_epoch)
        
        # Verify the update
        updated_result = self.db.selectDateInfoByImageId(image_id)
        self.assertEqual(updated_result[0], new_created_epoch)
        self.assertEqual(updated_result[1], new_uploaded_epoch)

    def test_database_date_info_nonexistent(self):
        """Test selecting non-existent date info returns None"""
        result = self.db.selectDateInfoByImageId(999999999)
        self.assertIsNone(result)

    def test_database_date_info_with_null_values(self):
        """Test inserting date info with null values"""
        image_id = 123456
        
        # Test with None values
        self.db.insertDateInfo(image_id, None, None)
        result = self.db.selectDateInfoByImageId(image_id)
        self.assertIsNotNone(result)
        self.assertIsNone(result[0])  # created_date_epoch should be None
        self.assertIsNone(result[1])  # uploaded_date_epoch should be None
        
        # Test with mixed values
        created_epoch = 1702628640
        self.db.insertDateInfo(image_id, created_epoch, None)
        result = self.db.selectDateInfoByImageId(image_id)
        self.assertEqual(result[0], created_epoch)
        self.assertIsNone(result[1])


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestUploadCreateDateRetrieval)
    unittest.TextTestRunner(verbosity=2).run(suite)