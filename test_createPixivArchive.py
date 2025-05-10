#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import unittest
import zipfile
import tempfile
import shutil
from unittest import mock

import PixivConfig
import PixivConstant
from PixivImageHandler import process_image

class TestCreatePixivArchive(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="pixiv_test_")
        self.config = PixivConfig.PixivConfig()
        self.config.rootDirectory = self.temp_dir
        
        # Mock the necessary classes and methods
        self.mock_caller = mock.MagicMock()
        self.mock_caller.__dbManager__ = mock.MagicMock()
        # Make selectImageByImageId return None to avoid "Already downloaded" check
        self.mock_caller.__dbManager__.selectImageByImageId.return_value = None
        # Set cleanupFileExists to return False to make sure file is not considered downloaded
        self.mock_caller.__dbManager__.cleanupFileExists.return_value = False
        self.mock_caller.__blacklistTags = []
        self.mock_caller.__blacklistMembers = []
        self.mock_caller.__blacklistTitles = []
        self.mock_caller.__suppressTags = []
        self.mock_caller.__seriesDownloaded = set()
        # Don't skip downloads for the test
        self.mock_caller.DEBUG_SKIP_DOWNLOAD_IMAGE = False
        
        # Mock download_image to fake download success
        self.download_patcher = mock.patch('PixivDownloadHandler.download_image')
        self.mock_download = self.download_patcher.start()
        self.mock_download.return_value = (PixivConstant.PIXIVUTIL_OK, None)
        
        # Mock browser factory to avoid actual API calls
        self.browser_patcher = mock.patch('PixivBrowserFactory.getBrowser')
        self.mock_browser_factory = self.browser_patcher.start()
        self.mock_browser = mock.MagicMock()
        self.mock_browser_factory.return_value = self.mock_browser
        
    def tearDown(self):
        self.download_patcher.stop()
        self.browser_patcher.stop()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def _setup_mock_image(self, is_manga=False):
        # Create a mock image response
        mock_image = mock.MagicMock()
        mock_image.imageId = 12345
        mock_image.imageTitle = "Test Image"
        mock_image.imageMode = "manga" if is_manga else "big"
        mock_image.imageCount = 3 if is_manga else 1
        mock_image.imageTags = ["tag1", "tag2"]
        mock_image.imageUrls = ["https://example.com/img1.jpg"] if not is_manga else ["https://example.com/img1_p0.jpg", "https://example.com/img1_p1.jpg", "https://example.com/img1_p2.jpg"]
        mock_image.imageResizedUrls = mock_image.imageUrls
        mock_image.worksDateDateTime = mock.MagicMock()
        
        # Create a mock artist
        mock_artist = mock.MagicMock()
        mock_artist.artistId = 9876
        mock_artist.artistName = "Test Artist"
        mock_artist.artistToken = "test_token"
        mock_image.artist = mock_artist
        
        # Setup the mock browser response
        self.mock_browser.getImagePage.return_value = (mock_image, None)
        
        return mock_image
    
    def test_create_pixiv_archive_single(self):
        """Test creating a zip archive for a single image"""
        # Enable createPixivArchive
        self.config.createPixivArchive = True
        
        # Setup mock image
        mock_image = self._setup_mock_image(is_manga=False)
        
        # Create target directory
        test_artist_dir = os.path.join(self.temp_dir, "test_artist")
        os.makedirs(test_artist_dir, exist_ok=True)
        
        # Mock download_image to create a sample file
        def mock_download_effect(caller, img, filename, referer, overwrite, retry, backup, image, page, notifier):
            # Create the directory
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            # Create a dummy file
            with open(filename, 'w') as f:
                f.write("test content")
            return (PixivConstant.PIXIVUTIL_OK, filename)
        
        self.mock_download.side_effect = mock_download_effect
        
        image_dir = os.path.join(test_artist_dir, "test_image")
        os.makedirs(image_dir, exist_ok=True)
        
        # Define the full expected path of files to be created
        image_filepath = os.path.join(image_dir, "test_image.jpg")
        
        # Test the process_image function
        with mock.patch('PixivHelper.make_filename', return_value=image_filepath):
            with mock.patch('PixivHelper.sanitize_filename', side_effect=lambda x, y: x):
                process_image(self.mock_caller, self.config, image_id=mock_image.imageId)
        
        # Check if zip file was created
        zip_path = os.path.join(self.temp_dir, "test_artist", "test_image.zip")
        self.assertTrue(os.path.exists(zip_path), f"ZIP file was not created at {zip_path}")
        
        # Check contents of the zip file
        with zipfile.ZipFile(zip_path, 'r') as z:
            files = z.namelist()
            self.assertEqual(len(files), 1, "Should have 1 file in the ZIP archive")
    
    def test_create_pixiv_archive_manga(self):
        """Test creating a zip archive for a manga (multiple images)"""
        # Enable createPixivArchive
        self.config.createPixivArchive = True
        self.config.createMangaDir = False
        
        # Setup mock image
        mock_image = self._setup_mock_image(is_manga=True)
        
        # Create test directories
        test_artist_dir = os.path.join(self.temp_dir, "test_artist")
        os.makedirs(test_artist_dir, exist_ok=True)
        
        test_manga_dir = os.path.join(test_artist_dir, "12345")
        os.makedirs(test_manga_dir, exist_ok=True)
        
        # Mock download_image to create sample files
        def mock_download_effect(caller, img, filename, referer, overwrite, retry, backup, image, page, notifier):
            # Create the directory
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            # Create a dummy file
            with open(filename, 'w') as f:
                f.write(f"test content page {page}")
            return (PixivConstant.PIXIVUTIL_OK, filename)
        
        self.mock_download.side_effect = mock_download_effect
        
        # Define filenames for the three manga pages
        filenames = [
            os.path.join(test_manga_dir, "12345_p0.jpg"),
            os.path.join(test_manga_dir, "12345_p1.jpg"),
            os.path.join(test_manga_dir, "12345_p2.jpg")
        ]
        
        # Test the process_image function with manga
        with mock.patch('PixivHelper.make_filename', side_effect=filenames):
            with mock.patch('PixivHelper.sanitize_filename', side_effect=lambda x, y: x):
                process_image(self.mock_caller, self.config, image_id=mock_image.imageId)
        
        # Check if zip file was created
        zip_path = os.path.join(self.temp_dir, "test_artist", "12345.zip")
        self.assertTrue(os.path.exists(zip_path), f"ZIP file was not created at {zip_path}")
        
        # Check contents of the zip file
        with zipfile.ZipFile(zip_path, 'r') as z:
            files = z.namelist()
            self.assertEqual(len(files), 3, "Should have 3 files in the ZIP archive")
            
    def test_create_pixiv_archive_with_zip_in_format(self):
        """Test creating a zip archive with explicit .zip in format"""
        # Enable createPixivArchive
        self.config.createPixivArchive = True
        # Set format with explicit .zip
        self.config.filenameFormat = "%artist%/%image_id%.zip/p_%page_number%"
        
        # Setup mock image
        mock_image = self._setup_mock_image(is_manga=False)
        
        # Create the target directory structure
        artist_dir = os.path.join(self.temp_dir, "Test Artist")
        os.makedirs(artist_dir, exist_ok=True)
        
        # Mock download_image to create a sample file
        def mock_download_effect(caller, img, filename, referer, overwrite, retry, backup, image, page, notifier):
            # Create the directory
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            # Create a dummy file
            with open(filename, 'w') as f:
                f.write("test content")
            return (PixivConstant.PIXIVUTIL_OK, filename)
        
        self.mock_download.side_effect = mock_download_effect
        
        test_file = os.path.join(self.temp_dir, "tmp", "test_file.jpg")
        
        # For this test, we'll need to mock the make_filename to return different values
        def mock_sanitize_filename(path, root_dir):
            if path.endswith(".zip"):
                # For the zip file
                return os.path.join(self.temp_dir, "Test Artist", "12345.zip")
            else:
                # For the content inside
                return test_file
        
        # Test the process_image function with explicit .zip format
        with mock.patch('PixivHelper.make_filename', return_value="Test Artist/12345.zip"):
            with mock.patch('PixivHelper.sanitize_filename', side_effect=mock_sanitize_filename):
                process_image(self.mock_caller, self.config, image_id=mock_image.imageId)
        
        # Check if zip file was created at the expected location
        zip_path = os.path.join(self.temp_dir, "Test Artist", "12345.zip")
        self.assertTrue(os.path.exists(zip_path), f"ZIP file was not created at {zip_path}")
        
        # Check contents of the zip file - should use the format after .zip
        with zipfile.ZipFile(zip_path, 'r') as z:
            files = z.namelist()
            self.assertEqual(len(files), 1, "Should have 1 file in the ZIP archive")

    def test_extract_existing_archive(self):
        """Test extracting and checking an existing archive"""
        # Enable createPixivArchive
        self.config.createPixivArchive = True
        self.config.overwrite = False
        self.config.alwaysCheckFileSize = False
        
        # Create a sample archive in the database
        mock_image = self._setup_mock_image(is_manga=True)
        
        # Create a test archive file
        test_artist_dir = os.path.join(self.temp_dir, "test_artist")
        os.makedirs(test_artist_dir, exist_ok=True)
        archive_path = os.path.join(test_artist_dir, "test_archive.zip")
        
        # Create files that will be in the archive
        with zipfile.ZipFile(archive_path, 'w') as z:
            z.writestr("page1.jpg", "test page 1 content")
            z.writestr("page2.jpg", "test page 2 content")
        
        # Setup DB to simulate existing archive
        self.mock_caller.__dbManager__.selectImageByImageId.return_value = [archive_path]
        self.mock_caller.__dbManager__.cleanupFileExists.return_value = True
        
        print(f"Debug: archive_path={archive_path}, exists={os.path.exists(archive_path)}")
        
        # Direct test of our modified condition 
        # Let's manually test the condition that should trigger our archive extraction:
        # if in_db and config.createPixivArchive and exists:
        in_db = True
        exists = True
        
        print(f"DEBUG: Condition would evaluate to: {in_db and self.config.createPixivArchive and exists}")
        
        # Now we'll only test the archive extraction part, not the whole process_image flow
        # This avoids issues with the mock objects not being fully set up for downstream processing
        with mock.patch('PixivHelper.print_and_log') as mock_log:
            with mock.patch('zipfile.ZipFile.extractall') as mock_extract:
                # We'll patch getBrowser to raise an exception after extraction to avoid downstream errors
                with mock.patch('PixivBrowserFactory.getBrowser', side_effect=Exception("Stopping after extraction")):
                    try:
                        process_image(self.mock_caller, self.config, image_id=mock_image.imageId)
                    except Exception as e:
                        # We expect an exception from our patched getBrowser
                        if "Stopping after extraction" not in str(e):
                            raise  # Only handle our expected exception
                    
                    # Verify that extractall was called
                    mock_extract.assert_called_once()
                    # Check if the extraction log message was printed
                    extract_msg_found = False
                    for call in mock_log.call_args_list:
                        args, _ = call
                        if len(args) >= 2 and 'extracting' in str(args[1]).lower():
                            extract_msg_found = True
                            break
                    
                    self.assertTrue(extract_msg_found, "Archive extraction message not found in logs")
                
        # Now test the case where the extraction fails
        with mock.patch('zipfile.is_zipfile', return_value=True):
            with mock.patch('zipfile.ZipFile') as mock_zipfile:
                mock_zipfile.return_value.__enter__.return_value.extractall.side_effect = Exception("Test extraction error")
                # Same approach to avoid downstream errors
                with mock.patch('PixivBrowserFactory.getBrowser', side_effect=Exception("Stopping after extraction")):
                    with mock.patch('PixivHelper.print_and_log') as mock_log:
                        try:
                            process_image(self.mock_caller, self.config, image_id=mock_image.imageId)
                        except Exception as e:
                            # We expect an exception from our patched getBrowser
                            if "Stopping after extraction" not in str(e):
                                raise  # Only handle our expected exception

                        # Verify error was logged
                        error_msg_found = False
                        for call in mock_log.call_args_list:
                            args, _ = call
                            if len(args) >= 2 and 'error extracting archive' in str(args[1]).lower():
                                error_msg_found = True
                                break
                        self.assertTrue(error_msg_found, "Error extraction message not found in logs")

if __name__ == '__main__':
    unittest.main() 