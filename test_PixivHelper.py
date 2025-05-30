#!C:/Python37-32/python
# -*- coding: UTF-8 -*-

import json
import os
import platform
from typing import Tuple
import unittest

from bs4 import BeautifulSoup

from PixivBrowserFactory import PixivBrowser
import PixivConfig
import PixivConstant
import PixivHelper
from PixivArtist import PixivArtist
from PixivImage import PixivImage

PixivConstant.PIXIVUTIL_LOG_FILE = 'pixivutil.test.log'


def mock_getMemberInfoWhitecube(self, member_id, artist: PixivArtist, bookmark=False):
    assert (artist is not None)

    return artist

def mock_getMemberPage(self, member_id, page=1, bookmark=False, tags=None, r18mode=False, throw_empty_error=False) -> Tuple[PixivArtist, str]:
    artist = PixivArtist(member_id)
    return (artist, "")

class TestPixivHelper(unittest.TestCase):
    currPath = os.path.abspath('.')
    PixivHelper.get_logger()

    def testSanitizeFilename(self):
        rootDir = '.'
        filename = u'12345.jpg'
        currentDir = os.path.abspath('.')
        expected = currentDir + os.sep + filename

        result = PixivHelper.sanitize_filename(filename, rootDir)

        self.assertEqual(result, expected)
        self.assertTrue(len(result) < 255)

    def testSanitizeFilename3(self):
        rootDir = 'D:\\Temp\\Pixiv2\\'
        if platform.system() != 'Windows':
            rootDir = '/home/travis/build/Nandaka/PixivUtil2/'

        nameformat = '%searchTags%\\%member_id% %member_token%\\%R-18% %urlFilename% - %title%'
        with open('./test/test-image-unicode-2493913.json', 'r', encoding="utf-8") as p:
            page = p.read()
        PixivBrowser.getMemberInfoWhitecube = mock_getMemberInfoWhitecube
        image = PixivImage(2493913, page)
        self.assertEqual(image.imageUrls[0], "https://i.pximg.net/img-original/img/2008/12/23/21/01/21/2493913_p0.jpg")
        filename = PixivHelper.make_filename(nameformat, image, fileUrl="2493913_p0.jpg")

        expected = "D:\\Temp\\Pixiv2\\267014 balzehn\\R-18 2493913_p0 - アラクネのいる日常２.jpg"
        if platform.system() != 'Windows':
            expected = "/home/travis/build/Nandaka/PixivUtil2/267014 balzehn/R-18 2493913_p0 - アラクネのいる日常２.jpg"

        result = PixivHelper.sanitize_filename(filename, rootDir)

        self.assertEqual(result, expected)
        self.assertTrue(len(result) < 255)

    def testCreateMangaFilename(self):
        with open(r'./test/test-image-manga-28820443.json', 'r', encoding='utf-8') as p:
            page = p.read()
        PixivBrowser.getMemberInfoWhitecube = mock_getMemberInfoWhitecube
        imageInfo = PixivImage(28820443, page)
        imageInfo.imageCount = 100

        # cross check with json value for artist info
        with open('./test/detail-554800.json', 'r') as js_file:
            js = json.load(js_file)

        assert (imageInfo.artist is not None)
        self.assertEqual(imageInfo.artist.artistId, int(js["user"]["id"]))
        self.assertEqual(imageInfo.artist.artistToken, js["user"]["account"])
        # self.assertEqual(imageInfo.artist.artistAvatar, js["user"]["profile_image_urls"]["medium"].replace("_170", ""))

        nameFormat = '%member_token% (%member_id%)\\%urlFilename% %page_number% %works_date_only% %works_res% %title%'

        expected = u'maidoll (554800)\\28865189_p0 001 2012-07-22 Multiple images: 2P C82おまけ本 「沙耶は俺の嫁」サンプル.jpg'
        result = PixivHelper.make_filename(nameFormat,
                                           imageInfo,
                                           artistInfo=None,
                                           tagsSeparator=' ',
                                           fileUrl='http://i2.pixiv.net/img26/img/ffei/28865189_p0.jpg')
        # print(result)
        self.assertEqual(result, expected)

        expected = u'maidoll (554800)\\28865189_p14 015 2012-07-22 Multiple images: 2P C82おまけ本 「沙耶は俺の嫁」サンプル.jpg'
        result = PixivHelper.make_filename(nameFormat,
                                           imageInfo,
                                           artistInfo=None,
                                           tagsSeparator=' ',
                                           fileUrl='http://i2.pixiv.net/img26/img/ffei/28865189_p14.jpg')
        # print(result)
        self.assertEqual(result, expected)

        expected = u'maidoll (554800)\\28865189_p921 922 2012-07-22 Multiple images: 2P C82おまけ本 「沙耶は俺の嫁」サンプル.jpg'
        result = PixivHelper.make_filename(nameFormat,
                                           imageInfo,
                                           artistInfo=None,
                                           tagsSeparator=' ',
                                           fileUrl='http://i2.pixiv.net/img26/img/ffei/28865189_p921.jpg')
        # print(result)
        self.assertEqual(result, expected)

    def testCreateFilenameUnicode(self):
        with open('./test/test-image-unicode-2493913.json', 'r', encoding='utf-8') as p:
            page = p.read()
        PixivBrowser.getMemberInfoWhitecube = mock_getMemberInfoWhitecube
        imageInfo = PixivImage(2493913, page)

        # cross check with json value for artist info
        with open('./test/detail-267014.json', 'r', encoding='utf-8') as js_file:
            js = json.load(js_file)

        assert (imageInfo.artist is not None)
        self.assertEqual(imageInfo.artist.artistId, int(js["user"]["id"]))
        self.assertEqual(imageInfo.artist.artistToken, js["user"]["account"])
        # self.assertEqual(imageInfo.artist.artistAvatar, js["user"]["profile_image_urls"]["medium"].replace("_170", ""))

        nameFormat = '%member_token% (%member_id%)\\%urlFilename% %works_date_only% %works_res% %title%'
        expected = u'balzehn (267014)\\2493913 2008-12-23 852x1200 アラクネのいる日常２.jpg'
        result = PixivHelper.make_filename(nameFormat,
                                           imageInfo,
                                           artistInfo=None,
                                           tagsSeparator=' ',
                                           fileUrl='http://i2.pixiv.net/img16/img/balzehn/2493913.jpg')
        # print(result)
        self.assertEqual(result, expected)

    def testCreateFilenameTranslatedTag(self):
        with open('./test/test-image-unicode-2493913.json', 'r', encoding='utf-8') as p:
            page = p.read()
        PixivBrowser.getMemberInfoWhitecube = mock_getMemberInfoWhitecube
        PixivBrowser.getMemberPage = mock_getMemberPage
        imageInfo = PixivImage(2493913, page)

        # cross check with json value for artist info
        with open('./test/detail-267014.json', 'r', encoding='utf-8') as js_file:
            js = json.load(js_file)
        assert (imageInfo.artist is not None)
        self.assertEqual(imageInfo.artist.artistId, int(js["user"]["id"]))
        self.assertEqual(imageInfo.artist.artistToken, js["user"]["account"])
        # self.assertEqual(imageInfo.artist.artistAvatar, js["user"]["profile_image_urls"]["medium"].replace("_170", ""))

        nameFormat = '%member_token% (%member_id%)\\%urlFilename% %works_date_only% %works_res% %title% %tags%'
        expected = 'balzehn (267014)\\2493913 2008-12-23 852x1200 アラクネのいる日常２ arachne monster girl モン娘のいる日常シリーズ non-human monster girl R-18 tsundere spider woman love-making.jpg'

        result = PixivHelper.make_filename(nameFormat,
                                           imageInfo,
                                           artistInfo=None,
                                           tagsSeparator=' ',
                                           fileUrl='http://i2.pixiv.net/img16/img/balzehn/2493913.jpg',
                                           useTranslatedTag=True,
                                           tagTranslationLocale="en")
        # print(result)
        self.assertEqual(result, expected)

        nameFormat2 = '%member_token% (%member_id%)\\folder%force_extension{png}%'
        expected2 = 'balzehn (267014)\\folder.png'

        result2 = PixivHelper.make_filename(nameFormat2,
                                           imageInfo,
                                           artistInfo=None,
                                           tagsSeparator=' ',
                                           fileUrl='http://i2.pixiv.net/img16/img/balzehn/2493913.jpg',
                                           useTranslatedTag=True,
                                           tagTranslationLocale="en")
        # #940
        self.assertEqual(result2, expected2)

    def testcreateAvatarFilenameFormatNoSubfolderNoRootDir(self):
        with open('./test/all-14095911.json', 'r') as p:
            artist = PixivArtist(14095911, p.read(), False, 192, 48)
        self.assertIsNotNone(artist)
        with open('./test/userdetail-14095911.json', 'r') as p2:
            info = json.loads(p2.read())
        artist.ParseInfo(info, False, False)

        targetDir = ''
        # change the config value
        _config = PixivConfig.PixivConfig()
        _config.avatarNameFormat = ''
        _config.filenameFormat = '%image_id% - %title%'
        _config.tagsSeparator = ' '
        _config.tagsLimit = 0
        PixivHelper.set_config(_config)

        filename = PixivHelper.create_avabg_filename(artist, targetDir, _config)
        self.assertEqual(filename[0], self.currPath + os.sep + u'folder.png')
        self.assertEqual(filename[1], self.currPath + os.sep + u'bg_folder.jpg')

    def testcreateAvatarFilenameFormatNoSubfolderNoRootDir883(self):
        with open('./test/all-4991959.json', 'r') as p:
            artist = PixivArtist(4991959, p.read(), False, 192, 48)
        self.assertIsNotNone(artist)
        with open('./test/userdetail-4991959.json', 'r') as p2:
            info = json.loads(p2.read())
        artist.ParseInfo(info, False, False)

        targetDir = ''
        # change the config value
        _config = PixivConfig.PixivConfig()
        _config.avatarNameFormat = ''
        _config.filenameFormat = "%artist% (%member_id%)" + os.sep + "%urlFilename% - %title%"
        _config.tagsSeparator = ' '
        _config.tagsLimit = 0
        PixivHelper.set_config(_config)

        filename = PixivHelper.create_avabg_filename(artist, targetDir, _config)
        self.assertEqual(filename[0], self.currPath + os.sep + f'{artist.artistName} ({artist.artistId}){os.sep}folder.png')
        self.assertEqual(filename[1], '')
        self.assertTrue(artist.artistAvatar != "no_profile")
        self.assertTrue(artist.artistBackground == "no_background")

    def testcreateAvatarFilenameFormatWithSubfolderNoRootDir(self):
        with open('./test/all-14095911.json', 'r') as p:
            artist = PixivArtist(14095911, p.read(), False, 192, 48)
        self.assertIsNotNone(artist)
        with open('./test/userdetail-14095911.json', 'r') as p2:
            info = json.loads(p2.read())
        artist.ParseInfo(info, False, False)

        targetDir = ''
        _config = PixivConfig.PixivConfig()
        _config.avatarNameFormat = ''
        _config.filenameFormat = '%member_token% (%member_id%)' + os.sep + '%image_id% - %title% - %tags%'
        _config.tagsSeparator = ' '
        _config.tagsLimit = 0
        PixivHelper.set_config(_config)

        filename = PixivHelper.create_avabg_filename(artist, targetDir, _config)
        self.assertEqual(filename[0], self.currPath + os.sep + u'p199451 (14095911)' + os.sep + 'folder.png')

    def testcreateAvatarFilenameFormatNoSubfolderWithRootDir3(self):
        with open('./test/all-14095911.json', 'r') as p:
            artist = PixivArtist(14095911, p.read(), False, 192, 48)
        self.assertIsNotNone(artist)
        with open('./test/userdetail-14095911.json', 'r') as p2:
            info = json.loads(p2.read())
        artist.ParseInfo(info, False, False)

        targetDir = os.path.abspath('.')
        _config = PixivConfig.PixivConfig()
        _config.avatarNameFormat = ''
        _config.filenameFormat = '%image_id% - %title%'
        _config.tagsSeparator = ' '
        _config.tagsLimit = 0
        PixivHelper.set_config(_config)

        filename = PixivHelper.create_avabg_filename(artist, targetDir, _config)
        self.assertEqual(filename[0], targetDir + os.sep + u'folder.png')

    def testcreateAvatarFilenameFormatWithSubfolderWithRootDir4(self):
        with open('./test/all-14095911.json', 'r') as p:
            artist = PixivArtist(14095911, p.read(), False, 192, 48)
        self.assertIsNotNone(artist)
        with open('./test/userdetail-14095911.json', 'r') as p2:
            info = json.loads(p2.read())
        artist.ParseInfo(info, False, False)

        targetDir = os.path.abspath('.')
        _config = PixivConfig.PixivConfig()
        _config.avatarNameFormat = ''
        _config.filenameFormat = '%member_token% (%member_id%)' + os.sep + '%R-18%' + os.sep + '%image_id% - %title% - %tags%'
        _config.tagsSeparator = ' '
        _config.tagsLimit = 0
        PixivHelper.set_config(_config)

        filename = PixivHelper.create_avabg_filename(artist, targetDir, _config)
        self.assertEqual(filename[0], targetDir + os.sep + u'p199451 (14095911)' + os.sep + 'folder.png')

    def testcreateAvatarFilenameFormatNoSubfolderWithCustomRootDir5(self):
        with open('./test/all-14095911.json', 'r') as p:
            artist = PixivArtist(14095911, p.read(), False, 192, 48)
        self.assertIsNotNone(artist)
        with open('./test/userdetail-14095911.json', 'r') as p2:
            info = json.loads(p2.read())
        artist.ParseInfo(info, False, False)

        targetDir = os.path.abspath(os.sep + 'images')
        _config = PixivConfig.PixivConfig()
        _config.avatarNameFormat = ''
        _config.filenameFormat = '%image_id% - %title%'
        _config.tagsSeparator = ' '
        _config.tagsLimit = 0
        PixivHelper.set_config(_config)

        filename = PixivHelper.create_avabg_filename(artist, targetDir, _config)
        self.assertEqual(filename[0], targetDir + os.sep + 'folder.png')

    def testcreateAvatarFilenameFormatWithSubfolderWithCustomRootDir6(self):
        with open('./test/all-14095911.json', 'r') as p:
            artist = PixivArtist(14095911, p.read(), False, 192, 48)
        self.assertIsNotNone(artist)
        with open('./test/userdetail-14095911.json', 'r') as p2:
            info = json.loads(p2.read())
        artist.ParseInfo(info, False, False)

        targetDir = os.path.abspath(os.sep + 'images')
        _config = PixivConfig.PixivConfig()
        _config.avatarNameFormat = ''
        _config.filenameFormat = '%member_token% (%member_id%)' + os.sep + '%R-18%' + os.sep + '%image_id% - %title% - %tags%'
        _config.tagsSeparator = ' '
        _config.tagsLimit = 0
        PixivHelper.set_config(_config)

        filename = PixivHelper.create_avabg_filename(artist, targetDir, _config)
        self.assertEqual(filename[0], targetDir + os.sep + 'p199451 (14095911)' + os.sep + 'folder.png')

    def testParseLoginError(self):
        with open('./test/test-login-error.htm', 'r', encoding='utf-8') as p:
            page = BeautifulSoup(p.read(), features="html5lib")
        r = page.find_all('span', attrs={'class': 'error'})
        self.assertTrue(len(r) > 0)
        self.assertEqual(u'Please ensure your pixiv ID, email address and password is entered correctly.', r[0].string)

    def testParseLoginForm(self):
        with open('./test/test-login-form.html', 'r', encoding='utf-8') as p:
            page = BeautifulSoup(p.read(), features="html5lib")
        r = page.find_all('form', attrs={'action': '/login.php'})
        # print(r)
        self.assertTrue(len(r) > 0)


if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPixivHelper)
    unittest.TextTestRunner(verbosity=5).run(suite)
