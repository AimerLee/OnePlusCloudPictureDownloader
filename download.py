import asyncio
import json
import os
import pickle
from typing import List, Dict, Tuple

import aiofiles
import aiohttp
import browser_cookie3
import requests
from tqdm import tqdm

LIST_PHOTOS_API = 'https://cloud.h2os.com/gallery/pc/listNormalPhotos'
QUERY_REAL_PHOTO_URL = 'https://cloud.h2os.com/gallery/pc/getRealPhotoUrls'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:73.0) Gecko/20100101 Firefox/73.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Content-Type': 'application/x-www-form-urlencoded',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://cloud.h2os.com',
    'Connection': 'keep-alive',
    'Referer': 'https://cloud.h2os.com/',
    'Cache-Control': 'max-age=0',
}


class PhotoInfoCollector:
    """
    Class used to collect the id and title information of all the photos in the cloud.
    Since the provided API can only retrieve photos in sequence, this stage shall take some time.
    """

    def __init__(self, cookies):
        self.cookies = cookies

    @staticmethod
    def payload(cursor, photo_index):
        return {
            'size': '100',
            'state': 'active',
            'smallPhotoScaleParams': 'image/resize,m_mfit,h_250,w_250',
            'originalPhotoScaleParams': 'image/resize,m_mfit,h_1300,w_1300',
            'cursor': cursor,
            'photoIndex': photo_index
        }

    def list_all_photos(self):
        """
        List all the photos in your album, returns the ids of the photo and the title of the photos
        :return: List of (id, title)
        """
        cursor, photo_index = '', ''
        all_photo_infos = []

        res_json = self.list_photos(cursor, photo_index)
        photo_nums = int(res_json['totalCount'])
        with tqdm(total=photo_nums) as pbar:
            while cursor != 'EOF':
                res_json = self.list_photos(cursor, photo_index)
                photo_infos = self.extract_id_and_title(res_json)
                all_photo_infos += photo_infos
                pbar.update(len(photo_infos))
                cursor, photo_index = self.parse_next_pos(res_json)
        return all_photo_infos

    def list_photos(self, cursor: str, photo_index: str) -> Dict:
        """
        :param cursor: the cursor position in the album
        :param photo_index: the index of the photo to start
        :return: the information of next few photos
        """
        res = requests.post(LIST_PHOTOS_API, headers=headers,
                            data=self.payload(cursor, photo_index), cookies=cookies)
        return json.loads(res.content.decode())

    @staticmethod
    def parse_next_pos(res_json: Dict) -> Tuple[str, str]:
        """
        Return next position of cursor and next photo_index
        :param res_json: the res_json of list_photos to parse
        :return: next position of cursor and next photo_index
        """
        next_cursor = res_json['lastMatchedMoment']
        next_photo_index = res_json['realPhotoIndex']
        return next_cursor, next_photo_index

    @staticmethod
    def extract_id_and_title(res_json: Dict) -> List[Tuple[str, str]]:
        """
        Extract id and title of all available photos in res_json from the result of list_photos
        :param res_json:
        :return: the list of id and title of each photo
        """
        photo_infos = []
        for photos in res_json['photos'].values():
            photo_infos += [(photo['id'], photo['title']) for photo in photos]
        return photo_infos


class TaskScheduler:
    """
    Class used to split the download task into several blocks
    """

    def __init__(self, cookies: Dict[str, str], photo_infos: List[Tuple[str, str]]):
        self.cookies = cookies
        self.photo_infos = photo_infos

    def download(self, save_dir: str = ".", block_size: int = 50) -> None:
        """
        :param save_dir: the directory to save the photos
        :param block_size: the num of pictures download concurrently, recommend value <=100
        :return:
        """
        downloader = Downloader(cookies, save_dir)
        block_num = len(self.photo_infos) // block_size
        for block_idx in tqdm(range(block_num)):
            target_photos = self.photo_infos[block_idx * block_size:(block_idx + 1) * block_size]
            asyncio.run(downloader.download(target_photos))


class Downloader:
    """
    Class used to download photos using coroutine.
    """

    def __init__(self, cookies: Dict[str, str], save_dir: str = '.'):
        """
        :param cookies: cookie used to pass authorization.
        :param save_dir: directory used to save the photos
        """
        self.cookies = cookies
        self.save_dir = save_dir

    async def download(self, photo_infos: List[Tuple[str, str]]) -> None:
        """
        Download photos by their infos.
        :param photo_infos: list contains the id and title tuple of each photo.
        """
        photo_links = self.get_photo_real_download_links(photo_infos)
        tasks = [self.download_single_photo(file_name, url) for file_name, url in photo_links.items()]

        responses = []
        for f in asyncio.as_completed(tasks):
            responses.append(await f)

    async def download_single_photo(self, photo_name: str, url: str) -> None:
        """
        Download the target photo and save it.
        :param photo_name: the file name used to save the photo as.
        :param url: the url used to download the photo
        """
        async with aiohttp.ClientSession(cookies=self.cookies) as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    f = await aiofiles.open(os.path.join(self.save_dir, photo_name), mode='wb')
                    await f.write(await resp.read())
                    await f.close()

    def get_photo_real_download_links(self, photo_infos: List[Tuple[str, str]]) -> Dict[str, str]:
        """
        Return a dict contains every photo's title as key and its download url as value.
        :param photo_infos: list contains the id and title tuple of each photo.
        :return: dict[id, download_url]
        """
        ids = [item[0] for item in photo_infos]
        photo_urls = self.query_download_link_by_ids(ids)

        photo_links = {}
        for id, title in photo_infos:
            photo_links[title] = photo_urls[id]
        return photo_links

    def query_download_link_by_ids(self, ids: List[str]) -> Dict[str, str]:
        """
        Query the download links of original photos by their id.
        :param ids: the id list of the photos
        :return: dict[id, download_url]
        """
        res = requests.post(QUERY_REAL_PHOTO_URL, headers=headers,
                            cookies=self.cookies, data={'ids': '%s' % ids})
        return json.loads(res.content.decode())


def load_cookie(browser: str = "chrome") -> Dict[str, str]:
    """
    Load cookie of h2os.com to download photo from h2 cloud.
    :param browser: the browser to load cookie from (chrome or firefox only).
    :return: a dict contains the key-value pairs in the cookie.
    """
    domain_name = 'h2os.com'
    if browser == "chrome":
        cj = browser_cookie3.chrome(domain_name=domain_name)
    elif browser == "firefox":
        cj = browser_cookie3.firefox(domain_name=domain_name)
    else:
        raise Exception("Only accept Chrome or Firefox for browser")
    cookie = {item.name: item.value for item in cj}
    return cookie


if __name__ == '__main__':
    browser = "chrome"
    save_dir = "./download"
    cookies = load_cookie(browser)

    # Stage one: collection the id and title of all the photos in the cloud
    photo_infos = PhotoInfoCollector(cookies).list_all_photos()
    with open("photo_info", "wb") as f:
        pickle.dump(photo_infos, f)

    # Stage two: download the photos using coroutine.
    with open("photo_info", "rb") as f:
        photo_infos = pickle.load(f)
        task_dispatcher = TaskScheduler(cookies, photo_infos)
        task_dispatcher.download(save_dir)
