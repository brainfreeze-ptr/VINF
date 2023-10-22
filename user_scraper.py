from selenium import webdriver
import time
import re
from collections import deque
import os


class GoodreadsScraper():

    def __init__(self, url_limit=10):
        self.url_limit = url_limit
        self.timeout = 2  # used to wait for page to load completely
        self.forbidden = ['/about/team_member/', '/admin', '/api', '/blog/list_rss', '/book/reviews/', '/book_link/follow/', '/buy_buttons/', '/ebooks', '/event/show/', '/home/index_rss', '/oggiPlayerLoader.htm', '/photo/group/', '/quotes/list_rss', '/reader', '/review/list_rss', '/review/rate', '/shelf/user_shelves', '/story', '/tooltips', '/track', '/trivia/answer', '/user/updates_rss']
        self.saved_sites = 0
        self.load_state()
        print("\n")

    def load_state(self):
        # load visited sites
        with open('visited_users.txt', 'r') as f:
            self.visited = set(f.readlines())

        # load queue
        with open('queue_users.txt', 'r') as f:
            self.queue = deque(f.readlines())
        if not self.queue:  # start with a single book if no queue found
            self.queue = deque(['https://www.goodreads.com/book/show/11.The_Hitchhiker_s_Guide_to_the_Galaxy'])

        # load sizes of scraped files
        self.download_size = 0
        for path, dirs, files in os.walk('user'):
            for f in files:
                fp = os.path.join(path, f)
                self.download_size += os.path.getsize(fp)
                self.saved_sites += 1

    def save_state(self):
        with open('visited_users.txt', 'w') as f:
            f.writelines([x if x.endswith('\n') else x + '\n' for x in self.visited])

        with open('queue_users.txt', 'w') as f:
            f.writelines([x if x.endswith('\n') else x + '\n' for x in self.queue])

    def crawl(self):
        self.t_begin = time.time()
        driver = webdriver.Chrome()
        while self.url_limit > 0:
            self.print_state()

            # visit new site, add to visited
            current_site = self.queue.popleft()
            self.visited.add(current_site)
            self.url_limit -= 1

            # load site content, wait for the content to load
            driver.get(current_site)
            time.sleep(self.timeout)
            page_content = driver.page_source

            self.save_site(current_site, page_content)

            # find other links
            found_sites = set(re.findall(r'\"(https://www\.goodreads\.com/user/show/.*?)\"', page_content))
            for site in found_sites:
                site = site[:site.index('?')] if '?' in site else site
                site = site.strip('\\/')
                if self.crawlable(current_site, site):
                    self.queue.append(site)


        self.elapsed_time = round((time.time() - self.t_begin) / 60, 2)
        self.print_state()
        print(f'Crawling session finished. Elapsed time: {self.elapsed_time} minutes')
        print(f'Saved: {self.saved_sites}, Queue size: {len(self.queue)}, users: {self.download_size/1000000:.2f} MB')
        self.save_state()
        driver.quit()

    def save_site(self, site, page_content):
        new_file_path = f"user/{re.findall(r'/(?:.(?!/))+$', site)[0]}"
        new_file_path = new_file_path[:100]  # clip long names
        if os.path.exists(new_file_path):
            return
        with open(new_file_path, 'w+') as f:
            f.write(page_content)

        self.download_size += len(page_content.encode('utf-8'))
        self.saved_sites += 1

    def crawlable(self, current_site, found_site):
        if current_site.startswith(found_site) or found_site.startswith(current_site):   # skip subpages
            return False

        if any([forbidden in found_site for forbidden in self.forbidden]):   # don't crawl forbidden sites (from robots.txt)
            return False

        if found_site in self.visited:  # don't crawl visited sites
            return False

        if found_site in self.queue:    # this site is already in queue, no need to add it twice
            return False

        return True

    def print_state(self):
        print(f'Saved: {self.saved_sites}, Queue size: {len(self.queue)}, users: {self.download_size/1000000:.2f} MB', end='                       \r')


if __name__ == '__main__':
    gs = GoodreadsScraper(200)
    gs.crawl()
