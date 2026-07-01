import os
import threading
from queue import Queue
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://files.dahua.support/"
OUTPUT_DIR = "Dahua"

CRAWL_THREADS = 16
DOWNLOAD_THREADS = 32


SKIP_DIRS = {
    "cgi-bin"
}

session = requests.Session()
session.headers["User-Agent"] = "Mozilla/5.0"

visited = set()
visited_lock = threading.Lock()

crawl_queue = Queue()
download_queue = Queue()


def download_worker():
    while True:
        item = download_queue.get()
        if item is None:
            download_queue.task_done()
            break

        url, path = item

        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)

            if os.path.exists(path):
                print("[SKIP]", path)
            else:
                r = session.get(url, stream=True, timeout=120)
                r.raise_for_status()

                with open(path, "wb") as f:
                    for chunk in r.iter_content(1024 * 1024):
                        if chunk:
                            f.write(chunk)

                print("[OK]", path)

        except Exception as e:
            print("[ERROR]", url)
            print(e)

        finally:
            download_queue.task_done()


def crawl_worker():
    while True:
        item = crawl_queue.get()

        if item is None:
            crawl_queue.task_done()
            break

        url, local = item

        try:
            with visited_lock:
                if url in visited:
                    crawl_queue.task_done()
                    continue
                visited.add(url)

            print("[DIR]", url)

            r = session.get(url, timeout=30)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")

            for a in soup.find_all("a"):

                href = a.get("href")

                if not href:
                    continue

                # Ignore apache sorting links
                if href.startswith("?"):
                    continue

                # Ignore parent directory
                if href == "../":
                    continue

                full = urljoin(url, href)

                if href.endswith("/"):
                    dirname = unquote(href[:-1])

                    if dirname.lower() in SKIP_DIRS:
                        continue

                    crawl_queue.put((
                        full,
                        os.path.join(local, dirname)
                    ))

                else:
                    filename = unquote(os.path.basename(href))

                    if not filename:
                        continue

                    download_queue.put((
                        full,
                        os.path.join(local, filename)
                    ))

        except Exception as e:
            print("[ERROR]", url)
            print(e)

        finally:
            crawl_queue.task_done()


# Start download workers
download_threads = []
for _ in range(DOWNLOAD_THREADS):
    t = threading.Thread(target=download_worker, daemon=True)
    t.start()
    download_threads.append(t)


crawl_threads = []
for _ in range(CRAWL_THREADS):
    t = threading.Thread(target=crawl_worker, daemon=True)
    t.start()
    crawl_threads.append(t)

crawl_queue.put((BASE_URL, OUTPUT_DIR))

crawl_queue.join()

for _ in crawl_threads:
    crawl_queue.put(None)

for t in crawl_threads:
    t.join()

download_queue.join()

for _ in download_threads:
    download_queue.put(None)

for t in download_threads:
    t.join()

print("DONE")
