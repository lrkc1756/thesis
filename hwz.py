import requests
import time
import csv
import json
from bs4 import BeautifulSoup, NavigableString, Tag
from datetime import datetime, timedelta
from urllib.parse import urljoin
import os
import sys
import re

def normalize_last_seen(last_seen):
    now = datetime.now()

    if last_seen.startswith("Today"):
        return now.strftime("%Y-%m-%d")
    elif last_seen.startswith("Yesterday"):
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")

    if re.search(r"(minute|hour)", last_seen, re.IGNORECASE):
        return now.strftime("%Y-%m-%d")

    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for i, day in enumerate(weekdays):
        if last_seen.startswith(day):
            today_weekday = now.weekday()
            diff = (today_weekday - i) % 7
            diff = diff if diff != 0 else 7
            return (now - timedelta(days=diff)).strftime("%Y-%m-%d")

    months = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
}

    for m in months:
        if last_seen.startswith(m):
            parts = last_seen.replace(",", "").split()

            month = months[parts[0]]

            # Case 1: "Jan 5 2024"
            if len(parts) >= 3 and parts[2].isdigit():
                day = parts[1].zfill(2)
                year = parts[2]
                return f"{year}-{month}-{day}"

            # Case 2: "Jan 2024"
            elif len(parts) == 2 and parts[1].isdigit():
                year = parts[1]
                return f"{year}-{month}-01"

            # Case 3: "Jan 5" (no year → use 2026)
            elif len(parts) == 2:
                day = parts[1].zfill(2)
                return f"2026-{month}-{day}"

            # Case 4: just "Jan"
            else:
                return f"2026-{month}-01"

    return last_seen.split()[0]

# Logging setup
start_time = time.time()
os.makedirs("./log/infotech", exist_ok=True)
consolelog = open('./log/infotech/hwz_scraper_infotech_v3_log.txt','w', encoding='utf-8')
class logging:
    def __init__(self, stream):
        self.stream = stream
    def write(self, data):
        self.stream.write(data)
        consolelog.write(data)
    def flush(self):
        self.stream.flush()
        consolelog.flush()
sys.stdout = logging(sys.stdout)


def extract_post_text(bbWrapper):
    if bbWrapper is None:
        return "", "", [], []

    parts, images, links = [], [], []
    quoted_text = ""

    def handle_media_embed(tag):
        media_type = tag.get("data-s9e-mediaembed")
        iframe_data = tag.get("data-s9e-mediaembed-iframe")

        for span in tag.find_all("span"):
            if span.has_attr("data-s9e-mediaembed-iframe"):
                iframe_data = span["data-s9e-mediaembed-iframe"]
                break

        if not media_type or not iframe_data:
            return

        iframe_data = iframe_data.replace("\\/", "/")

        if media_type == "twitter" and "twitter.min.html#" in iframe_data:
            tweet_id = iframe_data.split("#")[-1].strip('"')
            parts.append(f"[Twitter Post] https://twitter.com/i/web/status/{tweet_id}")
        elif media_type == "youtube" and "youtube.com/embed/" in iframe_data:
            video_id = iframe_data.split("youtube.com/embed/")[-1].split('"')[0]
            parts.append(f"[YouTube Video] https://www.youtube.com/watch?v={video_id}")
        elif media_type == "instagram":
            parts.append("[Instagram Post]")
        elif media_type == "tiktok":
            parts.append("[TikTok Video]")
        elif media_type == "reddit":
            parts.append("[Reddit Post]")

    for elem in bbWrapper.contents:
        if isinstance(elem, NavigableString):
            parts.append(str(elem))
            continue

        if isinstance(elem, Tag):
            if elem.name == "span" and elem.has_attr("data-s9e-mediaembed"):
                handle_media_embed(elem)
                continue

            if elem.name == "blockquote" and "bbCodeBlock--quote" in elem.get("class", []):
                quote_div = elem.find("div", class_="bbCodeBlock-content")
                if quote_div:
                    quoted_text = quote_div.get_text("\n", strip=True)
                continue

            if elem.name == "br":
                parts.append("\n")

            if elem.name == "img":
                if "smilie" in elem.get("class", []) or "smilie--emoji" in elem.get("class", []):
                    parts.append(elem.get("alt", ""))
                elif elem.get("src"):
                    images.append(elem.get("src"))

            if elem.name == "a" and elem.get("href"):
                links.append(elem["href"])

            for sub in elem.descendants:
                if isinstance(sub, NavigableString):
                    parts.append(str(sub))
                elif isinstance(sub, Tag):
                    if sub.name == "span" and sub.has_attr("data-s9e-mediaembed"):
                        handle_media_embed(sub)
                    elif sub.name == "img":
                        if "smilie" in sub.get("class", []) or "smilie--emoji" in sub.get("class", []):
                            parts.append(sub.get("alt", ""))
                        elif sub.get("src"):
                            images.append(sub.get("src"))
                    elif sub.name == "a" and sub.get("href"):
                        links.append(sub["href"])

    return ''.join(parts).strip(), quoted_text.strip(), images, links


def extract_threads_from_forum(forum_url, max_threads=5, max_forum_pages=1):
    headers = {'User-Agent': 'Mozilla/5.0'}
    threads = []

    for page_num in range(1, max_forum_pages + 1):
        url = forum_url if page_num == 1 else f"{forum_url}page-{page_num}"
        print(f"\n🔍 Scanning forum page: {url}")
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            print(f"❌ Failed to load forum page {page_num}")
            continue

        soup = BeautifulSoup(res.text, 'html.parser')
        thread_blocks = soup.select(".structItem--thread")
        print(f"Found {len(thread_blocks)} thread blocks on this page.")

        for item in thread_blocks:
            link_tag = item.select_one(".structItem-title a")
            if link_tag and link_tag.has_attr("href"):
                thread_url = urljoin("https://forums.hardwarezone.com.sg/", link_tag['href'])
                title = link_tag.get_text(strip=True)
                threads.append((title, thread_url))
                print(f"🧵 Thread: {title} → {thread_url}")
                if len(threads) >= max_threads:
                    return threads
        time.sleep(2)
    return threads


def scrape_hwz_thread(base_url, start_page, end_page, date_start_str, date_end_str, thread_title="", thread_url=""):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    date_start = datetime.strptime(date_start_str, "%Y-%m-%d")
    date_end = datetime.strptime(date_end_str, "%Y-%m-%d")
    all_posts = []

    for page in range(start_page, end_page + 1):
        url = base_url if page == 1 else f"{base_url}page-{page}"
        print(f"Fetching: {url}")

        crawl_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            print(f"Failed to load page {page}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        posts = soup.find_all("article", class_="message--post")

        for post in posts:
            # ✅ Save only this post’s HTML snippet
            post_html = str(post)

            post_id = post.get("data-content", "").replace("post-", "")
            username_tag = post.find("a", class_="username")
            username = username_tag.get_text(strip=True) if username_tag else "Unknown"
            user_id = username_tag.get("data-user-id") if username_tag else None
            user_title_tag = post.find("h5", class_="userTitle")
            user_title = user_title_tag.get_text(strip=True) if user_title_tag else ""
            joined = "" #test
            messages = "" #test
            reactions_score = "" #test
            last_seen = "Hidden/Privacy" #test
            points = "0" #test

            # User stats
            joined = messages = reactions_score = ""
            for dl in post.find_all("dl", class_="pairs--justified"):
                key = dl.find("dt").get_text(strip=True)
                val = dl.find("dd").get_text(strip=True)
                if key == "Joined":
                    joined = val
                elif key == "Messages":
                    messages = val
                elif key == "Reaction score":
                    reactions_score = val

            # Post datetime (datePublished)
            time_tag = post.find("time")
            post_datetime = time_tag["datetime"] if time_tag and time_tag.has_attr("datetime") else ""
            try:
                post_time_obj = datetime.strptime(post_datetime, "%Y-%m-%dT%H:%M:%S%z")
            except:
                continue

            # Filter by date
            if not (date_start <= post_time_obj.replace(tzinfo=None) <= date_end):
                print(f"Skipped post outside date: {post_time_obj.date()}")
                continue

            # Check for updated/edited timestamp
            date_published = post_datetime
            date_updated = post_datetime
            edited_tag = post.find("time", class_="u-concealed")
            if edited_tag and edited_tag.has_attr("datetime"):
                date_updated = edited_tag["datetime"]

            # Post number
            number_tag = post.find("ul", class_="message-attribution-opposite")
            post_number = number_tag.get_text(strip=True) if number_tag else ""

            # Post content
            content_div = post.find("div", class_="bbWrapper")
            post_content, quoted_text, post_images, post_links = extract_post_text(content_div)

            # Reactions
            reactions = ""
            reactions_block = post.find("div", class_="reactionsBar")
            if reactions_block:
                link = reactions_block.find("a", class_="reactionsBar-link")
                if link:
                    reactions = "Reactions: " + link.get_text(strip=True)

            
            #last seen and points TEST
            if username_tag and username_tag.has_attr('href'):
                # XenForo trick: Adding 'tooltip' to the end of the member URL
                user_url = urljoin("https://forums.hardwarezone.com.sg/", username_tag['href'])
                tooltip_url = f"{user_url.rstrip('/')}/tooltip" 
                
                try:
                    # Request the tooltip HTML specifically
                    u_res = requests.get(tooltip_url, headers=headers, timeout=5)
                    if u_res.status_code == 200:
                        u_soup = BeautifulSoup(u_res.text, "html.parser")
                        
                        # Last seen is often in a blurb class in the tooltip
                        ls_tag = u_soup.find("dt", string=lambda t: t and "Last seen" in t)
                        if ls_tag:
                            last_seen = ls_tag.find_next("dd").get_text(strip=True)
                        
                        # <-- Add normalization here
                        # Normalize "Last seen" to only date
                        if last_seen:
                            last_seen = normalize_last_seen(last_seen)

                        # Points are in the memberTooltip-stats section
                        pt_tag = u_soup.find("dt", title="Trophy points")
                        if pt_tag:
                            points = pt_tag.find_next("dd").get_text(strip=True)
                        elif u_soup.find("dt", string="Points"):
                            points = u_soup.find("dt", string="Points").find_next("dd").get_text(strip=True)
                
                    time.sleep(1) 
                except Exception as e:
                    print(f"Error getting tooltip for {username}: {e}")

            # ✅ Append all extracted info
            all_posts.append({
                "thread_title": thread_title,
                "thread_url": thread_url,
                "post_id": post_id,
                "username": username,
                "user_id": user_id,
                "user_title": user_title,
                "user_joined_date": joined,
                "user_last_seen": last_seen,     # ✅ NEW
                "user_points": points,           # ✅ NEW
                "user_message_count": messages,
                "user_reaction_score": reactions_score,
                "post_datetime": post_datetime,
                "post_number": post_number,
                "quoted_text": quoted_text,
                "post_content": post_content,
                "post_images": post_images,
                "post_links": post_links,
                "reactions": reactions,
                "html": post_html,                  
                "dateCrawled": crawl_time,          
                "datePublished": date_published,    
                "dateUpdated": date_updated         
            })

        time.sleep(5)

    return all_posts



def scrape_forum_threads(forum_url, max_threads, max_forum_pages, thread_start_page, thread_end_page, date_start, date_end):
    all_posts = []
    threads = extract_threads_from_forum(forum_url, max_threads, max_forum_pages)

    for thread_title, thread_url in threads:
        print(f"\n=== Scraping thread: {thread_title} ===")
        posts = scrape_hwz_thread(
            base_url=thread_url,
            start_page=thread_start_page,
            end_page=thread_end_page,
            date_start_str=date_start,
            date_end_str=date_end,
            thread_title=thread_title,
            thread_url=thread_url
        )
        all_posts.extend(posts)

    return all_posts



if __name__ == "__main__":
    forum_url = "https://forums.hardwarezone.com.sg/forums/hardware-clinic.2/"
    max_threads = 5 # No. of threads to scrape from the forum index (e.g., scrape 3 thread titles only), 51 for full
    max_forum_pages = 1 # No. of forum listing pages to scan (each forum page lists ~20 threads), 6 for full
    thread_start_page = 1 #Starting page number within each thread to scrape (usually starts at 1)
    thread_end_page = 1 # Last page number within each thread to scrape (e.g., scrape first 3 pages of posts), 21 for full
    date_start = "2020-01-01"
    date_end = "2025-12-31"

    posts = scrape_forum_threads(forum_url, max_threads, max_forum_pages, thread_start_page, thread_end_page, date_start, date_end)

    if not posts:
        print("⚠️ No posts scraped.")
    else:
        os.makedirs("./crawled_data/infotech", exist_ok=True)
        with open("./crawled_data/infotech/hwz_infotech_v3_posts.json", "w", encoding="utf-8") as f:
            json.dump(posts, f, indent=2, ensure_ascii=False)

        with open("./crawled_data/infotech/hwz_infotech_v3_posts.csv", "w", encoding="utf-8", newline="") as f:
            fieldnames = list(posts[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(posts)

        print(f"✅ Done. {len(posts)} posts saved.")

    elapsed = time.time() - start_time
    print(f"⏱️ Execution time: {int(elapsed // 60)} min {elapsed % 60:.2f} sec")