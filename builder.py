import asyncio
import json
import logging
import os
import re
import glob
from datetime import datetime
from playwright.async_api import async_playwright

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AnimePaheBuilder:
    def __init__(self):
        self.base_url = "https://animepahe.si"
        self.anime_index_dir = "anime_index"
        self.popular_anime_file = "popular_anime.json"
        self.fresh_episodes_file = "fresh_episodes.json"
        self.search_index_file = "search_index.json"
        self.all_anime = []

    def load_anime_index(self):
        """Load all anime from the index directory."""
        logger.info("📚 Loading anime index...")
        json_files = glob.glob(os.path.join(self.anime_index_dir, 'anime_*.json'))
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'anime' in data:
                        self.all_anime.extend(data['anime'])
            except Exception as e:
                logger.error(f"❌ Error loading {json_file}: {e}")
        logger.info(f"✅ Loaded {len(self.all_anime)} anime titles.")

    def build_search_index(self):
        """Create a lightweight search index."""
        logger.info("🔍 Building search index...")
        search_index = []
        for anime in self.all_anime:
            search_index.append({
                'id': anime['id'],
                'title': anime['title']
            })

        with open(self.search_index_file, 'w', encoding='utf-8') as f:
            json.dump(search_index, f, separators=(',', ':'), ensure_ascii=False)
        logger.info(f"✅ Search index saved to {self.search_index_file}")

    def update_popular_anime(self):
        """Update the popular anime list."""
        logger.info("🔥 Updating popular anime...")
        popular_titles = [
            "Jujutsu Kaisen", "One Piece", "Dan Da Dan", "Kaiju No. 8", "Black Clover",
            "Demon Slayer", "Akame ga Kill!", "Chainsaw Man", "Naruto", "Bleach",
            "Eminence in Shadow", "Attack on Titan", "My Hero Academia", "Spy x Family",
            "Tokyo Revengers", "Dr. Stone", "Blue Lock", "Haikyuu", "One Punch Man",
            "Mob Psycho 100", "Hunter x Hunter", "Death Note", "Fullmetal Alchemist: Brotherhood",
            "Code Geass", "Steins;Gate", "Re:Zero", "Konosuba", "Overlord"
        ]

        found_anime = []
        seen_ids = set()

        # 1. Exact/Prefix match
        for title in popular_titles:
            for anime in self.all_anime:
                if anime['title'].lower().startswith(title.lower()):
                    if anime['id'] not in seen_ids:
                        seen_ids.add(anime['id'])
                        found_anime.append(anime)
                        break

        # 2. Fuzzy fallback
        if len(found_anime) < 5:
             for title in popular_titles:
                for anime in self.all_anime:
                    if title.lower() in anime['title'].lower() and anime['id'] not in seen_ids:
                        seen_ids.add(anime['id'])
                        found_anime.append(anime)
                        break

        with open(self.popular_anime_file, 'w', encoding='utf-8') as f:
            json.dump(found_anime, f, indent=2, ensure_ascii=False)
        logger.info(f"✅ Popular anime saved to {self.popular_anime_file}")

    async def update_fresh_episodes(self):
        """Scrape fresh episodes and their iframes."""
        logger.info("📺 Updating fresh episodes...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()

            # --- 1. Get List of Fresh Episodes ---
            fresh_episodes = []
            try:
                for page_num in range(1, 4):
                    url = f"{self.base_url}?page={page_num}" if page_num > 1 else self.base_url
                    logger.info(f"   Scraping page {page_num}: {url}")

                    await page.goto(url, wait_until='networkidle', timeout=60000)

                    # DDoS check
                    title = await page.title()
                    if "Just a moment" in title or "DDoS-Guard" in title:
                        logger.info("   🛡️ DDoS Guard detected, waiting...")
                        await page.wait_for_timeout(5000)
                        await page.reload(wait_until='networkidle')

                    episode_links = await page.query_selector_all('a[href*="/play/"]')

                    # De-duplicate links on page
                    unique_links = []
                    seen_hrefs = set()
                    for link in episode_links:
                        href = await link.get_attribute('href')
                        if href and href not in seen_hrefs:
                            seen_hrefs.add(href)
                            unique_links.append(link)

                    for link in unique_links:
                        if len(fresh_episodes) >= 30: break

                        href = await link.get_attribute('href')
                        match = re.search(r'/play/([a-f0-9-]+)/([a-f0-9]+)', href)
                        if not match: continue

                        anime_id, session_id = match.groups()

                        # Extract text
                        text = await link.text_content()
                        # Try to get parent text for better context
                        parent = await link.query_selector('xpath=..')
                        if parent:
                            text = await parent.text_content()

                        anime_name, ep_num = self.parse_episode_text(text)

                        fresh_episodes.append({
                            'anime_id': anime_id,
                            'session_id': session_id,
                            'anime_name': anime_name,
                            'episode_number': ep_num,
                            'episode_url': f"{self.base_url}{href}" if href.startswith('/') else href,
                            'iframe_url': None
                        })

                    if len(fresh_episodes) >= 30: break
            except Exception as e:
                logger.error(f"❌ Error scraping list: {e}")

            # --- 2. Get Iframes for each episode ---
            logger.info(f"   Fetching iframes for {len(fresh_episodes)} episodes...")

            # Process in chunks or sequentially
            for i, episode in enumerate(fresh_episodes):
                try:
                    logger.info(f"   [{i+1}/{len(fresh_episodes)}] {episode['anime_name']} Ep {episode['episode_number']}...")
                    iframe = await self.get_iframe_for_episode(page, episode['episode_url'])
                    episode['iframe_url'] = iframe

                    if iframe:
                        logger.info(f"      ✅ Got iframe")
                    else:
                        logger.warning(f"      ⚠️ No iframe found")

                    # Small delay to be nice
                    await page.wait_for_timeout(1000)

                except Exception as e:
                    logger.error(f"      ❌ Error: {e}")

            # Save results
            with open(self.fresh_episodes_file, 'w', encoding='utf-8') as f:
                json.dump(fresh_episodes, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ Fresh episodes saved to {self.fresh_episodes_file}")

            await browser.close()

    def parse_episode_text(self, text):
        """Parse anime name and episode number (Robust version from airing.py)."""
        if not text: return "Unknown Anime", 1

        clean_text = re.sub(r'\s+', ' ', text).strip()
        clean_text = re.sub(r'^Watch\s+', '', clean_text)
        clean_text = re.sub(r'\s+Online\s*$', '', clean_text)

        patterns = [
            r'^(.+?)\s*-\s*[Ee]pisode\s*(\d+).*$',
            r'^(.+?)\s*-\s*[Ee][Pp]\s*(\d+).*$',
            r'^(.+?)\s+(\d+)\s*$',
        ]

        anime_name = "Unknown Anime"
        episode_number = 1

        for pattern in patterns:
            match = re.search(pattern, clean_text)
            if match and len(match.groups()) >= 2:
                anime_name = match.group(1).strip()
                try:
                    episode_number = int(match.group(2))
                except: pass
                break

        if anime_name == "Unknown Anime":
            # Fallback
            anime_name = clean_text

        return anime_name, episode_number

    async def get_iframe_for_episode(self, page, url):
        """Logic to extract iframe from episode page."""
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=45000)

            # DDoS Check
            if "Just a moment" in await page.title():
                logger.info("      🛡️ DDoS wait...")
                await page.wait_for_timeout(5000)

            # 1. Search directly (Standard)
            iframes = await page.query_selector_all('iframe')
            for frame in iframes:
                src = await frame.get_attribute('src')
                if src and any(x in src for x in ['kwik', 'embed', 'player']):
                    return self.make_absolute(src)

            # 2. Search in JS (Regex) - often found in script tags
            content = await page.content()
            matches = re.findall(r'(https?://[^\s"\']+(?:kwik|embed|player)[^\s"\']+)', content)
            if matches:
                return matches[0]

            # 3. Dynamic Player Container (e.g., #player iframe)
            player = await page.query_selector('#player iframe')
            if player:
                src = await player.get_attribute('src')
                return self.make_absolute(src)

            return None
        except Exception as e:
            logger.error(f"Error extracting iframe: {e}")
            return None

    def make_absolute(self, url):
        if url.startswith('//'): return 'https:' + url
        if url.startswith('/'): return self.base_url + url
        return url

async def main():
    builder = AnimePaheBuilder()
    builder.load_anime_index()
    builder.build_search_index()
    builder.update_popular_anime()
    await builder.update_fresh_episodes()

if __name__ == "__main__":
    asyncio.run(main())
