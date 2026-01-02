
import asyncio
from playwright.async_api import async_playwright
import json
import os
import time
import logging
from collections import defaultdict
import aiofiles
import random
from datetime import datetime

class MultiInstanceAnimeUpdater:
    def __init__(self, num_instances=3):
        self.base_url = "https://animepahe.si"
        self.anime_index_folder = "anime_index"
        self.cache_folder = os.path.join(self.anime_index_folder, "episode_cache")
        self.coordination_file = os.path.join(self.anime_index_folder, "coordination.json")
        self.num_instances = num_instances

        os.makedirs(self.cache_folder, exist_ok=True)

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [Instance %(name)s] - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(self.anime_index_folder, 'multi_instance_update.log')),
                logging.StreamHandler()
            ]
        )

        # Initialize coordination system
        self.init_coordination()

    def init_coordination(self):
        """Initialize the coordination system"""
        if not os.path.exists(self.coordination_file):
            coordination_data = {
                "locked_files": {},
                "processed_anime": [],
                "failed_anime": [],
                "instance_status": {},
                "start_time": time.time(),
                "total_processed": 0
            }
            with open(self.coordination_file, 'w') as f:
                json.dump(coordination_data, f, indent=2)

    def get_coordination_data(self):
        """Get current coordination data"""
        with open(self.coordination_file, 'r') as f:
            return json.load(f)

    def update_coordination_data(self, updates):
        """Update coordination data atomically"""
        import fcntl  # For file locking

        with open(self.coordination_file, 'r+') as f:
            # Try to get exclusive lock
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                data = json.load(f)
                data.update(updates)
                f.seek(0)
                json.dump(data, f, indent=2)
                f.truncate()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def lock_file(self, instance_id, filename):
        """Lock a file for a specific instance"""
        updates = {
            "locked_files": {**self.get_coordination_data()["locked_files"], filename: instance_id},
            "instance_status": {**self.get_coordination_data()["instance_status"], instance_id: f"Processing {filename}"}
        }
        self.update_coordination_data(updates)

    def unlock_file(self, filename):
        """Unlock a file"""
        data = self.get_coordination_data()
        locked_files = data["locked_files"].copy()
        if filename in locked_files:
            del locked_files[filename]
        self.update_coordination_data({"locked_files": locked_files})

    def mark_anime_processed(self, anime_id, instance_id):
        """Mark an anime as processed"""
        data = self.get_coordination_data()
        processed = data["processed_anime"].copy()
        if anime_id not in processed:
            processed.append(anime_id)

        updates = {
            "processed_anime": processed,
            "total_processed": data["total_processed"] + 1,
            "instance_status": {**data["instance_status"], instance_id: f"Processed {data['total_processed'] + 1} anime"}
        }
        self.update_coordination_data(updates)

    def is_file_locked(self, filename):
        """Check if a file is locked by another instance"""
        data = self.get_coordination_data()
        return filename in data["locked_files"]

    def is_anime_processed(self, anime_id):
        """Check if an anime has been processed"""
        data = self.get_coordination_data()
        return anime_id in data["processed_anime"]

    def get_available_work(self):
        """Get available JSON files that aren't locked"""
        available_files = []
        data = self.get_coordination_data()
        locked_files = set(data["locked_files"].keys())
        processed_anime = set(data["processed_anime"])

        for filename in os.listdir(self.anime_index_folder):
            if filename.startswith("anime_") and filename.endswith(".json") and "master_index" not in filename:
                if filename not in locked_files:
                    # Check if file has unprocessed anime
                    filepath = os.path.join(self.anime_index_folder, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            file_data = json.load(f)

                        # Count unprocessed anime in this file
                        unprocessed_count = 0
                        for anime in file_data.get('anime', []):
                            if anime.get('id') not in processed_anime:
                                unprocessed_count += 1

                        if unprocessed_count > 0:
                            available_files.append((filename, unprocessed_count))
                    except:
                        continue

        # Sort by most unprocessed work first
        available_files.sort(key=lambda x: x[1], reverse=True)
        return [file[0] for file in available_files]

    async def setup_browser(self, instance_id):
        """Setup browser for an instance"""
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled'
            ]
        )

        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )

        page = await context.new_page()

        return playwright, browser, context, page

    async def close_browser(self, playwright, browser):
        """Close browser for an instance"""
        await browser.close()
        await playwright.stop()

    async def smart_wait(self, min_delay=2, max_delay=6):
        """Smart waiting with random delays"""
        delay = random.uniform(min_delay, max_delay)
        await asyncio.sleep(delay)

    async def safe_navigate(self, page, url, max_retries=3):
        """Safe navigation with retry logic"""
        for attempt in range(max_retries):
            try:
                await page.goto(url, wait_until='networkidle', timeout=30000)
                await asyncio.sleep(2)
                return True
            except Exception as e:
                logging.getLogger().warning(f"Navigation attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    continue
                else:
                    logging.getLogger().error(f"Failed to navigate to {url} after {max_retries} attempts")
                    return False

    async def extract_episodes_simple(self, page, anime_url, anime_id):
        """Simple episode extraction"""
        cache_file = os.path.join(self.cache_folder, f"{anime_id}_episodes.json")

        # Check cache first
        if os.path.exists(cache_file):
            try:
                async with aiofiles.open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.loads(await f.read())
                    logging.getLogger().debug(f"Using cached episodes for {anime_id}")
                    return cached_data
            except:
                pass

        if not await self.safe_navigate(page, anime_url):
            return []

        await asyncio.sleep(3)

        episodes = await page.evaluate("""
            () => {
                const episodes = [];
                const playLinks = document.querySelectorAll('a[href*="/play/"]');

                playLinks.forEach(link => {
                    const href = link.href || '';
                    const text = link.textContent?.trim() || '';

                    let episodeNum = '0';
                    const numMatch = text.match(/(\\d+)/);
                    if (numMatch) {
                        episodeNum = numMatch[1];
                    }

                    episodes.push({
                        number: episodeNum,
                        title: text,
                        url: href,
                        episode_id: href.split('/').pop() || ''
                    });
                });

                return episodes;
            }
        """)

        # Remove duplicates and sort
        unique_episodes = []
        seen_urls = set()

        for episode in episodes:
            if episode['url'] not in seen_urls:
                seen_urls.add(episode['url'])
                unique_episodes.append(episode)

        unique_episodes.sort(key=lambda x: int(x['number']) if x['number'].isdigit() else 0)

        # Cache the results
        try:
            async with aiofiles.open(cache_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(unique_episodes, indent=2, ensure_ascii=False))
        except:
            pass

        return unique_episodes

    async def process_file(self, instance_id, filename):
        """Process a single JSON file"""
        logger = logging.getLogger(instance_id)
        filepath = os.path.join(self.anime_index_folder, filename)

        logger.info(f"🔒 Locking file: {filename}")
        self.lock_file(instance_id, filename)

        try:
            # Load the file
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            letter = data.get('letter', 'Unknown')
            total_anime = len(data.get('anime', []))
            processed_in_file = 0

            logger.info(f"📖 Processing {filename} (Letter: {letter}, Anime: {total_anime})")

            # Setup browser for this file
            playwright, browser, context, page = await self.setup_browser(instance_id)

            try:
                for i, anime in enumerate(data['anime']):
                    anime_id = anime['id']

                    # Skip if already processed by any instance
                    if self.is_anime_processed(anime_id):
                        continue

                    logger.info(f"🎬 [{i+1}/{total_anime}] Processing: {anime['title']}")

                    try:
                        # Extract episodes
                        episodes = await self.extract_episodes_simple(page, anime['url'], anime_id)

                        # Update anime data
                        anime['episodes'] = episodes
                        anime['episodes_count'] = len(episodes)

                        # Mark as processed
                        self.mark_anime_processed(anime_id, instance_id)
                        processed_in_file += 1

                        logger.info(f"✅ {anime['title']} - {len(episodes)} episodes")

                        # Save file every 3 anime
                        if processed_in_file % 3 == 0:
                            with open(filepath, 'w', encoding='utf-8') as f:
                                json.dump(data, f, indent=2, ensure_ascii=False)
                            logger.info(f"💾 Saved {processed_in_file} updates to {filename}")

                        # Delay between anime
                        await self.smart_wait(2, 5)

                    except Exception as e:
                        logger.error(f"❌ Failed to process {anime['title']}: {str(e)}")
                        # Mark as failed but continue
                        data = self.get_coordination_data()
                        failed = data["failed_anime"].copy()
                        failed.append({
                            'id': anime_id,
                            'title': anime['title'],
                            'file': filename,
                            'error': str(e),
                            'instance': instance_id,
                            'timestamp': time.time()
                        })
                        self.update_coordination_data({"failed_anime": failed})
                        continue

                # Final save
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                logger.info(f"🎉 Completed {filename}: {processed_in_file} anime updated")

            finally:
                await self.close_browser(playwright, browser)

        except Exception as e:
            logger.error(f"💥 Error processing file {filename}: {str(e)}")
        finally:
            logger.info(f"🔓 Unlocking file: {filename}")
            self.unlock_file(filename)

        return processed_in_file

    async def worker_instance(self, instance_id):
        """Worker instance that processes files"""
        logger = logging.getLogger(instance_id)

        # Staggered start - each instance starts at different times
        start_delay = (int(instance_id.split('_')[1]) - 1) * 10  # 10 seconds between instances
        logger.info(f"⏰ Instance starting in {start_delay} seconds...")
        await asyncio.sleep(start_delay)

        logger.info(f"🚀 Instance {instance_id} started!")

        total_processed = 0

        while True:
            # Get available work
            available_files = self.get_available_work()

            if not available_files:
                logger.info("✅ No more work available. Waiting...")
                await asyncio.sleep(30)  # Wait 30 seconds and check again

                # Double check if still no work
                available_files = self.get_available_work()
                if not available_files:
                    logger.info("🎉 All work completed! Shutting down.")
                    break

            # Take the first available file
            filename = available_files[0]

            # Process the file
            processed = await self.process_file(instance_id, filename)
            total_processed += processed

            # Brief pause before getting next file
            await asyncio.sleep(5)

        logger.info(f"🏁 Instance {instance_id} finished! Total processed: {total_processed}")
        return total_processed

    async def run_multi_instance(self):
        """Run multiple instances concurrently"""
        logger = logging.getLogger("MASTER")
        logger.info("🚀 Starting multi-instance anime updater!")
        logger.info(f"🖥️  Launching {self.num_instances} instances")

        # Initialize instance status
        instance_status = {}
        for i in range(self.num_instances):
            instance_status[f"instance_{i+1}"] = "Starting up"
        self.update_coordination_data({"instance_status": instance_status})

        # Start all instances
        tasks = []
        for i in range(self.num_instances):
            instance_id = f"instance_{i+1}"
            task = asyncio.create_task(self.worker_instance(instance_id))
            tasks.append(task)

        # Monitor progress
        await self.monitor_progress(tasks)

        # Wait for all instances to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_processed = sum(r for r in results if isinstance(r, int))
        logger.info(f"🎉 ALL INSTANCES COMPLETED! Total anime processed: {total_processed}")

        # Generate final report
        await self.generate_final_report()

    async def monitor_progress(self, tasks):
        """Monitor and log progress periodically"""
        import asyncio

        async def monitor():
            while any(not task.done() for task in tasks):
                await asyncio.sleep(60)  # Update every minute
                data = self.get_coordination_data()

                print(f"\n📊 PROGRESS UPDATE [{datetime.now().strftime('%H:%M:%S')}]")
                print(f"   Total Processed: {data['total_processed']}")
                print(f"   Failed: {len(data['failed_anime'])}")
                print(f"   Locked Files: {len(data['locked_files'])}")
                print("   Instance Status:")
                for instance, status in data['instance_status'].items():
                    print(f"     {instance}: {status}")
                print()

        # Run monitor in background
        asyncio.create_task(monitor())

    async def generate_final_report(self):
        """Generate final report"""
        data = self.get_coordination_data()
        total_time = time.time() - data['start_time']

        report = {
            "summary": {
                "total_processed": data["total_processed"],
                "failed_anime": len(data["failed_anime"]),
                "total_time_seconds": total_time,
                "completion_time": datetime.now().isoformat(),
                "instances_used": self.num_instances
            },
            "failed_anime": data["failed_anime"],
            "performance": {
                "anime_per_hour": (data["total_processed"] / total_time) * 3600,
                "average_time_per_anime": total_time / data["total_processed"] if data["total_processed"] > 0 else 0
            }
        }

        report_file = os.path.join(self.anime_index_folder, "multi_instance_final_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger = logging.getLogger("MASTER")
        logger.info(f"📊 Final report saved: {report_file}")

async def main():
    # You can adjust the number of instances based on your RAM
    # Start with 3, you can increase to 5 if RAM allows
    num_instances = 3

    updater = MultiInstanceAnimeUpdater(num_instances=num_instances)

    try:
        await updater.run_multi_instance()
    except KeyboardInterrupt:
        logger = logging.getLogger("MASTER")
        logger.info("⏸️ Multi-instance scraping interrupted by user.")
    except Exception as e:
        logger = logging.getLogger("MASTER")
        logger.error(f"💥 Master error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
