import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("google_reviews_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
import time
import pandas as pd
import argparse
import os
import json
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import random

try:
    import numpy
except ImportError as e:
    logger.error(f"Import error: {e}")
    raise

class GoogleReviewsScraper:
    def __init__(self, headless=True, timeout=30, max_retries=3, data_dir="scraped_data"):
        """Initialize the scraper with configurable options"""
        self.timeout = timeout
        self.max_retries = max_retries
        self.data_dir = data_dir

        # Create data directory if it doesn't exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Set up Chrome options
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--window-size=1920,1080")
        self.chrome_options.add_argument("--disable-notifications")
        self.chrome_options.add_argument("--disable-popup-blocking")

        # Add rotating user agents to mimic different browsers
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        ]
        self.chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")

        # Initialize driver
        self.driver = None

    def start_driver(self):
        """Start a new Chrome driver instance"""
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
            self.driver.set_page_load_timeout(self.timeout)
            logger.info("Chrome driver started successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to start Chrome driver: {e}")
            return False

    def quit_driver(self):
        """Safely quit the driver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("Chrome driver quit successfully")

    def get_place_url(self, query):
        """Get the Google Maps URL for a business using search"""
        try:
            search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
            self.driver.get(search_url)
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='maps/place']"))
            )
            # Get the first result
            place_link = self.driver.find_element(By.CSS_SELECTOR, "a[href*='maps/place']")
            place_url = place_link.get_attribute("href")
            logger.info(f"Found place URL: {place_url}")
            return place_url
        except Exception as e:
            logger.error(f"Failed to get place URL for '{query}': {e}")
            return None

    def navigate_to_reviews(self, url):
        """Navigate to the reviews section of a Google Maps place"""
        try:
            self.driver.get(url)
            # Wait for page to load
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.fontHeadlineSmall"))
            )

            # Find reviews section and click it
            for retry in range(self.max_retries):
                try:
                    # First approach: Look for elements containing 'reviews' and a number
                    reviews_elements = self.driver.find_elements(By.XPATH,
                                                                 "//button[contains(., 'reviews') or contains(., 'Reviews')]")
                    for element in reviews_elements:
                        if re.search(r'\d+\s+reviews', element.text, re.IGNORECASE):
                            element.click()
                            logger.info("Successfully clicked on reviews section")

                            # Wait for reviews to load
                            WebDriverWait(self.driver, self.timeout).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-review-id]"))
                            )
                            return True

                    # Second approach: Try clicking on the "All reviews" element
                    all_reviews = self.driver.find_elements(By.XPATH, "//button[contains(text(), 'All reviews')]")
                    if all_reviews:
                        all_reviews[0].click()
                        logger.info("Clicked on 'All reviews' section")

                        # Wait for reviews to load
                        WebDriverWait(self.driver, self.timeout).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-review-id]"))
                        )
                        return True

                    # Third approach: Look for review stars
                    star_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.ceHvDb, span.hqzQac")
                    for element in star_elements:
                        # Try to find a parent element to click
                        parent = element.find_element(By.XPATH, ".//ancestor::button")
                        parent.click()
                        logger.info("Clicked on star rating element to navigate to reviews")
                        time.sleep(2)

                        # Verify we have reviews loaded
                        if len(self.driver.find_elements(By.CSS_SELECTOR, "div[data-review-id]")) > 0:
                            return True

                except (StaleElementReferenceException, NoSuchElementException) as e:
                    logger.warning(f"Retry {retry + 1}/{self.max_retries}: {str(e)}")
                    time.sleep(2)

            # Final check: See if reviews are already visible without clicking
            if len(self.driver.find_elements(By.CSS_SELECTOR, "div[data-review-id]")) > 0:
                logger.info("Reviews already visible without clicking")
                return True

            logger.error("Failed to navigate to reviews section after multiple retries")
            return False

        except Exception as e:
            logger.error(f"Failed to navigate to reviews: {e}")
            return False

    def scroll_reviews(self, num_reviews=100, scroll_pause=1.5):
        """Scroll to load more reviews"""
        try:
            # Find the reviews container using multiple possible selectors
            scroll_container = None
            possible_selectors = [
                "//div[contains(@class, 'section-scrollbox')]",
                "//div[contains(@class, 'DxyBCb')]",
                "//div[contains(@class, 'review-dialog-list')]"
            ]

            for selector in possible_selectors:
                try:
                    containers = self.driver.find_elements(By.XPATH, selector)
                    if containers:
                        scroll_container = containers[0]
                        break
                except:
                    continue

            if not scroll_container:
                logger.warning("Could not find scroll container, trying body element instead")
                scroll_container = self.driver.find_element(By.TAG_NAME, "body")

            # Get initial reviews
            reviews_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[data-review-id]")
            initial_count = len(reviews_elements)
            logger.info(f"Initial review count: {initial_count}")

            # Implement scrolling to load more reviews
            current_count = initial_count
            max_scrolls = 50  # Safety limit
            scroll_count = 0
            stagnant_count = 0

            while current_count < num_reviews and scroll_count < max_scrolls and stagnant_count < 5:
                # Scroll down
                self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scroll_container)
                time.sleep(scroll_pause)

                # Expand "More" buttons if present to get full review text
                try:
                    more_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button.w8nwRe, button.LkLjZd")
                    for button in more_buttons[:10]:  # Limit to avoid too many expansions at once
                        try:
                            self.driver.execute_script("arguments[0].click();", button)
                        except:
                            pass
                except:
                    pass

                # Count reviews
                reviews_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[data-review-id]")
                new_count = len(reviews_elements)

                if new_count > current_count:
                    logger.info(f"Loaded {new_count} reviews so far...")
                    current_count = new_count
                    stagnant_count = 0
                else:
                    stagnant_count += 1

                scroll_count += 1

            if current_count >= num_reviews:
                logger.info(f"Successfully loaded requested {num_reviews} reviews")
            else:
                logger.info(f"Reached maximum available reviews: {current_count}")

            return current_count

        except Exception as e:
            logger.error(f"Error while scrolling reviews: {e}")
            return 0

    def extract_reviews(self):
        """Extract review data using Beautiful Soup"""
        try:
            # Get page source and parse with BeautifulSoup
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            # Find review elements
            review_data = []

            # Multiple selectors to handle different Google Maps layouts
            review_containers = soup.select("div[data-review-id], div.jftiEf")

            for container in review_containers:
                try:
                    # Extract review text
                    text_element = container.select_one(".MyEned, .wiI7pd")
                    review_text = text_element.get_text() if text_element else "No text"

                    # Extract star rating
                    rating_element = container.select_one("span[aria-label*='stars'], span.kvMYJc")
                    rating_text = rating_element.get('aria-label') if rating_element else "No rating"
                    # Convert "4 stars" to 4.0
                    rating = float(re.search(r'([\d.]+)', rating_text).group(1)) if rating_text != "No rating" else 0

                    # Extract reviewer name
                    name_element = container.select_one(".d4r55, .TSUbDb")
                    reviewer_name = name_element.get_text() if name_element else "Anonymous"

                    # Extract date
                    date_element = container.select_one(".rsqaWe, .dehysf")
                    review_date = date_element.get_text() if date_element else "No date"

                    # Extract if local guide
                    is_local_guide = bool(container.select_one("img[src*='localguide'], img.tEfPIe"))

                    # Extract review ID
                    review_id = container.get('data-review-id', 'unknown-id')

                    # Create review dictionary
                    review = {
                        'reviewer_name': reviewer_name,
                        'rating': rating,
                        'date': review_date,
                        'text': review_text,
                        'is_local_guide': is_local_guide,
                        'review_id': review_id
                    }

                    review_data.append(review)

                except Exception as e:
                    logger.warning(f"Error extracting a review: {e}")
                    continue

            # Get place information
            place_name_element = soup.select_one("h1.fontHeadlineLarge, h1.DUwDvf")
            place_name = place_name_element.get_text() if place_name_element else "Unknown Place"

            # Get overall rating
            overall_rating_element = soup.select_one("div.F7nice span[aria-hidden='true']")
            overall_rating = overall_rating_element.get_text() if overall_rating_element else "No overall rating"

            # Get total reviews count
            total_reviews_element = soup.select_one("div.F7nice div[aria-label*='reviews']")
            total_reviews_text = total_reviews_element.get('aria-label') if total_reviews_element else "0 reviews"
            total_reviews = int(
                re.search(r'(\d+)', total_reviews_text).group(1)) if "reviews" in total_reviews_text else 0

            # Get place metadata
            place_info = {
                'name': place_name,
                'overall_rating': overall_rating,
                'total_reviews': total_reviews,
                'scraped_reviews_count': len(review_data),
                'scrape_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            logger.info(f"Successfully extracted {len(review_data)} reviews for {place_name}")

            return place_info, review_data

        except Exception as e:
            logger.error(f"Error extracting reviews: {e}")
            return {"name": "Unknown", "error": str(e)}, []

    def save_reviews(self, place_info, reviews, business_name):
        """Save the scraped reviews to CSV and JSON"""
        try:
            # Create a safe filename
            safe_name = re.sub(r'[^a-zA-Z0-9]', '_', business_name).lower()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_prefix = f"{self.data_dir}/{safe_name}_{timestamp}"

            # Save to CSV
            df = pd.DataFrame(reviews)
            csv_path = f"{file_prefix}_reviews.csv"
            df.to_csv(csv_path, index=False)

            # Save to JSON
            json_data = {
                "place_info": place_info,
                "reviews": reviews
            }
            json_path = f"{file_prefix}_reviews.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=4)

            logger.info(f"Saved {len(reviews)} reviews to {csv_path} and {json_path}")
            return csv_path, json_path

        except Exception as e:
            logger.error(f"Error saving reviews: {e}")
            return None, None

    def scrape_business(self, business_name=None, business_url=None, num_reviews=100):
        """Main function to scrape reviews for a business"""
        if not business_name and not business_url:
            logger.error("Either business name or URL is required")
            return None, None

        try:
            # Start driver if not already running
            if not self.driver:
                if not self.start_driver():
                    return None, None

            # Get the business URL if only name provided
            url = business_url
            if not url and business_name:
                url = self.get_place_url(business_name)
                if not url:
                    logger.error(f"Could not find URL for business: {business_name}")
                    return None, None

            # Navigate to reviews section
            if not self.navigate_to_reviews(url):
                logger.error("Failed to navigate to reviews section")
                return None, None

            # Scroll to load desired number of reviews
            loaded_count = self.scroll_reviews(num_reviews=num_reviews)
            if loaded_count == 0:
                logger.error("Failed to load any reviews")
                return None, None

            # Extract reviews
            place_info, reviews = self.extract_reviews()

            # Use the actual place name from the extracted data if available
            actual_name = place_info.get('name', business_name or "Unknown Business")

            # Save data
            csv_path, json_path = self.save_reviews(place_info, reviews, actual_name)

            return place_info, reviews

        except Exception as e:
            logger.error(f"Error scraping business: {e}")
            return None, None

    # Don't close driver here to allow multiple businesses to be scraped

    def close(self):
        """Close the scraper and release resources"""
        self.quit_driver()

def show_menu():
    """Display interactive menu and get user choices"""
    print("\n=== Google Reviews Scraper ===")
    print("1. Scrape by Business Name")
    print("2. Scrape by URL")
    print("3. Exit")

    choice = input("\nEnter your choice (1-3): ").strip()

    if choice == "3":
        return None

    # Get common parameters
    reviews = input("Number of reviews to scrape (default: 100): ").strip()
    reviews = int(reviews) if reviews.isdigit() else 100

    headless = input("Run in headless mode? (y/N): ").strip().lower() == 'y'

    output = input("Output directory (default: scraped_data): ").strip()
    output = output if output else "scraped_data"

    if choice == "1":
        business = input("Enter business name: ").strip()
        if not business:
            print("Error: Business name is required")
            return None
        return {
            'business': business,
            'url': None,
            'reviews': reviews,
            'headless': headless,
            'output': output
        }

    elif choice == "2":
        url = input("Enter Google Maps URL: ").strip()
        if not url:
            print("Error: URL is required")
            return None
        return {
            'business': None,
            'url': url,
            'reviews': reviews,
            'headless': headless,
            'output': output
        }

    return None

def main():
    """Interactive menu interface for the scraper"""
    while True:
        options = show_menu()
        if not options:
            break

        scraper = GoogleReviewsScraper(headless=options['headless'], data_dir=options['output'])

        try:
            place_info, reviews = scraper.scrape_business(
                business_name=options['business'],
                business_url=options['url'],
                num_reviews=options['reviews']
            )

            if place_info and reviews:
                print(f"\nSuccessfully scraped {len(reviews)} reviews for {place_info['name']}")
                print(f"Overall rating: {place_info['overall_rating']}")
                print(f"Total reviews: {place_info['total_reviews']}")
                print(f"Data saved to {options['output']} directory")
            else:
                print("Failed to scrape reviews. Check the log file for details.")

        except KeyboardInterrupt:
            print("\nScraping interrupted by user")
        finally:
            scraper.close()

        again = input("\nWould you like to scrape another business? (y/N): ").strip().lower()
        if again != 'y':
            break

    print("\nThank you for using Google Reviews Scraper!")

if __name__ == "__main__":
    main()