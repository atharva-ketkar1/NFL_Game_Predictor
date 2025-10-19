import threading
import sys
import time

# --- 1. Import the refactored main functions from your scrapers ---
try:
    from get_fanduel_props import run_scraper as run_fanduel
    from get_draftkings_props import run_scraper as run_draftkings
except ImportError as e:
    print(f"Error: Could not import scraper functions: {e}")
    print("Please ensure 'get_fanduel_props.py' and 'get_draftkings_props.py' are in the same directory.")
    print("Also, make sure you have applied the modifications from Step 2 and 3.")
    sys.exit(1)

def main():
    # --- 2. Get the week number ONCE ---
    try:
        week_number_str = input("Enter the current NFL week number (e.g., 7): ")
        week_number = int(week_number_str)
    except ValueError:
        print("Invalid week number. Please enter a whole number.")
        return

    print(f"\n--- Starting All Scrapers for Week {week_number} ---")
    start_time = time.time()

    # --- 3. Create wrapper functions for threading ---
    # This helps us print messages when each thread is done
    def fanduel_wrapper():
        print("[Thread 1] ... Starting FanDuel Scraper ...")
        try:
            run_fanduel(week_number)
            print("[Thread 1] ✅ FanDuel Scraper Finished.")
        except Exception as e:
            print(f"[Thread 1] ❌ FanDuel Scraper FAILED: {e}")

    def draftkings_wrapper():
        print("[Thread 2] ... Starting DraftKings Scraper ...")
        try:
            run_draftkings(week_number)
            print("[Thread 2] ✅ DraftKings Scraper Finished.")
        except Exception as e:
            print(f"[Thread 2] ❌ DraftKings Scraper FAILED: {e}")

    # --- 4. Create, start, and join the threads ---
    fanduel_thread = threading.Thread(target=fanduel_wrapper)
    draftkings_thread = threading.Thread(target=draftkings_wrapper)

    # Start both threads
    fanduel_thread.start()
    draftkings_thread.start()

    # Wait for both threads to complete
    fanduel_thread.join()
    draftkings_thread.join()

    end_time = time.time()
    print(f"\n--- All Scraping Complete in {end_time - start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()