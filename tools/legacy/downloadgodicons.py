import os
import requests
import json

OUTPUT_DIR = "assets/gods"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load your existing god list from metadata
with open("data/gods_metadata.json", "r") as f:
    gods_data = json.load(f)

god_names = [god["God"] for god in gods_data]
print(f"Found {len(god_names)} gods to download")

def download_god_icons():
    success = 0
    failed = 0
    
    for god in god_names:
        safe_name = god.replace(' ', '')  # Remove spaces for URL
        downloaded = False
        
        # Try multiple filename patterns (some gods have S1/S2 suffixes)
        patterns = [
            f"T_{safe_name}_Default.png",
            f"T_{safe_name}S2_Default.png",
            f"T_{safe_name}S1_Default.png",
            f"T_{safe_name.replace('_', '')}_Default.png",
            f"SkinArt_{safe_name}_Default.jpg",
            f"SkinArt_{safe_name}S2_Default.jpg",
            f"SkinArt_{safe_name}S1_Default.jpg",
        ]
        
        for filename in patterns:
            url = f"https://wiki.smite2.com/images/{filename}"
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200 and len(resp.content) > 1000:
                    with open(os.path.join(OUTPUT_DIR, f"{safe_name}.png"), "wb") as f:
                        f.write(resp.content)
                    print(f"✓ {god} ({filename})")
                    success += 1
                    downloaded = True
                    break
            except:
                continue
        
        if not downloaded:
            print(f"✗ {god}")
            failed += 1
    
    print(f"\nDone! {success} downloaded, {failed} failed")

if __name__ == "__main__":
    download_god_icons()