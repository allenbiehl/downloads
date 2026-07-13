import json
import random

def generate_standard_json_file(filename="mock_geo_data.json", target_mb=1):
    # Convert target size to exact bytes
    target_bytes = target_mb * 1024 * 1024
    
    dates = ["2026/01/01", "2026/01/02", "2026/01/03"]
    
    def create_record(idx):
        return {
            "event_id": f"evt_{idx:08d}",
            "event_time": random.choice(dates) + f" {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            "status": random.choice(["success", "fail", "pending"]),
            
            # --- 10 GEO TYPE FIELDS ---
            "lat": round(random.uniform(-90.0, 90.0), 6),
            "lon": round(random.uniform(-180.0, 180.0), 6),
            "alt_meters": round(random.uniform(0.0, 8000.0), 2),
            "speed_kmh": round(random.uniform(0.0, 120.0), 2),
            "heading_degrees": random.randint(0, 359),
            "horizontal_accuracy": round(random.uniform(1.0, 50.0), 1),
            "vertical_accuracy": round(random.uniform(1.0, 50.0), 1),
            "satellite_count": random.randint(4, 12),
            "hdop": round(random.uniform(0.5, 5.0), 2),
            "geohash": "".join(random.choices("0123456789bcdefghjkmnpqrstuvwxyz", k=9))
        }

    print(f"Generating standard JSON array file to target {target_mb}MB...")
    
    current_bytes = 0
    record_index = 0
    
    with open(filename, "wb") as f:
        # 1. Write the opening bracket of the JSON array
        current_bytes += 2
        
        # We loop until we are close to the target, leaving room for the closing bracket "]"
        while current_bytes < (target_bytes - 500):
            record = create_record(record_index)
            
            # 2. Format with indentation or clean spacing for predictability
            line_str = json.dumps(record) + "\n"
            
            line_bytes = line_str.encode("utf-8")
            f.write(line_bytes)
            current_bytes += len(line_bytes)
            record_index += 1
            
        current_bytes += 2
            
    print(f"File '{filename}' generated cleanly.")
    print(f"Total Array Records Written: {record_index}")
    print(f"Verified Final File Size: {current_bytes / (1024 * 1024):.2f} MB")

if __name__ == "__main__":
    generate_standard_json_file()
