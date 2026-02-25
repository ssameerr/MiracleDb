import requests
import time
import json

def generate_query(tables):
    query = "SELECT count(*) FROM bench_t0"
    for i in range(1, tables):
        query += f" JOIN bench_t{i} ON bench_t0.id = bench_t{i}.id"
    return query

import hmac
import hashlib
import base64
import json
import time

def get_valid_token():
    # Header
    header = {"alg": "HS256", "typ": "JWT"}
    header_bytes = json.dumps(header).encode('utf-8')
    header_b64 = base64.urlsafe_b64encode(header_bytes).decode('utf-8').rstrip('=')
    
    # Payload
    payload = {
        "sub": "admin",
        "role": "admin",
        "roles": ["admin"],  # Just in case
        "exp": int(time.time()) + 3600
    }
    payload_bytes = json.dumps(payload).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
    
    # Signature
    secret = "default_secret_do_not_use_in_prod".encode('utf-8')
    msg = f"{header_b64}.{payload_b64}".encode('utf-8')
    signature = hmac.new(secret, msg, hashlib.sha256).digest()
    signature_b64 = base64.urlsafe_b64encode(signature).decode('utf-8').rstrip('=')
    
    return f"{header_b64}.{payload_b64}.{signature_b64}"

def run_benchmark():
    url = "http://localhost:8080/api/v1/query"
    sql = generate_query(50)
    payload = {"sql": sql}
    
    token = get_valid_token()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    # Register tables first using the benchmark endpoint (it has resume logic)
    print("Registering tables (this might take a few seconds)...")
    reg_url = "http://localhost:8080/api/v1/debug/benchmark"
    reg_payload = {"tables": 50, "rows": 1000000}
    try:
        # This endpoint is public as per middleware.rs
        reg_response = requests.post(reg_url, json=reg_payload)
        reg_response.raise_for_status()
        print("Registration complete.")
    except Exception as e:
         print(f"Registration failed: {e}")
         if hasattr(e, 'response') and e.response is not None:
             print(f"Response Body: {e.response.text}")
         return

    times = []
    
    print(f"Executing 50-way JOIN query 10 times...")
    print("-" * 50)
    
    for i in range(1, 11):
        start = time.time()
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            duration = (time.time() - start) * 1000 # ms
            times.append(duration)
            print(f"Run {i}: {duration:.2f} ms")
        except Exception as e:
            print(f"Run {i}: FAILED - {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response Body: {e.response.text}")

    if times:
        avg = sum(times) / len(times)
        print("-" * 50)
        print(f"Average execution time: {avg:.2f} ms")
        print(f"Min: {min(times):.2f} ms")
        print(f"Max: {max(times):.2f} ms")
    else:
        print("No successful runs.")

if __name__ == "__main__":
    run_benchmark()
