import time
import requests

def safe_request(url, params=None, timeout=5, return_json=True, max_retries=3):
    start = time.time()
    errors = 0
    payload = None

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response_time_ms = int((time.time() - start) * 1000)

            if response.status_code != 200:
                errors += 1
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
            else:
                if return_json:
                    payload = response.json()
                else:
                    payload = {"html": response.text}
                break

        except Exception:
            errors += 1
            response_time_ms = int((time.time() - start) * 1000)
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            else:
                break
    
    if payload is None:
        response_time_ms = int((time.time() - start) * 1000)

    return payload, response_time_ms, errors