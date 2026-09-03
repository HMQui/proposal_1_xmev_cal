import json
import hashlib
from pathlib import Path

def freeze_config(file_path):
    # Read the configuration file
    with open(file_path, 'rb') as f:
        file_content = f.read()
    
    # Calculate the SHA-256 hash
    hash_sha256 = hashlib.sha256(file_content).hexdigest()
    
    print(f"--- PROTOCOL FROZEN ---")
    print(f"File: {file_path}")
    print(f"SHA-256 Checksum: {hash_sha256}")
    
    # Save the hash to a accompanying file as proof
    checksum_path = Path(file_path).with_suffix('.sha256')
    with open(checksum_path, 'w') as f:
        f.write(hash_sha256)
    print(f"Checksum saved to: {checksum_path}")

if __name__ == "__main__":
    config_file = "artifact/manifests/mvp_config.json"
    freeze_config(config_file)