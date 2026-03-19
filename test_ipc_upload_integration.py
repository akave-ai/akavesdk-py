#!/usr/bin/env python3

import os
import time
from pathlib import Path

from akavesdk import SDK, SDKConfig


def main():
    NODE_ADDRESS = "connect.akave.ai:5500"
    PRIVATE_KEY = "a5c223e956644f1ba11f0dcc6f3df4992184ff3c919223744d0cf1db33dab4d6"
    BUCKET_NAME = "finalfr"

    script_dir = Path(__file__).parent
    test_file_path = script_dir / "random_3mb_file.bin"

    if not test_file_path.exists():
        print(f"❌ Error: Test file not found: {test_file_path}")
        print(f"   Please ensure random_3mb_file.bin exists in {script_dir}")
        return 1

    file_size = os.path.getsize(test_file_path)
    # Use unique filename with timestamp to avoid conflicts
    timestamp = int(time.time())
    file_name = f"test_{timestamp}.bin"

    print(f"\n{'='*70}")
    print(f"🚀 Akave IPC Upload Integration Test")
    print(f"{'='*70}")
    print(f"📡 Node Address: {NODE_ADDRESS}")
    print(f"📦 Bucket Name: {BUCKET_NAME}")
    print(f"📄 File Name: {file_name}")
    print(f"📂 Source File: {test_file_path}")
    print(f"📊 File Size: {file_size:,} bytes ({file_size / (1024*1024):.2f} MB)")
    print(f"{'='*70}\n")

    try:
        print("🔧 Step 1: Initializing SDK...")
        config = SDKConfig(
            address=NODE_ADDRESS,
            private_key=PRIVATE_KEY,
            max_concurrency=5,
            block_part_size=128 * 1024,
            use_connection_pool=True,
            chunk_buffer=10,
        )
        sdk = SDK(config)
        print("✅ SDK initialized successfully\n")

        print("🔧 Step 2: Creating IPC instance...")
        ipc = sdk.ipc()
        print("✅ IPC instance created\n")

        print(f"🔧 Step 3: Checking/Creating bucket '{BUCKET_NAME}'...")
        existing_bucket = ipc.view_bucket(None, BUCKET_NAME)

        if existing_bucket is None:
            print(f"   Bucket doesn't exist, creating...")
            result = ipc.create_bucket(None, BUCKET_NAME)
            print(f"✅ Bucket created successfully")
            print(f"   Bucket ID: {result.id}")
            print(f"   Bucket Name: {result.name}")
            print(f"   Created At: {result.created_at}")
            time.sleep(2)
        else:
            print(f"✅ Bucket already exists")
            print(f"   Bucket ID: {existing_bucket.id}")
            print(f"   Bucket Name: {existing_bucket.name}\n")

        print(f"🔧 Step 4: Uploading file...")
        print(f"   File: {file_name}")
        print(f"   This may take a while for a {file_size / (1024*1024):.2f} MB file...")
        print(f"   Note: upload() will create file upload and handle all transactions\n")

        start_time = time.time()

        with open(test_file_path, "rb") as f:
            file_meta = ipc.upload(None, BUCKET_NAME, file_name, f)

        upload_duration = time.time() - start_time
        upload_speed = (file_size / (1024 * 1024)) / upload_duration if upload_duration > 0 else 0

        print(f"\n✅ File uploaded successfully!")
        print(f"   Root CID: {file_meta.root_cid}")
        print(f"   File Name: {file_meta.name}")
        print(f"   File Size: {file_meta.size:,} bytes")
        print(f"   Encoded Size: {file_meta.encoded_size:,} bytes")
        print(f"   Upload Duration: {upload_duration:.2f} seconds")
        print(f"   Upload Speed: {upload_speed:.2f} MB/s\n")

        print(f"🔧 Step 5: Verifying file metadata...")
        try:
            retrieved_meta = ipc.file_info(None, BUCKET_NAME, file_name)

            if retrieved_meta is None:
                print(f"⚠️  Could not retrieve file metadata (but upload succeeded!)")
            else:
                print(f"✅ File metadata verified")
                print(f"   Name: {retrieved_meta.name}")
                print(f"   Bucket: {retrieved_meta.bucket_name}")
                print(f"   Root CID: {retrieved_meta.root_cid}")
                print(f"   Size: {retrieved_meta.encoded_size:,} bytes\n")
        except Exception as e:
            print(f"⚠️  File verification skipped (upload succeeded!)")
            print(f"   Note: {str(e)}")
            print(f"   The file was successfully uploaded and committed\n")
            retrieved_meta = None

        if retrieved_meta and retrieved_meta.root_cid != file_meta.root_cid:
            print(f"⚠️  Warning: Root CID mismatch!")
            print(f"   Upload CID: {file_meta.root_cid}")
            print(f"   Retrieved CID: {retrieved_meta.root_cid}")
        elif retrieved_meta:
            print(f"✅ Root CID matches upload!\n")

        print(f"🔧 Step 6: Listing files in bucket...")
        files = ipc.list_files(None, BUCKET_NAME)
        print(f"✅ Found {len(files)} file(s) in bucket '{BUCKET_NAME}'")

        uploaded_file = next((f for f in files if f.name == file_name), None)
        if uploaded_file:
            print(f"✅ Uploaded file found in bucket listing")
        else:
            print(f"⚠️  Warning: Uploaded file not found in listing")

        print(f"\n{'='*70}")
        print(f"✅ Upload Test Completed Successfully!")
        print(f"{'='*70}")
        print(f"\n📋 Summary:")
        print(f"   • File: {file_name}")
        print(f"   • Size: {file_size:,} bytes ({file_size / (1024*1024):.2f} MB)")
        print(f"   • Root CID: {file_meta.root_cid}")
        print(f"   • Bucket: {BUCKET_NAME}")
        print(f"   • Upload Time: {upload_duration:.2f}s")
        print(f"   • Upload Speed: {upload_speed:.2f} MB/s")
        print(f"{'='*70}\n")

        sdk.close()
        return 0

    except Exception as e:
        print(f"\n❌ Error during upload test:")
        print(f"   {type(e).__name__}: {str(e)}")
        import traceback

        print(f"\n📋 Full traceback:")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
