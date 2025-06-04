#!/usr/bin/env python3
"""
Comprehensive test script for SDK IPC bucket operations
Tests: create_bucket and delete_bucket with robust error handling
"""
import os
import time
import logging
import sys
import traceback
import uuid
from web3 import Web3
from eth_account import Account
from akavesdk import SDK
from akavesdk import SDKError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bucket_operations_test.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("bucket_operations_test")

class BucketTestRunner:
    def __init__(self):
        self.test_results = {}
        self.sdk = None
        self.ipc = None
        
        # Configuration
        self.eth_uri = os.getenv('ETH_RPC', 'https://n1-us.akave.ai/ext/bc/2JMWNmZbYvWcJRPPy1siaDBZaDGTDAaqXoY5UBKh4YrhNFzEce/rpc')
        self.grpc_address = os.getenv('GRPC_ADDRESS', 'yucca.akave.ai:5500')
        self.storage_address = os.getenv('STORAGE_ADDRESS', '0x9aa8ff1604280d66577ecb5051a3833a983ca3af')
        self.private_key = os.getenv('PRIVATE_KEY', '0xa5c223e956644f1ba11f0dcc6f3df4992184ff3c919223744d0cf1db33dab4d6')
        
        # Test buckets
        self.test_bucket_name = f"test-bucket-{uuid.uuid4().hex[:8]}"
        self.existing_bucket_name = "test-bucket-sdk-5dbb2fd7"  # Known existing bucket
        
    def setup_sdk(self):
        """Initialize SDK and IPC client"""
        try:
            logger.info("🚀 Setting up SDK and IPC client...")
            print("🚀 Setting up SDK and IPC client...")
            
            # Set environment variables (will be replaced by dynamic fetching in SDK)
            os.environ['ETHEREUM_NODE_URL'] = self.eth_uri
            os.environ['STORAGE_CONTRACT_ADDRESS'] = self.storage_address
            
            # Initialize Web3 and account
            web3 = Web3(Web3.HTTPProvider(self.eth_uri))
            account = Account.from_key(self.private_key)
            
            logger.info(f"Using account address: {account.address}")
            print(f"📋 Using account address: {account.address}")
            
            # Initialize SDK with dynamic contract fetching
            self.sdk = SDK(
                address=self.grpc_address,
                max_concurrency=10,
                block_part_size=1000000,
                use_connection_pool=True,
                private_key=self.private_key
            )
            
            # Get IPC client
            self.ipc = self.sdk.ipc()
            
            logger.info("✅ SDK and IPC client initialized successfully")
            print("✅ SDK and IPC client initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup SDK: {e}")
            print(f"❌ Failed to setup SDK: {e}")
            return False
    
    def run_test(self, test_name, test_func, *args, **kwargs):
        """Run a single test with error handling"""
        try:
            logger.info(f"🔄 Running test: {test_name}")
            print(f"\n🔄 Running test: {test_name}")
            
            result = test_func(*args, **kwargs)
            
            if result:
                self.test_results[test_name] = "PASSED"
                logger.info(f"✅ {test_name}: PASSED")
                print(f"✅ {test_name}: PASSED")
            else:
                self.test_results[test_name] = "FAILED"
                logger.error(f"❌ {test_name}: FAILED")
                print(f"❌ {test_name}: FAILED")
                
            return result
            
        except Exception as e:
            self.test_results[test_name] = f"ERROR: {str(e)}"
            logger.error(f"❌ {test_name}: ERROR - {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            print(f"❌ {test_name}: ERROR - {e}")
            return False
    
    def test_create_bucket_success(self):
        """Test successful bucket creation"""
        try:
            logger.info(f"Creating bucket: {self.test_bucket_name}")
            print(f"📦 Creating bucket: {self.test_bucket_name}")
            
            # Step 1: Create the bucket
            result = self.ipc.create_bucket(None, self.test_bucket_name)
            
            if not result:
                logger.error("Create bucket returned None/False")
                print("❌ Create bucket returned None/False")
                return False
            
            logger.info(f"Bucket created successfully: {result}")
            print(f"✅ Bucket created successfully")
            
            # Step 2: Wait for blockchain confirmation
            print("⏳ Waiting 5 seconds for blockchain confirmation...")
            time.sleep(5)
            
            # Step 3: Verify the bucket exists
            try:
                view_result = self.ipc.view_bucket(None, self.test_bucket_name)
                if view_result:
                    logger.info(f"Bucket verification successful: {view_result.name}")
                    print(f"✅ Bucket verification successful: {view_result.name}")
                    return True
                else:
                    logger.error("Bucket not found after creation")
                    print("❌ Bucket not found after creation")
                    return False
            except Exception as e:
                logger.error(f"Failed to verify bucket creation: {e}")
                print(f"❌ Failed to verify bucket creation: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Create bucket failed: {e}")
            print(f"❌ Create bucket failed: {e}")
            return False
    
    def test_list_buckets(self):
        """Test listing buckets to verify our test bucket appears"""
        try:
            logger.info("Testing list buckets functionality")
            print("📋 Testing list buckets functionality")
            
            buckets = self.ipc.list_buckets(None)
            logger.info(f"Found {len(buckets)} total buckets")
            print(f"✅ Found {len(buckets)} total buckets")
            
            # Check if our test bucket is in the list
            bucket_found = False
            for bucket in buckets:
                if bucket.name == self.test_bucket_name:
                    bucket_found = True
                    logger.info(f"Test bucket found in list: {bucket.name}")
                    print(f"✅ Test bucket found in list: {bucket.name}")
                    break
            
            if bucket_found:
                return True
            else:
                logger.warning(f"Test bucket '{self.test_bucket_name}' not found in list")
                print(f"⚠️ Test bucket '{self.test_bucket_name}' not found in list")
                return False
                
        except Exception as e:
            logger.error(f"List buckets failed: {e}")
            print(f"❌ List buckets failed: {e}")
            return False
    
    def test_view_bucket(self):
        """Test viewing the created bucket"""
        try:
            logger.info(f"Testing view bucket: {self.test_bucket_name}")
            print(f"👀 Testing view bucket: {self.test_bucket_name}")
            
            view_result = self.ipc.view_bucket(None, self.test_bucket_name)
            
            if view_result:
                logger.info(f"Bucket details retrieved:")
                logger.info(f"  Name: {view_result.name}")
                logger.info(f"  ID: {view_result.id}")
                logger.info(f"  Created at: {view_result.created_at}")
                
                print(f"✅ Bucket details retrieved:")
                print(f"  📦 Name: {view_result.name}")
                print(f"  🆔 ID: {view_result.id}")
                print(f"  📅 Created at: {view_result.created_at}")
                
                return True
            else:
                logger.error("View bucket returned None")
                print("❌ View bucket returned None")
                return False
                
        except Exception as e:
            logger.error(f"View bucket failed: {e}")
            print(f"❌ View bucket failed: {e}")
            return False
    
    def test_delete_bucket_success(self):
        """Test successful bucket deletion"""
        try:
            logger.info(f"Testing delete bucket: {self.test_bucket_name}")
            print(f"🗑️ Testing delete bucket: {self.test_bucket_name}")
            
            # Step 1: Delete the bucket
            self.ipc.delete_bucket(None, self.test_bucket_name)
            logger.info("Delete bucket operation completed")
            print("✅ Delete bucket operation completed")
            
            # Step 2: Wait for blockchain confirmation
            print("⏳ Waiting 10 seconds for blockchain confirmation...")
            time.sleep(10)
            
            # Step 3: Verify the bucket is deleted
            try:
                view_result = self.ipc.view_bucket(None, self.test_bucket_name)
                if view_result:
                    logger.error("Bucket still exists after deletion")
                    print("❌ Bucket still exists after deletion")
                    return False
                else:
                    logger.info("Bucket correctly not found after deletion")
                    print("✅ Bucket correctly not found after deletion")
                    return True
            except Exception as e:
                if "not found" in str(e).lower():
                    logger.info(f"Bucket correctly not found after deletion: {e}")
                    print("✅ Bucket correctly not found after deletion")
                    return True
                else:
                    logger.error(f"Unexpected error when viewing deleted bucket: {e}")
                    print(f"❌ Unexpected error when viewing deleted bucket: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"Delete bucket failed: {e}")
            print(f"❌ Delete bucket failed: {e}")
            return False
    
    def test_delete_bucket_nonexistent(self):
        """Test deleting a non-existent bucket"""
        try:
            non_existent_bucket = f"non-existent-bucket-{uuid.uuid4().hex[:8]}"
            logger.info(f"Testing delete non-existent bucket: {non_existent_bucket}")
            print(f"🔄 Testing delete non-existent bucket: {non_existent_bucket}")
            
            try:
                self.ipc.delete_bucket(None, non_existent_bucket)
                logger.error("Delete non-existent bucket should have failed")
                print("❌ Delete non-existent bucket should have failed")
                return False
            except Exception as e:
                if "not found" in str(e).lower():
                    logger.info(f"Delete non-existent bucket correctly rejected: {e}")
                    print("✅ Delete non-existent bucket correctly rejected")
                    return True
                else:
                    logger.error(f"Unexpected error for non-existent bucket deletion: {e}")
                    print(f"❌ Unexpected error for non-existent bucket deletion: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"Delete non-existent bucket test failed: {e}")
            print(f"❌ Delete non-existent bucket test failed: {e}")
            return False
    
    def test_delete_bucket_invalid_name(self):
        """Test deleting bucket with invalid name"""
        try:
            logger.info("Testing delete bucket with empty name")
            print("🔄 Testing delete bucket with empty name")
            
            try:
                self.ipc.delete_bucket(None, "")
                logger.error("Delete bucket with empty name should have failed")
                print("❌ Delete bucket with empty name should have failed")
                return False
            except SDKError as e:
                logger.info(f"Delete bucket with empty name correctly rejected: {e}")
                print("✅ Delete bucket with empty name correctly rejected")
                return True
            except Exception as e:
                if "invalid" in str(e).lower() or "name" in str(e).lower():
                    logger.info(f"Delete bucket with empty name correctly rejected: {e}")
                    print("✅ Delete bucket with empty name correctly rejected")
                    return True
                else:
                    logger.error(f"Unexpected error for empty name deletion: {e}")
                    print(f"❌ Unexpected error for empty name deletion: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"Delete invalid name test failed: {e}")
            print(f"❌ Delete invalid name test failed: {e}")
            return False
    
    def run_all_tests(self):
        """Run all bucket operation tests"""
        logger.info("🚀 Starting comprehensive bucket operations test suite")
        print("🚀 Starting comprehensive bucket operations test suite")
        print(f"📦 Test bucket name: {self.test_bucket_name}")
        
        if not self.setup_sdk():
            print("❌ Failed to setup SDK - aborting tests")
            return False
        
        # Test sequence - each test runs regardless of previous failures
        tests = [
            ("Create Bucket (Success)", self.test_create_bucket_success),
            ("List Buckets", self.test_list_buckets),
            ("View Bucket", self.test_view_bucket),
            ("Delete Bucket (Success)", self.test_delete_bucket_success),
            ("Delete Bucket (Non-existent)", self.test_delete_bucket_nonexistent),
            ("Delete Bucket (Invalid Name)", self.test_delete_bucket_invalid_name),
        ]
        
        # Run all tests
        for test_name, test_func in tests:
            self.run_test(test_name, test_func)
        
        # Print summary
        self.print_test_summary()
        
        # Return overall success
        failed_tests = [name for name, result in self.test_results.items() if result != "PASSED"]
        return len(failed_tests) == 0
    
    def print_test_summary(self):
        """Print comprehensive test summary"""
        print("\n" + "="*80)
        print("🏁 TEST SUMMARY")
        print("="*80)
        
        passed_count = 0
        failed_count = 0
        error_count = 0
        
        for test_name, result in self.test_results.items():
            if result == "PASSED":
                print(f"✅ {test_name}: {result}")
                passed_count += 1
            elif result == "FAILED":
                print(f"❌ {test_name}: {result}")
                failed_count += 1
            else:
                print(f"💥 {test_name}: {result}")
                error_count += 1
        
        print("-" * 80)
        print(f"📊 RESULTS: {passed_count} PASSED | {failed_count} FAILED | {error_count} ERRORS")
        
        if failed_count == 0 and error_count == 0:
            print("🎉 ALL TESTS PASSED!")
            logger.info("All bucket operation tests passed successfully!")
        else:
            print("⚠️  SOME TESTS FAILED - Check logs for details")
            logger.warning(f"Test results: {passed_count} passed, {failed_count} failed, {error_count} errors")
        
        print("="*80)
    
    def cleanup(self):
        """Clean up resources"""
        try:
            if self.sdk:
                self.sdk.close()
                logger.info("SDK closed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def main():
    """Main test runner"""
    test_runner = BucketTestRunner()
    
    try:
        success = test_runner.run_all_tests()
        
        if success:
            print("\n🎉 All bucket operation tests completed successfully!")
            sys.exit(0)
        else:
            print("\n⚠️  Some bucket operation tests failed - check logs for details")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error during test execution: {e}")
        logger.error(f"Unexpected error during test execution: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        test_runner.cleanup()

if __name__ == "__main__":
    main() 