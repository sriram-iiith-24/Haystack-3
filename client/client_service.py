"""
Haystack Client - Command-line interface for distributed storage system
IMPROVEMENTS:
- Issue #7: Uses Nginx load balancer (high availability)
- Issue #9: Benefits from push-on-write caching
"""

import requests
import hashlib
import os
import sys
import time
import json
from pathlib import Path
from typing import Optional, List, Dict
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HaystackClient")

# IMPROVEMENT #7: Connect to Nginx load balancer instead of hardcoded directory
DIRECTORY_URL = os.getenv("DIRECTORY_SERVICE_URL", "http://nginx")
CACHE_URL = os.getenv("CACHE_SERVICE_URL", "http://cache:8100")
REPLICATION_URL = os.getenv("REPLICATION_MANAGER_URL", "http://replication:9003")

# Configuration
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 2))

# Colors for output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_success(message: str):
    """Print success message in green"""
    print(f"{Colors.OKGREEN}✓ {message}{Colors.ENDC}")


def print_error(message: str):
    """Print error message in red"""
    print(f"{Colors.FAIL}✗ {message}{Colors.ENDC}")


def print_info(message: str):
    """Print info message in blue"""
    print(f"{Colors.OKBLUE}ℹ {message}{Colors.ENDC}")


def print_warning(message: str):
    """Print warning message in yellow"""
    print(f"{Colors.WARNING}⚠ {message}{Colors.ENDC}")


def print_header(message: str):
    """Print header message"""
    print(f"\n{Colors.BOLD}{Colors.HEADER}{message}{Colors.ENDC}")


class HaystackClient:
    """Client for interacting with Haystack distributed storage system"""
    
    def __init__(self, directory_url: str, cache_url: str, replication_url: str):
        self.directory_url = directory_url
        self.cache_url = cache_url
        self.replication_url = replication_url
        
        logger.info(f"Initialized Haystack Client")
        logger.info(f"  Directory: {directory_url} (via Nginx load balancer)")
        logger.info(f"  Cache: {cache_url}")
        logger.info(f"  Replication: {replication_url}")
    
    def check_health(self) -> bool:
        """Check if all services are healthy"""
        print_header("Checking Service Health")
        
        services = {
            "Directory Service (Nginx)": f"{self.directory_url}/health",
            "Cache Service": f"{self.cache_url}/health",
            "Replication Manager": f"{self.replication_url}/health"
        }
        
        all_healthy = True
        
        for service_name, health_url in services.items():
            try:
                response = requests.get(health_url, timeout=5)
                if response.status_code == 200:
                    print_success(f"{service_name} is healthy")
                else:
                    print_error(f"{service_name} returned status {response.status_code}")
                    all_healthy = False
            except Exception as e:
                print_error(f"{service_name} is unreachable: {e}")
                all_healthy = False
        
        return all_healthy
    
    def upload(self, file_path: str, photo_id: Optional[str] = None) -> Optional[str]:
        """
        Upload a photo to the system
        IMPROVEMENT #16: Calculate SHA256 checksum
        """
        print_header(f"Uploading: {file_path}")
        
        if not os.path.exists(file_path):
            print_error(f"File not found: {file_path}")
            return None
        
        try:
            with open(file_path, 'rb') as f:
                photo_data = f.read()
            
            file_size_mb = len(photo_data) / (1024 * 1024)
            print_info(f"File size: {file_size_mb:.2f} MB")
            
            # IMPROVEMENT #16: Calculate checksum
            checksum = hashlib.sha256(photo_data).hexdigest()
            print_info(f"SHA256: {checksum[:16]}...")
            
        except Exception as e:
            print_error(f"Error reading file: {e}")
            return None
        
        if not photo_id:
            photo_id = checksum
            print_info(f"Generated photo ID: {photo_id[:16]}...")
        
        # Step 1: Allocate write location
        print_info("Step 1/3: Allocating write location...")
        
        try:
            response = requests.post(
                f"{self.directory_url}/allocate",
                json={"photo_id": photo_id},
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code != 200:
                print_error(f"Allocation failed: {response.status_code} - {response.text}")
                return None
            
            allocation = response.json()
            primary_store_url = allocation['primary_store_url']
            primary_store_id = allocation['primary_store_id']
            
            print_success(f"Allocated to {primary_store_id}")
            
        except Exception as e:
            print_error(f"Allocation failed: {e}")
            return None
        
        # Step 2: Write to store
        print_info(f"Step 2/3: Uploading to {primary_store_id}...")
        
        try:
            response = requests.post(
                f"{primary_store_url}/write",
                files={'photo': photo_data},
                data={'photo_id': photo_id},
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code != 200:
                print_error(f"Upload failed: {response.status_code} - {response.text}")
                return None
            
            result = response.json()
            store_id = result['store_id']
            volume_id = result['volume_id']
            
            print_success(f"Uploaded to {store_id}/{volume_id}")
            print_info("Photo automatically pushed to cache!")
            
        except Exception as e:
            print_error(f"Upload failed: {e}")
            return None
        
        # Step 3: Register with directory
        print_info("Step 3/3: Registering with directory...")
        
        try:
            response = requests.post(
                f"{self.directory_url}/register",
                json={
                    "photo_id": photo_id,
                    "store_id": store_id,
                    "volume_id": volume_id,
                    "checksum": checksum,
                    "size_bytes": len(photo_data)
                },
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code != 200:
                print_error(f"Registration failed: {response.status_code} - {response.text}")
                return None
            
            print_success("Registered with directory")
            
        except Exception as e:
            print_error(f"Registration failed: {e}")
            return None
        
        print_success(f"✓ Upload complete! Photo ID: {photo_id[:16]}...")
        print_info("Replication will happen automatically in the background")
        
        return photo_id
    
    def download(self, photo_id: str, output_path: Optional[str] = None, use_cache: bool = True) -> bool:
        """
        Download a photo from the system
        IMPROVEMENT #9: Benefits from push-on-write (cache likely populated)
        """
        print_header(f"Downloading: {photo_id[:16]}...")
        
        photo_data = None
        from_cache = False
        
        # Try cache first
        if use_cache:
            print_info("Step 1: Checking cache...")
            
            try:
                response = requests.get(
                    f"{self.cache_url}/cache/{photo_id}",
                    timeout=REQUEST_TIMEOUT
                )
                
                if response.status_code == 200:
                    photo_data = response.content
                    from_cache = True
                    print_success(f"Cache HIT! ({len(photo_data) / (1024 * 1024):.2f} MB)")
                else:
                    print_info("Cache MISS - will fetch from store")
            
            except Exception as e:
                logger.debug(f"Cache check failed: {e}")
                print_info("Cache unavailable - will fetch from store")
        else:
            print_info("Skipping cache (disabled)")
        
        # Fetch from store if not in cache
        if not photo_data:
            print_info("Step 2: Locating photo...")
            
            try:
                response = requests.get(
                    f"{self.directory_url}/locate/{photo_id}",
                    timeout=REQUEST_TIMEOUT
                )
                
                if response.status_code == 404:
                    print_error("Photo not found in directory")
                    return False
                
                if response.status_code != 200:
                    print_error(f"Location lookup failed: {response.status_code}")
                    return False
                
                locations = response.json()['locations']
                
                if not locations:
                    print_error("No locations found for photo")
                    return False
                
                print_success(f"Found at {len(locations)} healthy location(s)")
                for loc in locations:
                    print_info(f"  - {loc['store_id']}/{loc['volume_id']}")
                
            except Exception as e:
                print_error(f"Location lookup failed: {e}")
                return False
            
            print_info("Step 3: Downloading from store...")
            
            for attempt, location in enumerate(locations, 1):
                store_url = location['store_url']
                store_id = location['store_id']
                
                print_info(f"Attempt {attempt}/{len(locations)}: Trying {store_id}...")
                
                try:
                    response = requests.get(
                        f"{store_url}/read/{photo_id}",
                        timeout=REQUEST_TIMEOUT
                    )
                    
                    if response.status_code == 200:
                        photo_data = response.content
                        print_success(f"Downloaded from {store_id} ({len(photo_data) / (1024 * 1024):.2f} MB)")
                        break
                    else:
                        print_warning(f"{store_id} returned status {response.status_code}")
                
                except Exception as e:
                    print_warning(f"Failed to download from {store_id}: {e}")
            
            if not photo_data:
                print_error("Failed to download from all replicas")
                return False
        
        # Save to file if output path specified
        if output_path:
            try:
                with open(output_path, 'wb') as f:
                    f.write(photo_data)
                print_success(f"Saved to: {output_path}")
            except Exception as e:
                print_error(f"Failed to save file: {e}")
                return False
        
        source = "cache" if from_cache else "store"
        print_success(f"✓ Download complete! (from {source})")
        
        return True
    
    def delete(self, photo_id: str) -> bool:
        """Delete a photo from the system"""
        print_header(f"Deleting: {photo_id[:16]}...")
        
        print_info("Deleting from directory...")
        
        try:
            response = requests.delete(
                f"{self.directory_url}/delete/{photo_id}",
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 404:
                print_error("Photo not found")
                return False
            
            if response.status_code != 200:
                print_error(f"Delete failed: {response.status_code}")
                return False
            
            print_success("Deleted from directory")
            
        except Exception as e:
            print_error(f"Delete failed: {e}")
            return False
        
        print_info("Invalidating cache...")
        
        try:
            response = requests.delete(
                f"{self.cache_url}/cache/{photo_id}",
                timeout=REQUEST_TIMEOUT
            )
            print_success("Cache invalidated")
        except:
            logger.debug("Cache invalidation failed")
        
        print_success("✓ Delete complete!")
        print_info("Physical deletion from stores will happen during compaction")
        
        return True
    
    def status(self, photo_id: str) -> Optional[Dict]:
        """Get status information for a photo"""
        print_header(f"Status: {photo_id[:16]}...")
        
        try:
            response = requests.get(
                f"{self.replication_url}/replication/status/{photo_id}",
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 404:
                print_error("Photo not found")
                return None
            
            if response.status_code != 200:
                print_error(f"Status check failed: {response.status_code}")
                return None
            
            status = response.json()
            
            print_info(f"Photo ID: {photo_id[:16]}...")
            print_info(f"Replicas: {status['current_replica_count']}")
            
            print_info("Locations:")
            for loc in status['locations']:
                print(f"  • {loc['store_id']}/{loc['volume_id']}")
            
            if status.get('pending_tasks'):
                print_warning(f"Pending replication tasks: {len(status['pending_tasks'])}")
                for task in status['pending_tasks']:
                    print(f"  • To {task['target_store_id']} (status: {task['status']})")
            
            if status.get('access_stats'):
                stats = status['access_stats']
                print_info(f"Access Statistics:")
                print(f"  • Total accesses: {stats['total_accesses']}")
                print(f"  • Rate: {stats['access_rate_per_minute']:.1f} req/min")
            
            return status
            
        except Exception as e:
            print_error(f"Status check failed: {e}")
            return None
    
    def stats(self) -> bool:
        """Show system statistics"""
        print_header("System Statistics")
        
        # Directory stats
        print("\n" + Colors.BOLD + "Directory Service:" + Colors.ENDC)
        try:
            response = requests.get(f"{self.directory_url}/stats", timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                print(f"  Instance: {data['instance_id']}")
                print(f"  Is Leader: {data['is_leader']}")
                print(f"  Current Leader: {data['current_leader']}")
                print(f"  Total photos: {data['total_photos']}")
                print(f"  Total stores: {data['total_stores']}")
                print(f"  Healthy stores: {data['healthy_stores']}")
        except Exception as e:
            print_error(f"Failed to get directory stats: {e}")
        
        # Cache stats
        print("\n" + Colors.BOLD + "Cache Service (Redis):" + Colors.ENDC)
        try:
            response = requests.get(f"{self.cache_url}/cache/stats", timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                print(f"  Size: {data['size']['current_gb']:.2f}GB / {data['size']['max_gb']:.2f}GB ({data['size']['utilization_percent']:.1f}%)")
                print(f"  Entries: {data['entries']['count']} photos")
                print(f"  Hit rate: {data['operations']['hit_rate_percent']:.1f}%")
                print(f"  Hits: {data['operations']['hits']}, Misses: {data['operations']['misses']}")
        except Exception as e:
            print_error(f"Failed to get cache stats: {e}")
        
        # Replication Manager stats
        print("\n" + Colors.BOLD + "Replication Manager:" + Colors.ENDC)
        try:
            response = requests.get(f"{self.replication_url}/stats", timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                print(f"  Pending tasks: {data['tasks']['pending']}")
                print(f"  In progress: {data['tasks']['in_progress']}")
                print(f"  Completed: {data['tasks']['completed']}")
                print(f"  Failed: {data['tasks']['failed']}")
                print(f"  De-replicated: {data['tasks']['dereplicated']}")
                print(f"  Photos tracked: {data['access_stats']['photos_tracked']}")
                print(f"  Hot photos: {data['access_stats']['hot_photos']}")
        except Exception as e:
            print_error(f"Failed to get replication stats: {e}")
        
        return True


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Haystack Distributed Storage Client',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload a photo
  python client_service.py upload photo.jpg
  
  # Download a photo
  python client_service.py download <photo_id> output.jpg
  
  # Check photo status
  python client_service.py status <photo_id>
  
  # Delete a photo
  python client_service.py delete <photo_id>
  
  # Show system statistics
  python client_service.py stats
  
  # Check service health
  python client_service.py health
        """
    )
    
    parser.add_argument(
        'command',
        choices=['upload', 'download', 'delete', 'status', 'stats', 'health'],
        help='Command to execute'
    )
    
    parser.add_argument(
        'args',
        nargs='*',
        help='Command arguments'
    )
    
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Skip cache for downloads'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create client
    client = HaystackClient(DIRECTORY_URL, CACHE_URL, REPLICATION_URL)
    
    # Execute command
    try:
        if args.command == 'health':
            success = client.check_health()
            sys.exit(0 if success else 1)
        
        elif args.command == 'upload':
            if len(args.args) < 1:
                print_error("Usage: client_service.py upload <file_path>")
                sys.exit(1)
            
            photo_id = client.upload(args.args[0])
            sys.exit(0 if photo_id else 1)
        
        elif args.command == 'download':
            if len(args.args) < 1:
                print_error("Usage: client_service.py download <photo_id> [output_path]")
                sys.exit(1)
            
            photo_id = args.args[0]
            output_path = args.args[1] if len(args.args) > 1 else None
            
            success = client.download(photo_id, output_path, use_cache=not args.no_cache)
            sys.exit(0 if success else 1)
        
        elif args.command == 'delete':
            if len(args.args) < 1:
                print_error("Usage: client_service.py delete <photo_id>")
                sys.exit(1)
            
            success = client.delete(args.args[0])
            sys.exit(0 if success else 1)
        
        elif args.command == 'status':
            if len(args.args) < 1:
                print_error("Usage: client_service.py status <photo_id>")
                sys.exit(1)
            
            status = client.status(args.args[0])
            sys.exit(0 if status else 1)
        
        elif args.command == 'stats':
            success = client.stats()
            sys.exit(0 if success else 1)
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()