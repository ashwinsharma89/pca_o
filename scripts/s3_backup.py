"""
S3 Backup Integration
Uploads backups to AWS S3 for off-site storage
"""
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

# Try to import boto3
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    logger.warning("boto3 not installed. Install with: pip install boto3")


class S3BackupManager:
    """Manages backup uploads to AWS S3."""
    
    def __init__(
        self,
        bucket_name: str = None,
        region: str = None,
        prefix: str = "backups/"
    ):
        """
        Initialize S3 backup manager.
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region
            prefix: S3 key prefix for backups
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for S3 integration. Install with: pip install boto3")
        
        self.bucket_name = bucket_name or os.getenv('S3_BACKUP_BUCKET')
        self.region = region or os.getenv('AWS_REGION', 'us-east-1')
        self.prefix = prefix
        
        if not self.bucket_name:
            raise ValueError("S3_BACKUP_BUCKET environment variable must be set")
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3', region_name=self.region)
        
        logger.info(f"S3 backup manager initialized: {self.bucket_name}/{self.prefix}")
    
    def upload_backup(self, backup_file: Path) -> Dict[str, Any]:
        """
        Upload backup file to S3.
        
        Args:
            backup_file: Path to backup file
            
        Returns:
            Dictionary with upload information
        """
        if not backup_file.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_file}")
        
        # Generate S3 key
        s3_key = f"{self.prefix}{backup_file.name}"
        
        logger.info(f"Uploading {backup_file.name} to S3...")
        logger.info(f"Bucket: {self.bucket_name}")
        logger.info(f"Key: {s3_key}")
        
        try:
            # Upload file
            file_size = backup_file.stat().st_size
            
            self.s3_client.upload_file(
                str(backup_file),
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'StorageClass': 'STANDARD_IA',  # Infrequent Access for cost savings
                    'ServerSideEncryption': 'AES256'  # Encrypt at rest
                }
            )
            
            logger.info(f"✅ Upload completed: {file_size / (1024 * 1024):.2f} MB")
            
            return {
                'success': True,
                'bucket': self.bucket_name,
                'key': s3_key,
                'size_mb': file_size / (1024 * 1024),
                'timestamp': datetime.now().isoformat()
            }
            
        except NoCredentialsError:
            logger.error("❌ AWS credentials not found")
            return {
                'success': False,
                'error': 'AWS credentials not configured'
            }
        except ClientError as e:
            logger.error(f"❌ S3 upload failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """
        List backups in S3.
        
        Returns:
            List of backup information dictionaries
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix
            )
            
            backups = []
            for obj in response.get('Contents', []):
                backups.append({
                    'key': obj['Key'],
                    'size_mb': obj['Size'] / (1024 * 1024),
                    'last_modified': obj['LastModified'].isoformat(),
                    'storage_class': obj.get('StorageClass', 'STANDARD')
                })
            
            return backups
            
        except ClientError as e:
            logger.error(f"Failed to list S3 backups: {e}")
            return []
    
    def download_backup(self, s3_key: str, local_path: Path) -> bool:
        """
        Download backup from S3.
        
        Args:
            s3_key: S3 object key
            local_path: Local path to save backup
            
        Returns:
            True if successful
        """
        try:
            logger.info(f"Downloading {s3_key} from S3...")
            
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                str(local_path)
            )
            
            logger.info(f"✅ Downloaded to: {local_path}")
            return True
            
        except ClientError as e:
            logger.error(f"❌ S3 download failed: {e}")
            return False
    
    def cleanup_old_backups(self, retention_days: int = 90):
        """
        Delete backups older than retention period.
        
        Args:
            retention_days: Number of days to keep backups
        """
        from datetime import timedelta
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        logger.info(f"Cleaning up S3 backups older than {retention_days} days...")
        
        backups = self.list_backups()
        deleted_count = 0
        
        for backup in backups:
            last_modified = datetime.fromisoformat(backup['last_modified'].replace('Z', '+00:00'))
            
            if last_modified < cutoff_date:
                try:
                    self.s3_client.delete_object(
                        Bucket=self.bucket_name,
                        Key=backup['key']
                    )
                    logger.info(f"Deleted old backup: {backup['key']}")
                    deleted_count += 1
                except ClientError as e:
                    logger.error(f"Failed to delete {backup['key']}: {e}")
        
        logger.info(f"✅ Cleaned up {deleted_count} old backups")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Manage S3 backups')
    parser.add_argument('action', choices=['upload', 'list', 'download', 'cleanup'],
                       help='Action to perform')
    parser.add_argument('--file', help='Backup file to upload')
    parser.add_argument('--key', help='S3 key for download')
    parser.add_argument('--output', help='Output path for download')
    parser.add_argument('--retention-days', type=int, default=90,
                       help='Retention period for cleanup (default: 90)')
    
    args = parser.parse_args()
    
    # Initialize S3 manager
    try:
        s3_manager = S3BackupManager()
    except Exception as e:
        logger.error(f"Failed to initialize S3 manager: {e}")
        sys.exit(1)
    
    # Perform action
    if args.action == 'upload':
        if not args.file:
            logger.error("--file required for upload")
            sys.exit(1)
        
        result = s3_manager.upload_backup(Path(args.file))
        if not result['success']:
            sys.exit(1)
    
    elif args.action == 'list':
        backups = s3_manager.list_backups()
        
        if not backups:
            print("No backups found in S3")
            return
        
        print("\nS3 Backups:")
        print("=" * 80)
        for backup in backups:
            print(f"Key: {backup['key']}")
            print(f"Size: {backup['size_mb']:.2f} MB")
            print(f"Modified: {backup['last_modified']}")
            print(f"Storage: {backup['storage_class']}")
            print()
    
    elif args.action == 'download':
        if not args.key or not args.output:
            logger.error("--key and --output required for download")
            sys.exit(1)
        
        success = s3_manager.download_backup(args.key, Path(args.output))
        if not success:
            sys.exit(1)
    
    elif args.action == 'cleanup':
        s3_manager.cleanup_old_backups(args.retention_days)


if __name__ == '__main__':
    main()
