"""
Vector Store Backup
S3/GCS backup for ChromaDB embeddings
"""
import os
import shutil
import boto3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Try to import GCS
try:
    from google.cloud import storage as gcs
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False


class VectorStoreBackup:
    """
    Backup ChromaDB vector store to cloud storage
    
    Supports:
    - AWS S3
    - Google Cloud Storage
    - Local filesystem
    
    Usage:
        backup = VectorStoreBackup(provider="s3", bucket="my-bucket")
        backup.backup("./chroma_db")
        backup.restore("./chroma_db", version="2024-01-15_12-00-00")
    """
    
    def __init__(
        self,
        provider: str = "local",  # s3, gcs, local
        bucket: str = None,
        backup_dir: str = "./backups/vectorstore"
    ):
        self.provider = provider
        self.bucket = bucket
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        self._s3 = self._init_s3() if provider == "s3" else None
        self._gcs = self._init_gcs() if provider == "gcs" else None
    
    def _init_s3(self):
        try:
            return boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION", "us-east-1")
            )
        except Exception as e:
            logger.error(f"S3 initialization failed: {e}")
            return None
    
    def _init_gcs(self):
        if not GCS_AVAILABLE:
            return None
        try:
            return gcs.Client()
        except Exception as e:
            logger.error(f"GCS initialization failed: {e}")
            return None
    
    def backup(self, source_path: str, prefix: str = "vectorstore") -> str:
        """
        Backup vector store to cloud storage
        
        Args:
            source_path: Path to ChromaDB directory
            prefix: Backup prefix
            
        Returns:
            Backup version identifier
        """
        source = Path(source_path)
        if not source.exists():
            raise ValueError(f"Source path does not exist: {source_path}")
        
        # Create backup version
        version = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        
        # Create local archive
        archive_name = f"{prefix}_{version}"
        archive_path = self.backup_dir / archive_name
        
        logger.info(f"Creating backup archive: {archive_path}")
        shutil.make_archive(str(archive_path), 'zip', source_path)
        archive_file = f"{archive_path}.zip"
        
        # Upload to cloud
        if self.provider == "s3" and self._s3:
            self._upload_s3(archive_file, f"{prefix}/{archive_name}.zip")
        elif self.provider == "gcs" and self._gcs:
            self._upload_gcs(archive_file, f"{prefix}/{archive_name}.zip")
        
        # Create manifest
        manifest = {
            "version": version,
            "created_at": datetime.utcnow().isoformat(),
            "source_path": str(source_path),
            "archive_file": archive_file,
            "provider": self.provider,
            "bucket": self.bucket
        }
        
        manifest_path = self.backup_dir / f"{archive_name}_manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"Backup completed: {version}")
        return version
    
    def _upload_s3(self, local_path: str, key: str):
        logger.info(f"Uploading to S3: s3://{self.bucket}/{key}")
        self._s3.upload_file(local_path, self.bucket, key)
    
    def _upload_gcs(self, local_path: str, key: str):
        logger.info(f"Uploading to GCS: gs://{self.bucket}/{key}")
        bucket = self._gcs.bucket(self.bucket)
        blob = bucket.blob(key)
        blob.upload_from_filename(local_path)
    
    def restore(self, target_path: str, version: str) -> bool:
        """
        Restore vector store from backup
        
        Args:
            target_path: Path to restore to
            version: Backup version to restore
        """
        target = Path(target_path)
        
        # Find backup
        archive_name = f"vectorstore_{version}.zip"
        local_archive = self.backup_dir / archive_name
        
        # Download if not local
        if not local_archive.exists():
            if self.provider == "s3" and self._s3:
                self._download_s3(f"vectorstore/{archive_name}", str(local_archive))
            elif self.provider == "gcs" and self._gcs:
                self._download_gcs(f"vectorstore/{archive_name}", str(local_archive))
            else:
                raise ValueError(f"Backup not found: {version}")
        
        # Backup existing if present
        if target.exists():
            backup_existing = target.parent / f"{target.name}_pre_restore"
            shutil.move(str(target), str(backup_existing))
            logger.info(f"Existing data moved to: {backup_existing}")
        
        # Extract
        target.mkdir(parents=True, exist_ok=True)
        shutil.unpack_archive(str(local_archive), str(target))
        
        logger.info(f"Restored backup {version} to {target_path}")
        return True
    
    def _download_s3(self, key: str, local_path: str):
        logger.info(f"Downloading from S3: s3://{self.bucket}/{key}")
        self._s3.download_file(self.bucket, key, local_path)
    
    def _download_gcs(self, key: str, local_path: str):
        logger.info(f"Downloading from GCS: gs://{self.bucket}/{key}")
        bucket = self._gcs.bucket(self.bucket)
        blob = bucket.blob(key)
        blob.download_to_filename(local_path)
    
    def list_backups(self) -> list:
        """List available backups"""
        backups = []
        
        # Local backups
        for f in self.backup_dir.glob("*_manifest.json"):
            with open(f) as file:
                backups.append(json.load(file))
        
        # Cloud backups
        if self.provider == "s3" and self._s3:
            try:
                response = self._s3.list_objects_v2(Bucket=self.bucket, Prefix="vectorstore/")
                for obj in response.get('Contents', []):
                    if obj['Key'].endswith('.zip'):
                        backups.append({
                            "version": obj['Key'].split('_')[-1].replace('.zip', ''),
                            "size": obj['Size'],
                            "provider": "s3"
                        })
            except Exception as e:
                logger.error(f"Failed to list S3 backups: {e}")
        
        return sorted(backups, key=lambda x: x.get('version', ''), reverse=True)
    
    def cleanup_old_backups(self, keep_count: int = 5):
        """Remove old backups, keeping the most recent"""
        backups = self.list_backups()
        
        if len(backups) <= keep_count:
            return
        
        to_remove = backups[keep_count:]
        
        for backup in to_remove:
            version = backup.get('version')
            archive_path = self.backup_dir / f"vectorstore_{version}.zip"
            manifest_path = self.backup_dir / f"vectorstore_{version}_manifest.json"
            
            if archive_path.exists():
                archive_path.unlink()
            if manifest_path.exists():
                manifest_path.unlink()
            
            logger.info(f"Removed old backup: {version}")


def get_backup_manager() -> VectorStoreBackup:
    """Get configured backup manager"""
    provider = os.getenv("BACKUP_PROVIDER", "local")
    bucket = os.getenv("BACKUP_BUCKET")
    
    return VectorStoreBackup(provider=provider, bucket=bucket)
