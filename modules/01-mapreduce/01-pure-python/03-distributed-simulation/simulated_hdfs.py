"""
Simulated HDFS - Distributed File System Simulation

Simulates HDFS by splitting files into blocks and distributing them across "nodes".
"""

import os
import json
from pathlib import Path


class SimulatedHDFS:
    """Simulates Hadoop Distributed File System locally."""
    
    def __init__(self, base_dir="hdfs_storage", block_size=1024, replication=3):
        """
        Initialize simulated HDFS.
        
        Args:
            base_dir: Directory to store HDFS data
            block_size: Size of each block in bytes
            replication: Number of replicas for each block
        """
        self.base_dir = Path(base_dir)
        self.block_size = block_size
        self.replication = replication
        self.metadata_file = self.base_dir / "metadata.json"
        
        # Create HDFS structure
        self.base_dir.mkdir(exist_ok=True)
        for i in range(replication):
            (self.base_dir / f"node_{i}").mkdir(exist_ok=True)
        
        # Load or initialize metadata
        self.metadata = self._load_metadata()
    
    def _load_metadata(self):
        """Load metadata from disk."""
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_metadata(self):
        """Save metadata to disk."""
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
    
    def put(self, local_file, hdfs_path):
        """
        Upload a file to HDFS.
        
        Args:
            local_file: Path to local file
            hdfs_path: Destination path in HDFS
        """
        print(f"\n[HDFS] Uploading {local_file} to {hdfs_path}")
        
        # Read file content
        with open(local_file, 'rb') as f:
            content = f.read()
        
        # Split into blocks
        blocks = []
        block_id = 0
        for i in range(0, len(content), self.block_size):
            block_data = content[i:i + self.block_size]
            block_name = f"{hdfs_path.replace('/', '_')}_block_{block_id}"
            
            # Replicate block across nodes
            replicas = []
            for node_id in range(self.replication):
                node_dir = self.base_dir / f"node_{node_id}"
                block_path = node_dir / block_name
                
                with open(block_path, 'wb') as f:
                    f.write(block_data)
                
                replicas.append(f"node_{node_id}/{block_name}")
            
            blocks.append({
                'block_id': block_id,
                'size': len(block_data),
                'replicas': replicas
            })
            
            print(f"  Block {block_id}: {len(block_data)} bytes -> {replicas}")
            block_id += 1
        
        # Save metadata
        self.metadata[hdfs_path] = {
            'size': len(content),
            'blocks': blocks,
            'block_size': self.block_size
        }
        self._save_metadata()
        
        print(f"[HDFS] Upload complete: {len(blocks)} blocks, {len(content)} bytes")
    
    def get_blocks(self, hdfs_path):
        """
        Get block information for a file.
        
        Args:
            hdfs_path: Path in HDFS
        
        Returns:
            List of block information
        """
        if hdfs_path not in self.metadata:
            raise FileNotFoundError(f"File not found in HDFS: {hdfs_path}")
        
        return self.metadata[hdfs_path]['blocks']
    
    def read_block(self, block_replica):
        """
        Read a block from a specific replica.
        
        Args:
            block_replica: Path to block replica (e.g., "node_0/file_block_0")
        
        Returns:
            Block content as bytes
        """
        block_path = self.base_dir / block_replica
        with open(block_path, 'rb') as f:
            return f.read()
    
    def list_files(self):
        """List all files in HDFS."""
        return list(self.metadata.keys())
    
    def get_info(self, hdfs_path):
        """Get file information."""
        if hdfs_path not in self.metadata:
            raise FileNotFoundError(f"File not found in HDFS: {hdfs_path}")
        
        info = self.metadata[hdfs_path]
        print(f"\n[HDFS] File: {hdfs_path}")
        print(f"  Size: {info['size']} bytes")
        print(f"  Blocks: {len(info['blocks'])}")
        print(f"  Block size: {info['block_size']} bytes")
        print(f"  Replication: {self.replication}")
        
        return info


if __name__ == "__main__":
    # Demo
    hdfs = SimulatedHDFS(block_size=512, replication=3)
    
    # Create a sample file
    sample_file = "test_file.txt"
    with open(sample_file, 'w') as f:
        f.write("This is a test file for HDFS simulation.\n" * 50)
    
    # Upload to HDFS
    hdfs.put(sample_file, "/user/data/test.txt")
    
    # Get file info
    hdfs.get_info("/user/data/test.txt")
    
    # List files
    print(f"\n[HDFS] Files: {hdfs.list_files()}")
    
    # Clean up
    os.remove(sample_file)
