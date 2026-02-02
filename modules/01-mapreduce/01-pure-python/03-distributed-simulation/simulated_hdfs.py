"""
Simulated HDFS - Distributed File System Simulation

Simulates HDFS by splitting files into blocks and distributing them across "nodes".
"""

import os
import json
from pathlib import Path


class SimulatedHDFS:
    """Simulates Hadoop Distributed File System locally."""
    
    def __init__(self, base_dir="hdfs_storage", block_size=1024, replication=3, num_nodes=6):
        """
        Initialize simulated HDFS.
        
        Args:
            base_dir: Directory to store HDFS data
            block_size: Size of each block in bytes
            replication: Number of replicas for each block
            num_nodes: Total number of nodes in the cluster
        """
        self.base_dir = Path(base_dir)
        self.block_size = block_size
        self.replication = replication
        self.num_nodes = num_nodes
        self.metadata_file = self.base_dir / "metadata.json"
        
        # Create HDFS structure with multiple nodes
        self.base_dir.mkdir(exist_ok=True)
        for i in range(num_nodes):
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
        import random
        
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
            
            # Select random nodes for this block (simulating HDFS placement)
            # Each block goes to different set of nodes
            available_nodes = list(range(self.num_nodes))
            selected_nodes = random.sample(available_nodes, min(self.replication, self.num_nodes))
            
            # Replicate block across selected nodes
            replicas = []
            for node_id in selected_nodes:
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
    import sys
    
    # Initialize HDFS
    hdfs = SimulatedHDFS(block_size=8192, replication=3, num_nodes=6)
    
    if len(sys.argv) > 1:
        # User provided file(s)
        for file_path in sys.argv[1:]:
            if not os.path.exists(file_path):
                print(f"Error: File not found: {file_path}")
                continue
            
            file_name = os.path.basename(file_path)
            hdfs_path = f"/user/data/{file_name}"
            
            # Upload to HDFS
            hdfs.put(file_path, hdfs_path)
            
            # Get file info
            hdfs.get_info(hdfs_path)
        
        # List all files
        print(f"\n[HDFS] All files: {hdfs.list_files()}")
    else:
        # Demo mode - create sample file
        print("Demo mode: Creating sample file...")
        print("Usage: python simulated_hdfs.py <file1> [file2] ...\n")
        
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
        
        print("\nTo use with your own files:")
        print("  python simulated_hdfs.py myfile.txt")
        print("  python simulated_hdfs.py file1.txt file2.txt")
