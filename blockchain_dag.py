import json
import hashlib
import time
import uuid
from typing import Dict, List, Optional, Tuple, Set, Any
import os
import random


class Node:
    """Represents a single node in the DAG blockchain."""
    
    def __init__(
        self,
        asset_id: str,
        action: str,
        user_id: str,
        timestamp: float = None,
        references: List[str] = None,
        signature: str = None,
        node_id: str = None,
        data: Dict[str, Any] = None
    ):
        """
        Initialize a new Node in the DAG.
        
        Args:
            asset_id: Unique identifier for the asset
            action: Type of action (register, transfer, staking)
            user_id: ID of the user performing the action
            timestamp: Time when the node was created (defaults to current time)
            references: IDs of previous nodes this node references (defaults to [])
            signature: Digital signature (simulated in this prototype)
            node_id: Unique ID for this node (generated if not provided)
            data: Additional data associated with this action
        """
        self.asset_id = asset_id
        self.action = action
        self.user_id = user_id
        self.timestamp = timestamp if timestamp is not None else time.time()
        self.references = references if references is not None else []
        self.data = data if data is not None else {}
        
        # Generate a simulated signature if none provided
        self.signature = signature if signature is not None else self._generate_signature()
        
        # Generate a unique ID for this node if none provided
        self.node_id = node_id if node_id is not None else str(uuid.uuid4())
        
        # Calculate the hash of this node's content
        self.hash = self._calculate_hash()
    
    def _generate_signature(self) -> str:
        """Generate a simulated signature for this node."""
        # In a real implementation, this would use asymmetric cryptography
        # For this prototype, we'll use a simple hash of user_id + timestamp as a simulation
        signature_base = f"{self.user_id}:{self.timestamp}:{random.randint(1, 10000)}"
        return hashlib.sha256(signature_base.encode()).hexdigest()
    
    def _calculate_hash(self) -> str:
        """Calculate the hash of this node's content."""
        # Create a string representation of the node's content
        content = (
            f"{self.asset_id}:{self.action}:{self.user_id}:"
            f"{self.timestamp}:{':'.join(self.references)}:"
            f"{self.signature}:{json.dumps(self.data, sort_keys=True)}"
        )
        return hashlib.sha256(content.encode()).hexdigest()
    
    def to_dict(self) -> Dict:
        """Convert the node to a dictionary for serialization."""
        return {
            "node_id": self.node_id,
            "asset_id": self.asset_id,
            "action": self.action,
            "user_id": self.user_id,
            "timestamp": self.timestamp,
            "references": self.references,
            "signature": self.signature,
            "hash": self.hash,
            "data": self.data
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Node':
        """Create a Node from a dictionary."""
        return cls(
            asset_id=data["asset_id"],
            action=data["action"],
            user_id=data["user_id"],
            timestamp=data["timestamp"],
            references=data["references"],
            signature=data["signature"],
            node_id=data["node_id"],
            data=data.get("data", {})
        )


class DAG:
    """Represents a Directed Acyclic Graph blockchain."""
    
    def __init__(self, storage_path: str = "blockchain_dag.json"):
        """
        Initialize a new DAG blockchain.
        
        Args:
            storage_path: Path to the file where the blockchain will be stored
        """
        self.nodes: Dict[str, Node] = {}  # Map of node_id to Node
        self.tips: Set[str] = set()  # Set of node IDs that are tips (not referenced by any other node)
        self.storage_path = storage_path
        
        # Load existing blockchain if the file exists
        if os.path.exists(storage_path):
            self.load()
    
    def add_node(self, node: Node) -> Tuple[bool, str]:
        """
        Add a new node to the DAG.
        
        Args:
            node: The node to add
            
        Returns:
            Tuple of (success, message)
        """
        # Validate the node
        valid, message = self._validate_node(node)
        if not valid:
            return False, message
        
        # Add the node
        self.nodes[node.node_id] = node
        
        # Update tips: remove referenced nodes from tips and add this node
        for ref in node.references:
            if ref in self.tips:
                self.tips.remove(ref)
        self.tips.add(node.node_id)
        
        # Save the updated DAG
        self.save()
        
        return True, f"Node {node.node_id} added successfully"
    
    def _validate_node(self, node: Node) -> Tuple[bool, str]:
        """
        Validate a node before adding it to the DAG.
        
        Args:
            node: The node to validate
            
        Returns:
            Tuple of (valid, message)
        """
        # Check if node with this ID already exists
        if node.node_id in self.nodes:
            return False, f"Node with ID {node.node_id} already exists"
        
        # Verify that all references exist in the DAG
        for ref in node.references:
            if ref not in self.nodes:
                return False, f"Referenced node {ref} does not exist"
        
        # Verify that there are at most 2 references (for a true DAG)
        if len(node.references) > 2:
            return False, "A node cannot have more than 2 references"
        
        # Additional validations for different action types
        if node.action == "register":
            # Check if this asset_id is already registered
            for existing_node in self.nodes.values():
                if existing_node.asset_id == node.asset_id and existing_node.action == "register":
                    return False, f"Asset {node.asset_id} is already registered"
        
        elif node.action == "transfer":
            # Check if referenced nodes include a register node for this asset
            has_register = False
            current_owner = None
            
            # Traverse the DAG to find the current owner
            owner_history = self.get_asset_ownership_history(node.asset_id)
            if not owner_history:
                return False, f"Asset {node.asset_id} is not registered"
            
            current_owner = owner_history[-1]["user_id"]
            
            # Verify the transfer is requested by the current owner
            if current_owner != node.user_id:
                return False, f"Transfer requested by {node.user_id}, but asset is owned by {current_owner}"
            
            # Verify the transfer includes a recipient
            if "recipient_id" not in node.data:
                return False, "Transfer must include a recipient_id in the data"
        
        elif node.action == "staking":
            # Check if the asset exists and is owned by the staking user
            owner_history = self.get_asset_ownership_history(node.asset_id)
            if not owner_history:
                return False, f"Asset {node.asset_id} is not registered"
            
            current_owner = owner_history[-1]["user_id"]
            
            # Verify the staking is requested by the current owner
            if current_owner != node.user_id:
                return False, f"Staking requested by {node.user_id}, but asset is owned by {current_owner}"
        
        return True, "Node is valid"
    
    def get_node(self, node_id: str) -> Optional[Node]:
        """Get a node by its ID."""
        return self.nodes.get(node_id)
    
    def get_asset_nodes(self, asset_id: str) -> List[Node]:
        """Get all nodes related to a specific asset."""
        return [node for node in self.nodes.values() if node.asset_id == asset_id]
    
    def get_user_nodes(self, user_id: str) -> List[Node]:
        """Get all nodes related to a specific user."""
        return [node for node in self.nodes.values() if node.user_id == user_id]
    
    def get_asset_ownership_history(self, asset_id: str) -> List[Dict]:
        """
        Get the ownership history of an asset.
        
        Returns a list of dictionaries with user_id, timestamp, and node_id for each ownership change.
        """
        asset_nodes = self.get_asset_nodes(asset_id)
        if not asset_nodes:
            return []
        
        # Sort nodes by timestamp
        asset_nodes.sort(key=lambda x: x.timestamp)
        
        ownership_history = []
        current_owner = None
        
        for node in asset_nodes:
            if node.action == "register":
                current_owner = node.user_id
                ownership_history.append({
                    "user_id": current_owner,
                    "timestamp": node.timestamp,
                    "node_id": node.node_id,
                    "action": "register"
                })
            
            elif node.action == "transfer" and "recipient_id" in node.data:
                current_owner = node.data["recipient_id"]
                ownership_history.append({
                    "user_id": current_owner,
                    "timestamp": node.timestamp,
                    "node_id": node.node_id,
                    "action": "transfer"
                })
        
        return ownership_history
    
    def get_user_assets(self, user_id: str) -> List[str]:
        """Get all assets currently owned by a user."""
        all_assets = set()
        owned_assets = set()
        
        # First, find all assets
        for node in self.nodes.values():
            all_assets.add(node.asset_id)
        
        # Then, check the current owner of each asset
        for asset_id in all_assets:
            ownership_history = self.get_asset_ownership_history(asset_id)
            if ownership_history and ownership_history[-1]["user_id"] == user_id:
                owned_assets.add(asset_id)
        
        return list(owned_assets)
    
    def get_user_staking_balance(self, user_id: str) -> int:
        """Calculate the staking balance for a user based on staking events."""
        staking_nodes = [node for node in self.nodes.values() 
                         if node.action == "staking" and node.user_id == user_id]
        
        # Sum up staking amounts
        total_staking = 0
        for node in staking_nodes:
            # Get the staking amount from the node data (default to 1 if not specified)
            staking_amount = node.data.get("staking_amount", 1)
            
            # Add to total
            total_staking += staking_amount
        
        return total_staking
    
    def save(self):
        """Save the DAG to a JSON file."""
        data = {
            "nodes": {node_id: node.to_dict() for node_id, node in self.nodes.items()},
            "tips": list(self.tips)
        }
        
        with open(self.storage_path, "w") as f:
            json.dump(data, f, indent=2)
    
    def load(self):
        """Load the DAG from a JSON file."""
        with open(self.storage_path, "r") as f:
            data = json.load(f)
        
        self.nodes = {node_id: Node.from_dict(node_data) for node_id, node_data in data["nodes"].items()}
        self.tips = set(data["tips"])
    
    def get_tips(self) -> List[Node]:
        """Get all tip nodes (nodes that haven't been referenced yet)."""
        return [self.nodes[node_id] for node_id in self.tips]
    
    def choose_references(self) -> List[str]:
        """
        Choose reference nodes for a new node.
        
        In this implementation, we choose up to 2 random tips.
        A more sophisticated implementation might use a weighted selection
        based on transaction confidence or other factors.
        """
        tips_list = list(self.tips)
        
        # If we have at least 2 tips, choose 2 random ones
        if len(tips_list) >= 2:
            return random.sample(tips_list, 2)
        
        # Otherwise, return all available tips
        return tips_list
    
    def visualize(self) -> str:
        """
        Generate a simple text-based visualization of the DAG.
        
        Returns:
            A string representing the DAG structure
        """
        if not self.nodes:
            return "Empty DAG"
        
        # Sort nodes by timestamp
        sorted_nodes = sorted(self.nodes.values(), key=lambda x: x.timestamp)
        
        # Build a text representation
        lines = []
        for i, node in enumerate(sorted_nodes):
            node_repr = f"{i+1}. [{node.node_id[:8]}] {node.action} - Asset: {node.asset_id} - User: {node.user_id}"
            if node.references:
                refs = [f"[{ref[:8]}]" for ref in node.references]
                node_repr += f" - Refs: {', '.join(refs)}"
            lines.append(node_repr)
        
        return "\n".join(lines)
    
    def verify_integrity(self) -> Tuple[bool, str]:
        """
        Verify the integrity of the entire DAG.
        
        Returns:
            Tuple of (integrity_ok, message)
        """
        # Check that all referenced nodes exist
        for node_id, node in self.nodes.items():
            for ref in node.references:
                if ref not in self.nodes:
                    return False, f"Node {node_id} references non-existent node {ref}"
        
        # Verify hash integrity for each node
        for node_id, node in self.nodes.items():
            expected_hash = node._calculate_hash()
            if node.hash != expected_hash:
                return False, f"Hash mismatch for node {node_id}"
        
        return True, "DAG integrity verified"


# Helper functions for the blockchain API

def create_blockchain(storage_path: str = "blockchain_dag.json") -> DAG:
    """Create or load a blockchain DAG."""
    return DAG(storage_path=storage_path)

def register_asset(blockchain: DAG, asset_id: str, user_id: str, asset_data: Dict = None) -> Tuple[bool, str]:
    """
    Register a new asset on the blockchain.
    
    Args:
        blockchain: The blockchain DAG
        asset_id: Unique identifier for the asset
        user_id: ID of the user registering the asset
        asset_data: Additional data about the asset
        
    Returns:
        Tuple of (success, message or node_id)
    """
    # Choose references for the new node
    references = blockchain.choose_references()
    
    # Create a new node
    node = Node(
        asset_id=asset_id,
        action="register",
        user_id=user_id,
        references=references,
        data=asset_data or {}
    )
    
    # Add the node to the blockchain
    success, message = blockchain.add_node(node)
    if success:
        return True, node.node_id
    else:
        return False, message

def transfer_asset(blockchain: DAG, asset_id: str, from_user_id: str, to_user_id: str) -> Tuple[bool, str]:
    """
    Transfer an asset from one user to another.
    
    Args:
        blockchain: The blockchain DAG
        asset_id: ID of the asset to transfer
        from_user_id: ID of the current owner
        to_user_id: ID of the recipient
        
    Returns:
        Tuple of (success, message or node_id)
    """
    # Choose references for the new node
    references = blockchain.choose_references()
    
    # Create a new node
    node = Node(
        asset_id=asset_id,
        action="transfer",
        user_id=from_user_id,
        references=references,
        data={"recipient_id": to_user_id}
    )
    
    # Add the node to the blockchain
    success, message = blockchain.add_node(node)
    if success:
        return True, node.node_id
    else:
        return False, message

def stake_asset(blockchain: DAG, asset_id: str, user_id: str, staking_amount: int = 1) -> Tuple[bool, str]:
    references = blockchain.choose_references()
    
    # Create a new node
    node = Node(
        asset_id=asset_id,
        action="staking",
        user_id=user_id,
        references=references,
        data={"staking_amount": staking_amount}
    )
    
    # Add the node to the blockchain
    success, message = blockchain.add_node(node)
    if success:
        return True, node.node_id
    else:
        return False, message

def get_user_token_balance(blockchain: DAG, user_id: str) -> int:
    """Get the token balance for a user based on their staking activity."""
    return blockchain.get_user_staking_balance(user_id)

def get_user_owned_assets(blockchain: DAG, user_id: str) -> List[str]:
    """Get a list of assets currently owned by a user."""
    return blockchain.get_user_assets(user_id)

def verify_asset_ownership(blockchain: DAG, asset_id: str, user_id: str) -> bool:
    """Verify if a user owns a specific asset."""
    ownership_history = blockchain.get_asset_ownership_history(asset_id)
    return ownership_history and ownership_history[-1]["user_id"] == user_id


# API Server prototype using Flask (uncomment to use)
"""
from flask import Flask, request, jsonify

app = Flask(__name__)
blockchain = create_blockchain()

@app.route('/register_asset', methods=['POST'])
def api_register_asset():
    data = request.json
    asset_id = data.get('asset_id')
    user_id = data.get('user_id')
    asset_data = data.get('asset_data', {})
    
    if not asset_id or not user_id:
        return jsonify({"success": False, "message": "Missing required fields"}), 400
    
    success, result = register_asset(blockchain, asset_id, user_id, asset_data)
    return jsonify({"success": success, "result": result})

@app.route('/transfer_asset', methods=['POST'])
def api_transfer_asset():
    data = request.json
    asset_id = data.get('asset_id')
    from_user_id = data.get('from_user_id')
    to_user_id = data.get('to_user_id')
    
    if not asset_id or not from_user_id or not to_user_id:
        return jsonify({"success": False, "message": "Missing required fields"}), 400
    
    success, result = transfer_asset(blockchain, asset_id, from_user_id, to_user_id)
    return jsonify({"success": success, "result": result})

@app.route('/stake_asset', methods=['POST'])
def api_stake_asset():
    data = request.json
    asset_id = data.get('asset_id')
    user_id = data.get('user_id')
    staking_amount = data.get('staking_amount', 1)
    
    if not asset_id or not user_id:
        return jsonify({"success": False, "message": "Missing required fields"}), 400
    
    success, result = stake_asset(blockchain, asset_id, user_id, staking_amount)
    return jsonify({"success": success, "result": result})

@app.route('/user_balance/<user_id>', methods=['GET'])
def api_user_balance(user_id):
    balance = get_user_token_balance(blockchain, user_id)
    return jsonify({"user_id": user_id, "balance": balance})

@app.route('/user_assets/<user_id>', methods=['GET'])
def api_user_assets(user_id):
    assets = get_user_owned_assets(blockchain, user_id)
    return jsonify({"user_id": user_id, "assets": assets})

@app.route('/verify_ownership', methods=['GET'])
def api_verify_ownership():
    asset_id = request.args.get('asset_id')
    user_id = request.args.get('user_id')
    
    if not asset_id or not user_id:
        return jsonify({"success": False, "message": "Missing required parameters"}), 400
    
    is_owner = verify_asset_ownership(blockchain, asset_id, user_id)
    return jsonify({"asset_id": asset_id, "user_id": user_id, "is_owner": is_owner})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
"""

# Example usage of the blockchain
def run_example():
    # Create a new blockchain
    blockchain = create_blockchain("example_blockchain.json")
    
    print("Creating blockchain and registering assets...")
    
    # Register some assets
    asset1_id = "ASSET001"
    asset2_id = "ASSET002"
    user1_id = "USER001"
    user2_id = "USER002"
    
    # Register assets
    success1, result1 = register_asset(blockchain, asset1_id, user1_id, {"name": "Valuable Painting", "description": "A masterpiece"})
    success2, result2 = register_asset(blockchain, asset2_id, user2_id, {"name": "Sculpture", "description": "Modern art"})
    
    print(f"Registered asset 1: {success1}, {result1}")
    print(f"Registered asset 2: {success2}, {result2}")
    
    # Transfer asset 1 from user 1 to user 2
    success3, result3 = transfer_asset(blockchain, asset1_id, user1_id, user2_id)
    print(f"Transferred asset 1 to user 2: {success3}, {result3}")
    
    # Stake asset 2
    success4, result4 = stake_asset(blockchain, asset2_id, user2_id, 5)
    print(f"Staked asset 2: {success4}, {result4}")
    
    # Check ownership and balances
    print(f"User 1 owns: {get_user_owned_assets(blockchain, user1_id)}")
    print(f"User 2 owns: {get_user_owned_assets(blockchain, user2_id)}")
    print(f"User 1 token balance: {get_user_token_balance(blockchain, user1_id)}")
    print(f"User 2 token balance: {get_user_token_balance(blockchain, user2_id)}")
    
    # Verify ownership
    print(f"User 1 owns asset 1? {verify_asset_ownership(blockchain, asset1_id, user1_id)}")
    print(f"User 2 owns asset 1? {verify_asset_ownership(blockchain, asset1_id, user2_id)}")
    
    # Show the DAG structure
    print("\nBlockchain DAG structure:")
    print(blockchain.visualize())
    
    # Verify blockchain integrity
    integrity_ok, message = blockchain.verify_integrity()
    print(f"\nBlockchain integrity: {integrity_ok}, {message}")


if __name__ == "__main__":
    run_example()