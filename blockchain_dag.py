import json
import hashlib
import time
import uuid
from typing import Dict, List, Optional, Tuple, Set, Any
import os
import random
from flask import Flask, request, jsonify

class Node:
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
        self.asset_id = asset_id
        self.action = action
        self.user_id = user_id
        self.timestamp = timestamp if timestamp is not None else time.time()
        self.references = references if references is not None else []
        self.data = data if data is not None else {}
        self.signature = signature if signature is not None else self._generate_signature()
        self.node_id = node_id if node_id is not None else str(uuid.uuid4())
        self.hash = self._calculate_hash()
    
    def _generate_signature(self) -> str:
        signature_base = f"{self.user_id}:{self.timestamp}:{random.randint(1, 10000)}"
        return hashlib.sha256(signature_base.encode()).hexdigest()
    
    def _calculate_hash(self) -> str:
        content = (
            f"{self.asset_id}:{self.action}:{self.user_id}:"
            f"{self.timestamp}:{':'.join(self.references)}:"
            f"{self.signature}:{json.dumps(self.data, sort_keys=True)}"
        )
        return hashlib.sha256(content.encode()).hexdigest()
    
    def to_dict(self) -> Dict:
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
    def __init__(self, storage_path: str = "blockchain_dag.json"):
        self.nodes: Dict[str, Node] = {}
        self.tips: Set[str] = set()
        self.storage_path = storage_path
        
        if os.path.exists(storage_path):
            self.load()
    
    def add_node(self, node: Node) -> Tuple[bool, str]:
        valid, message = self._validate_node(node)
        if not valid:
            return False, message
        
        self.nodes[node.node_id] = node
        
        for ref in node.references:
            if ref in self.tips:
                self.tips.remove(ref)
        self.tips.add(node.node_id)
        
        self.save()
        
        return True, f"Node {node.node_id} added successfully"
    
    def _validate_node(self, node: Node) -> Tuple[bool, str]:
        if node.node_id in self.nodes:
            return False, f"Node with ID {node.node_id} already exists"
        
        for ref in node.references:
            if ref not in self.nodes:
                return False, f"Referenced node {ref} does not exist"
        
        if len(node.references) > 2:
            return False, "A node cannot have more than 2 references"
        
        if node.action == "register":
            for existing_node in self.nodes.values():
                if existing_node.asset_id == node.asset_id and existing_node.action == "register":
                    return False, f"Asset {node.asset_id} is already registered"
        
        elif node.action == "transfer":
            owner_history = self.get_asset_ownership_history(node.asset_id)
            if not owner_history:
                return False, f"Asset {node.asset_id} is not registered"
            
            current_owner = owner_history[-1]["user_id"]
            
            if current_owner != node.user_id:
                return False, f"Transfer requested by {node.user_id}, but asset is owned by {current_owner}"
            
            if "recipient_id" not in node.data:
                return False, "Transfer must include a recipient_id in the data"
        
        elif node.action == "staking":
            owner_history = self.get_asset_ownership_history(node.asset_id)
            if not owner_history:
                return False, f"Asset {node.asset_id} is not registered"
            
            current_owner = owner_history[-1]["user_id"]
            
            if current_owner != node.user_id:
                return False, f"Staking requested by {node.user_id}, but asset is owned by {current_owner}"
        
        return True, "Node is valid"
    
    def get_node(self, node_id: str) -> Optional[Node]:
        return self.nodes.get(node_id)
    
    def get_asset_nodes(self, asset_id: str) -> List[Node]:
        return [node for node in self.nodes.values() if node.asset_id == asset_id]
    
    def get_user_nodes(self, user_id: str) -> List[Node]:
        return [node for node in self.nodes.values() if node.user_id == user_id]
    
    def get_asset_ownership_history(self, asset_id: str) -> List[Dict]:
        asset_nodes = self.get_asset_nodes(asset_id)
        if not asset_nodes:
            return []
        
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
        all_assets = set()
        owned_assets = set()
        
        for node in self.nodes.values():
            all_assets.add(node.asset_id)
        
        for asset_id in all_assets:
            ownership_history = self.get_asset_ownership_history(asset_id)
            if ownership_history and ownership_history[-1]["user_id"] == user_id:
                owned_assets.add(asset_id)
        
        return list(owned_assets)
    
    def get_user_staking_balance(self, user_id: str) -> int:
        staking_nodes = [node for node in self.nodes.values() 
                         if node.action == "staking" and node.user_id == user_id]
        
        total_staking = 0
        for node in staking_nodes:
            staking_amount = node.data.get("staking_amount", 1)
            total_staking += staking_amount
        
        return total_staking
    
    def save(self):
        data = {
            "nodes": {node_id: node.to_dict() for node_id, node in self.nodes.items()},
            "tips": list(self.tips)
        }
        
        with open(self.storage_path, "w") as f:
            json.dump(data, f, indent=2)
    
    def load(self):
        with open(self.storage_path, "r") as f:
            data = json.load(f)
        
        self.nodes = {node_id: Node.from_dict(node_data) for node_id, node_data in data["nodes"].items()}
        self.tips = set(data["tips"])
    
    def get_tips(self) -> List[Node]:
        return [self.nodes[node_id] for node_id in self.tips]
    
    def choose_references(self) -> List[str]:
        tips_list = list(self.tips)
        
        if len(tips_list) >= 2:
            return random.sample(tips_list, 2)
        
        return tips_list
    
    def visualize(self) -> str:
        if not self.nodes:
            return "Empty DAG"
        
        sorted_nodes = sorted(self.nodes.values(), key=lambda x: x.timestamp)
        
        lines = []
        for i, node in enumerate(sorted_nodes):
            node_repr = f"{i+1}. [{node.node_id[:8]}] {node.action} - Asset: {node.asset_id} - User: {node.user_id}"
            if node.references:
                refs = [f"[{ref[:8]}]" for ref in node.references]
                node_repr += f" - Refs: {', '.join(refs)}"
            lines.append(node_repr)
        
        return "\n".join(lines)
    
    def verify_integrity(self) -> Tuple[bool, str]:
        for node_id, node in self.nodes.items():
            for ref in node.references:
                if ref not in self.nodes:
                    return False, f"Node {node_id} references non-existent node {ref}"
        
        for node_id, node in self.nodes.items():
            expected_hash = node._calculate_hash()
            if node.hash != expected_hash:
                return False, f"Hash mismatch for node {node_id}"
        
        return True, "DAG integrity verified"


def create_blockchain(storage_path: str = "blockchain_dag.json") -> DAG:
    return DAG(storage_path=storage_path)

def register_asset(blockchain: DAG, asset_id: str, user_id: str, asset_data: Dict = None) -> Tuple[bool, str]:
    references = blockchain.choose_references()
    
    node = Node(
        asset_id=asset_id,
        action="register",
        user_id=user_id,
        references=references,
        data=asset_data or {}
    )
    
    success, message = blockchain.add_node(node)
    if success:
        return True, node.node_id
    else:
        return False, message

def transfer_asset(blockchain: DAG, asset_id: str, from_user_id: str, to_user_id: str) -> Tuple[bool, str]:
    references = blockchain.choose_references()
    
    node = Node(
        asset_id=asset_id,
        action="transfer",
        user_id=from_user_id,
        references=references,
        data={"recipient_id": to_user_id}
    )
    
    success, message = blockchain.add_node(node)
    if success:
        return True, node.node_id
    else:
        return False, message

def stake_asset(blockchain: DAG, asset_id: str, user_id: str, staking_amount: int = 1) -> Tuple[bool, str]:
    references = blockchain.choose_references()
    
    node = Node(
        asset_id=asset_id,
        action="staking",
        user_id=user_id,
        references=references,
        data={"staking_amount": staking_amount}
    )
    
    success, message = blockchain.add_node(node)
    if success:
        return True, node.node_id
    else:
        return False, message

def get_user_token_balance(blockchain: DAG, user_id: str) -> int:
    return blockchain.get_user_staking_balance(user_id)

def get_user_owned_assets(blockchain: DAG, user_id: str) -> List[str]:
    return blockchain.get_user_assets(user_id)

def verify_asset_ownership(blockchain: DAG, asset_id: str, user_id: str) -> bool:
    ownership_history = blockchain.get_asset_ownership_history(asset_id)
    return ownership_history and ownership_history[-1]["user_id"] == user_id


app = Flask(__name__)
blockchain = create_blockchain()

@app.route('/process_nfc_tag', methods=['POST'])
def api_process_nfc_tag():
    data = request.json
    tag_id = data.get('tag_id')
    user_id = data.get('user_id')
    tag_technologies = data.get('tag_technologies', [])
    ndef_message = data.get('ndef_message', '')
    timestamp = data.get('timestamp')
    
    if not tag_id or not user_id:
        return jsonify({"success": False, "result": "Missing required fields", "action": "error", "asset_id": ""}), 400
    
    asset_id = tag_id
    
    asset_nodes = blockchain.get_asset_nodes(asset_id)
    register_node = next((node for node in asset_nodes if node.action == "register"), None)
    
    if register_node:
        owner_history = blockchain.get_asset_ownership_history(asset_id)
        current_owner = owner_history[-1]["user_id"] if owner_history else None
        
        if current_owner == user_id:
            return jsonify({
                "success": True,
                "result": "Asset verified, you are the owner",
                "action": "verify",
                "asset_id": asset_id
            })
        else:
            return jsonify({
                "success": True,
                "result": f"Asset owned by {current_owner}",
                "action": "verify",
                "asset_id": asset_id
            })
    else:
        return jsonify({
            "success": True,
            "result": "Asset not registered yet",
            "action": "unregistered",
            "asset_id": asset_id
        })

@app.route('/register_asset', methods=['POST'])
def api_register_asset():
    data = request.json
    asset_id = data.get('asset_id')
    user_id = data.get('user_id')
    asset_data = data.get('asset_data', {})
    
    if not asset_id or not user_id:
        return jsonify({"success": False, "result": "Missing required fields"}), 400
    
    success, result = register_asset(blockchain, asset_id, user_id, asset_data)
    return jsonify({"success": success, "result": result})

@app.route('/transfer_asset', methods=['POST'])
def api_transfer_asset():
    data = request.json
    asset_id = data.get('asset_id')
    from_user_id = data.get('from_user_id')
    to_user_id = data.get('to_user_id')
    
    if not asset_id or not from_user_id or not to_user_id:
        return jsonify({"success": False, "result": "Missing required fields"}), 400
    
    success, result = transfer_asset(blockchain, asset_id, from_user_id, to_user_id)
    return jsonify({"success": success, "result": result})

@app.route('/stake_asset', methods=['POST'])
def api_stake_asset():
    data = request.json
    asset_id = data.get('asset_id')
    user_id = data.get('user_id')
    staking_amount = data.get('staking_amount', 1)
    
    if not asset_id or not user_id:
        return jsonify({"success": False, "result": "Missing required fields"}), 400
    
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
        return jsonify({"success": False, "result": "Missing required parameters"}), 400
    
    is_owner = verify_asset_ownership(blockchain, asset_id, user_id)
    return jsonify({"asset_id": asset_id, "user_id": user_id, "is_owner": is_owner})

@app.route('/asset_history/<asset_id>', methods=['GET'])
def api_asset_history(asset_id):
    history = blockchain.get_asset_ownership_history(asset_id)
    return jsonify({"asset_id": asset_id, "history": history})

@app.route('/asset_data/<asset_id>', methods=['GET'])
def api_asset_data(asset_id):
    asset_nodes = blockchain.get_asset_nodes(asset_id)
    register_node = next((node for node in asset_nodes if node.action == "register"), None)
    
    data = {}
    if register_node and register_node.data:
        data = {k: str(v) for k, v in register_node.data.items()}
    
    return jsonify({"asset_id": asset_id, "data": data})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)