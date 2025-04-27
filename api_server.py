from flask import Flask, request, jsonify
import blockchain_dag as bc
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='inlock_api.log'
)
logger = logging.getLogger('inlock_api')

# Initialize the application
app = Flask(__name__)
blockchain = bc.create_blockchain("production_blockchain.json")

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "ok", "service": "InLock Blockchain API"})

@app.route('/register_asset', methods=['POST'])
def api_register_asset():
    """Register a new asset on the blockchain."""
    try:
        data = request.json
        asset_id = data.get('asset_id')
        user_id = data.get('user_id')
        asset_data = data.get('asset_data', {})
        
        if not asset_id or not user_id:
            return jsonify({"success": False, "message": "Missing required fields"}), 400
        
        logger.info(f"Registering asset {asset_id} for user {user_id}")
        success, result = bc.register_asset(blockchain, asset_id, user_id, asset_data)
        
        if success:
            logger.info(f"Successfully registered asset {asset_id}, node_id: {result}")
        else:
            logger.warning(f"Failed to register asset {asset_id}: {result}")
            
        return jsonify({"success": success, "result": result})
    except Exception as e:
        logger.error(f"Error in register_asset: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

@app.route('/transfer_asset', methods=['POST'])
def api_transfer_asset():
    """Transfer an asset from one user to another."""
    try:
        data = request.json
        asset_id = data.get('asset_id')
        from_user_id = data.get('from_user_id')
        to_user_id = data.get('to_user_id')
        
        if not asset_id or not from_user_id or not to_user_id:
            return jsonify({"success": False, "message": "Missing required fields"}), 400
        
        logger.info(f"Transferring asset {asset_id} from {from_user_id} to {to_user_id}")
        success, result = bc.transfer_asset(blockchain, asset_id, from_user_id, to_user_id)
        
        if success:
            logger.info(f"Successfully transferred asset {asset_id}, node_id: {result}")
        else:
            logger.warning(f"Failed to transfer asset {asset_id}: {result}")
            
        return jsonify({"success": success, "result": result})
    except Exception as e:
        logger.error(f"Error in transfer_asset: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

@app.route('/stake_asset', methods=['POST'])
def api_stake_asset():
    """Stake an asset to generate tokens."""
    try:
        data = request.json
        asset_id = data.get('asset_id')
        user_id = data.get('user_id')
        staking_amount = data.get('staking_amount', 1)
        
        if not asset_id or not user_id:
            return jsonify({"success": False, "message": "Missing required fields"}), 400
        
        logger.info(f"Staking asset {asset_id} for user {user_id} with amount {staking_amount}")
        success, result = bc.stake_asset(blockchain, asset_id, user_id, staking_amount)
        
        if success:
            logger.info(f"Successfully staked asset {asset_id}, node_id: {result}")
        else:
            logger.warning(f"Failed to stake asset {asset_id}: {result}")
            
        return jsonify({"success": success, "result": result})
    except Exception as e:
        logger.error(f"Error in stake_asset: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

@app.route('/user_balance/<user_id>', methods=['GET'])
def api_user_balance(user_id):
    """Get the token balance for a user."""
    try:
        logger.info(f"Getting token balance for user {user_id}")
        balance = bc.get_user_token_balance(blockchain, user_id)
        return jsonify({"user_id": user_id, "balance": balance})
    except Exception as e:
        logger.error(f"Error in user_balance: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

@app.route('/user_assets/<user_id>', methods=['GET'])
def api_user_assets(user_id):
    """Get all assets owned by a user."""
    try:
        logger.info(f"Getting assets for user {user_id}")
        assets = bc.get_user_owned_assets(blockchain, user_id)
        return jsonify({"user_id": user_id, "assets": assets})
    except Exception as e:
        logger.error(f"Error in user_assets: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

@app.route('/verify_ownership', methods=['GET'])
def api_verify_ownership():
    """Verify if a user owns a specific asset."""
    try:
        asset_id = request.args.get('asset_id')
        user_id = request.args.get('user_id')
        
        if not asset_id or not user_id:
            return jsonify({"success": False, "message": "Missing required parameters"}), 400
        
        logger.info(f"Verifying ownership of asset {asset_id} for user {user_id}")
        is_owner = bc.verify_asset_ownership(blockchain, asset_id, user_id)
        return jsonify({"asset_id": asset_id, "user_id": user_id, "is_owner": is_owner})
    except Exception as e:
        logger.error(f"Error in verify_ownership: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

@app.route('/asset_history/<asset_id>', methods=['GET'])
def api_asset_history(asset_id):
    """Get the ownership history of an asset."""
    try:
        logger.info(f"Getting history for asset {asset_id}")
        history = blockchain.get_asset_ownership_history(asset_id)
        return jsonify({"asset_id": asset_id, "history": history})
    except Exception as e:
        logger.error(f"Error in asset_history: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

@app.route('/verify_integrity', methods=['GET'])
def api_verify_integrity():
    """Verify the integrity of the entire blockchain."""
    try:
        logger.info("Verifying blockchain integrity")
        integrity_ok, message = blockchain.verify_integrity()
        return jsonify({"integrity_ok": integrity_ok, "message": message})
    except Exception as e:
        logger.error(f"Error in verify_integrity: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

@app.route('/blockchain_stats', methods=['GET'])
def api_blockchain_stats():
    """Get statistics about the blockchain."""
    try:
        logger.info("Getting blockchain statistics")
        stats = {
            "total_nodes": len(blockchain.nodes),
            "total_tips": len(blockchain.tips),
            "unique_assets": len(set(node.asset_id for node in blockchain.nodes.values())),
            "unique_users": len(set(node.user_id for node in blockchain.nodes.values())),
            "action_counts": {
                "register": len([node for node in blockchain.nodes.values() if node.action == "register"]),
                "transfer": len([node for node in blockchain.nodes.values() if node.action == "transfer"]),
                "staking": len([node for node in blockchain.nodes.values() if node.action == "staking"])
            }
        }
        return jsonify({"success": True, "stats": stats})
    except Exception as e:
        logger.error(f"Error in blockchain_stats: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

if __name__ == '__main__':
    # Starting the server
    logger.info("Starting InLock Blockchain API Server")
    app.run(debug=False, host='0.0.0.0', port=5000)