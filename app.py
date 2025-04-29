import os
import logging
from flask import Flask, request, jsonify
import blockchain_dag as bc

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('inlock_api')

# Initialize the application
app = Flask(__name__)

# Initialize blockchain
blockchain = bc.create_blockchain("local_blockchain.json")

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "ok", "service": "InLock Blockchain API"})

@app.route('/process_nfc_tag', methods=['POST'])
def process_nfc_tag():
    """Process an NFC tag and register it as an asset if not already registered."""
    try:
        data = request.json
        tag_id = data.get('tag_id')
        user_id = data.get('user_id')
        
        if not tag_id or not user_id:
            return jsonify({"success": False, "message": "Missing tag_id or user_id"}), 400
        
        # Extract NDEF message if available
        ndef_message = data.get('ndef_message', '')
        
        # Create asset data from the tag information
        asset_data = {
            "tag_type": data.get('tag_type', 'NFC'),
            "tag_technologies": data.get('tag_technologies', []),
            "ndef_message": ndef_message,
            "scanned_timestamp": data.get('timestamp', 0)
        }
        
        # Check if asset already exists
        asset_exists = False
        for node in blockchain.nodes.values():
            if node.asset_id == tag_id and node.action == "register":
                asset_exists = True
                break
        
        if asset_exists:
            # If asset exists, just stake it
            logger.info(f"Asset {tag_id} already registered, staking it for user {user_id}")
            success, result = bc.stake_asset(blockchain, tag_id, user_id)
            return jsonify({
                "success": success,
                "result": result,
                "action": "staking",
                "asset_id": tag_id
            })
        else:
            # If asset doesn't exist, register it
            logger.info(f"Registering new asset {tag_id} for user {user_id}")
            success, result = bc.register_asset(blockchain, tag_id, user_id, asset_data)
            return jsonify({
                "success": success,
                "result": result,
                "action": "register",
                "asset_id": tag_id
            })
            
    except Exception as e:
        logger.error(f"Error processing NFC tag: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500

# Include all other endpoints from the original app.py
# ...

# Add a specific endpoint for your test tag
@app.route('/test_tag', methods=['GET'])
def test_tag():
    """Return information about the test tag."""
    test_tag_id = "07DE735808010"
    test_tag_data = {
        "tag_id": test_tag_id,
        "tag_technologies": [
            "android.nfc.tech.NfcV",
            "android.nfc.tech.Ndef"
        ],
        "ndef_message": "Record 0: E00401085873DE07"
    }
    
    # Check if this tag exists in blockchain
    asset_exists = False
    current_owner = None
    
    for node in blockchain.nodes.values():
        if node.asset_id == test_tag_id and node.action == "register":
            asset_exists = True
            # Find current owner
            history = blockchain.get_asset_ownership_history(test_tag_id)
            if history:
                current_owner = history[-1]["user_id"]
            break
    
    test_tag_data["registered"] = asset_exists
    test_tag_data["current_owner"] = current_owner
    
    return jsonify(test_tag_data)

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

if __name__ == '__main__':
    # Starting the server
    logger.info("Starting InLock Blockchain API Server")
    app.run(debug=True, host='0.0.0.0', port=5001)