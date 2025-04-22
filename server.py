import os
from flask import Flask, request, jsonify, render_template, send_from_directory
from werkzeug.utils import secure_filename
from app import process_data, add_state_transformation, delete_state_transformation, state_rule_status
import config
from flask_cors import CORS
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Flask app configuration
app = Flask(__name__)
CORS(app)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
ALLOWED_EXTENSIONS = {'csv'}

# Get Neo4j credentials from environment
NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

# Create upload folder if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    return render_template('ui.html')

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'}), 200

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files[]' not in request.files:
        return jsonify({'error': 'No files provided'}), 400

    uploaded_files = request.files.getlist('files[]')
    saved_files = []

    for file in uploaded_files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            saved_files.append(filename)

    if saved_files:
        try:
            # Refresh the configuration to pick up new files
            config.CONFIG.update(config.load_config())
            
            # Process the data
            result = process_data()
            return jsonify({
                'message': 'Files uploaded and processed successfully',
                'files': saved_files,
                'result': result
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    return jsonify({'error': 'No valid files uploaded'}), 400

@app.route('/download/<filename>')
def download_file(filename):
    directory = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(directory, filename, as_attachment=True)

@app.route("/toggle_state_transformation", methods=["POST"])
def toggle_state_transformation():
    action = request.json.get("action")
    if not action:
        return jsonify({"success": False, "error": "Missing action parameter"}), 400

    print(f"Processing toggle action: {action}")
    try:
        if action == "add":
            add_state_transformation(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
            print("Successfully added transformation rule")
            return jsonify({
                "success": True, 
                "message": "State transformation rule added successfully."
            })
        elif action == "delete":
            try:
                delete_state_transformation(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
                print("Successfully deleted transformation rule")
                return jsonify({
                    "success": True, 
                    "message": "State transformation rule removed successfully."
                })
            except Exception as e:
                print(f"Error in delete_state_transformation: {str(e)}")
                return jsonify({
                    "success": False,
                    "error": f"Database error: {str(e)}"
                }), 500
        else:
            return jsonify({"success": False, "error": "Invalid action."}), 400
    except Exception as e:
        print(f"Error in toggle_state_transformation: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/state_rule_status", methods=["POST"])
def state_rule_status_route():
    try:
        active = state_rule_status(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        return jsonify({"success": True, "active": active})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)