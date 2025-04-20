import os
from flask import Flask, request, jsonify, render_template, send_from_directory
from werkzeug.utils import secure_filename
from app import process_data, CONFIG

# Flask app configuration
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
ALLOWED_EXTENSIONS = {'csv'}

# Create upload folder if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    return render_template('ui.html')

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
            # Update CONFIG with new file paths
            for filename in saved_files:
                file_key = filename.lower().replace('.csv', '').replace(' ', '_')
                CONFIG['files_config'][file_key] = {
                    'path': os.path.join(app.config['UPLOAD_FOLDER'], filename),
                    'key_field': 'customercode' if 'customercode' in filename else 'index',
                    'headers': []  # Will be populated when processing
                }
            
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

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)