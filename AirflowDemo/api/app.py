from flask import Flask, request, jsonify
import yaml
import os

app = Flask(__name__)

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), '../config/sample.yaml')

@app.route('/config', methods=['GET'])
def get_config():
    with open(CONFIG_FILE_PATH, 'r') as file:
        config = yaml.safe_load(file)
    return jsonify(config)

@app.route('/config', methods=['POST'])
def update_config():
    new_config = request.json
    with open(CONFIG_FILE_PATH, 'w') as file:
        yaml.safe_dump(new_config, file)
    return jsonify({"message": "Config updated successfully"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
