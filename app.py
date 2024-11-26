from flask import Flask, jsonify, render_template
from pymongo import MongoClient
import os

app = Flask(__name__)

# MongoDB configuration
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")
DB_NAME = "xkcdDB"
COLLECTION_NAME = "comics"

# Initialize MongoDB client
client = MongoClient(MONGO_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

@app.route('/')
def index():
    print("Rendering index page")
    return render_template('index.html')

@app.route('/comics', methods=['GET'])
def get_comics():
    comics = list(collection.find({}, {'_id': 0}))
    return jsonify(comics)

@app.route('/comics/<int:comic_id>', methods=['GET'])
def get_comic(comic_id):
    comic = collection.find_one({"id": comic_id}, {'_id': 0})
    if comic:
        return jsonify(comic)
    else:
        return jsonify({"error": "Comic not found"}), 404

@app.route('/comic/<int:comic_id>')
def comic_page(comic_id):
    return render_template('comic.html')

@app.route('/test_connection')
def test_connection():
    try:
        # Try a simple database operation
        comic = collection.find_one()  # Fetch a single document from the collection
        if comic:
            return jsonify({"message": "MongoDB connection successful", "comic": comic})
        else:
            return jsonify({"message": "MongoDB connection successful, but no comics found!"})
    except Exception as e:
        return jsonify({"error": f"Failed to connect to MongoDB: {str(e)}"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)