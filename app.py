from flask import Flask, render_template, request, jsonify
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
def search_landing():
    return render_template('comic.html')

@app.route('/search', methods=['GET'])
def search_comics():
    query = request.args.get('query', '')
    if not query:
        return jsonify({"error": "Query parameter is required"}), 400

    comic = collection.find_one({"$text": {"$search": query}}, {'_id': 0})
    if comic:
        return render_template('search_results.html', comic=comic, query=query)
    else:
        return render_template('search_results.html', comic=None, query=query)

if __name__ == '__main__':
    app.run(debug=True)