from flask import Flask, render_template, jsonify
from pymongo import MongoClient
import datetime

app = Flask(__name__)
client = MongoClient('mongodb://mongo:27017/')
db = client.sentiment

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/sentiment-data')
def get_sentiment_data():
    pipeline = [
        {"$group": {
            "_id": "$predicted_sentiment",
            "count": {"$sum": 1}
        }}
    ]
    results = list(db.predictions.aggregate(pipeline))
    return jsonify(results)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)