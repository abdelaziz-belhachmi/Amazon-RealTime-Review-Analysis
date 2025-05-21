from flask import Flask, render_template, jsonify
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)
client = MongoClient('mongodb://mongo:27017/')
db = client.sentiment

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/offline')
def offline_dashboard():
    return render_template('offlineDashboard.html')

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

@app.route('/api/overall-sentiment')
def get_overall_sentiment():
    pipeline = [
        {"$group": {
            "_id": "$predicted_sentiment",
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    results = list(db.predictions.aggregate(pipeline))
    return jsonify(results)

@app.route('/api/sentiment-by-date')
def get_sentiment_by_date():
    pipeline = [
        {
            "$addFields": {
                "date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$prediction_timestamp"
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "date": "$date",
                    "sentiment": "$predicted_sentiment"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$group": {
                "_id": "$_id.date",
                "sentiments": {
                    "$push": {
                        "sentiment": "$_id.sentiment",
                        "count": "$count"
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "negative": {
                    "$sum": {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": "$sentiments",
                                    "cond": {"$eq": ["$$this.sentiment", 0]}
                                }
                            },
                            "as": "s",
                            "in": "$$s.count"
                        }
                    }
                },
                "neutral": {
                    "$sum": {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": "$sentiments",
                                    "cond": {"$eq": ["$$this.sentiment", 1]}
                                }
                            },
                            "as": "s",
                            "in": "$$s.count"
                        }
                    }
                },
                "positive": {
                    "$sum": {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": "$sentiments",
                                    "cond": {"$eq": ["$$this.sentiment", 2]}
                                }
                            },
                            "as": "s",
                            "in": "$$s.count"
                        }
                    }
                }
            }
        },
        {"$sort": {"date": 1}}
    ]
    results = list(db.predictions.aggregate(pipeline))
    return jsonify(results)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)