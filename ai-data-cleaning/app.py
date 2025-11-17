#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FatigueSet 数据浏览器 - Flask Web 应用
用于浏览和分析 SQLite 数据库中的生理传感器数据
"""

from flask import Flask, render_template, jsonify, request, send_from_directory
import json
import os
from database import FatigueSetDatabase

app = Flask(__name__)

# 数据库配置
DB_PATH = "/Users/naluwei/Code/AI-Programming-Education-Suite/ai-data-cleaning/fatigueset.db"

def get_db():
    """获取数据库连接"""
    return FatigueSetDatabase(DB_PATH)

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    """获取数据库统计信息"""
    db = get_db()
    try:
        stats = db.get_database_stats()
        return jsonify(stats)
    finally:
        db.close()

@app.route('/api/participants')
def get_participants():
    """获取参与者列表"""
    db = get_db()
    try:
        participants = db.get_participants()
        return jsonify(participants)
    finally:
        db.close()

@app.route('/api/sensors')
def get_sensors():
    """获取传感器类型列表"""
    db = get_db()
    try:
        sensors = db.get_available_sensors()
        return jsonify(sensors)
    finally:
        db.close()

@app.route('/api/sensor/<sensor_name>/summary')
def get_sensor_summary(sensor_name):
    """获取传感器数据统计"""
    db = get_db()
    try:
        summary = db.get_sensor_data_summary(sensor_name)
        if summary is None:
            return jsonify({'error': 'Sensor not found or no data'}), 404
        return jsonify(summary)
    finally:
        db.close()

@app.route('/api/sensor/<sensor_name>/data')
def get_sensor_data(sensor_name):
    """获取传感器数据"""
    participant_id = request.args.get('participant_id')
    session_id = request.args.get('session_id')
    limit = int(request.args.get('limit', 1000))
    
    db = get_db()
    try:
        data = db.get_sensor_data(sensor_name, participant_id, session_id, limit)
        return jsonify(data)
    finally:
        db.close()

@app.route('/api/participant/<participant_id>/overview')
def get_participant_overview(participant_id):
    """获取参与者数据概览"""
    db = get_db()
    try:
        overview = db.get_participant_overview(participant_id)
        if overview is None:
            return jsonify({'error': 'Participant not found'}), 404
        return jsonify(overview)
    finally:
        db.close()

@app.route('/api/search')
def search_data():
    """通用数据搜索接口"""
    query_type = request.args.get('type', 'participants')
    search_params = request.args.to_dict()
    search_params.pop('type', None)  # 移除type参数
    
    db = get_db()
    try:
        result = db.search_data(query_type, search_params)
        if 'error' in result:
            return jsonify(result), 400
        return jsonify(result)
    finally:
        db.close()

# 静态文件路由
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

if __name__ == '__main__':
    app.run(debug=True, port=8080)