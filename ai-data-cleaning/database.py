#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库查询工具 - 用于前端数据展示
提供各种查询接口和数据统计功能
"""

import sqlite3
import pandas as pd
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class FatigueSetDatabase:
    """FatigueSet 数据库查询接口"""
    
    def __init__(self, db_path):
        """
        初始化数据库连接
        
        Args:
            db_path: SQLite 数据库文件路径
        """
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """建立数据库连接"""
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # 使查询结果可以通过列名访问
        return self.conn
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def get_participants(self):
        """获取所有参与者信息"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT participant_id, low_session, medium_session, high_session
            FROM participants
            ORDER BY participant_id
        """)
        results = cursor.fetchall()
        conn.close()
        
        # 转换为字典列表
        participants = []
        for row in results:
            participants.append({
                'participant_id': row[0],
                'low_session': row[1],
                'medium_session': row[2],
                'high_session': row[3]
            })
        return participants
    
    def get_available_sensors(self):
        """获取所有可用的传感器类型"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sensor_name, description
            FROM data_dictionary
            ORDER BY sensor_name
        """)
        results = cursor.fetchall()
        conn.close()
        
        # 转换为字典列表
        sensors = []
        for row in results:
            sensors.append({
                'sensor_name': row[0],
                'description': row[1]
            })
        return sensors
    
    def get_sensor_stats(self, sensor_type):
        """获取指定传感器类型的统计信息"""
        conn = self.connect()
        cursor = conn.cursor()
        table_name = f"{sensor_type}_data"
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        result = cursor.fetchone()
        conn.close()
        return result['count'] if result else 0
    
    def get_sensor_data(self, sensor_type, participant_id=None, session_id=None, limit=1000):
        """获取传感器数据"""
        conn = self.connect()
        cursor = conn.cursor()
        table_name = f"{sensor_type}_data"
        
        query = f"SELECT * FROM {table_name} WHERE 1=1"
        params = []
        
        if participant_id:
            query += " AND participant_id = ?"
            params.append(participant_id)
        
        if session_id:
            query += " AND session_id = ?"
            params.append(session_id)
        
        query += " ORDER BY timestamp LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        return results
    
    def get_sensor_data_summary(self, sensor_name):
        """获取传感器数据统计信息"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (sensor_name,))
        
        if not cursor.fetchone():
            return None
        
        # 获取统计信息
        cursor.execute(f"SELECT COUNT(*) as total_records FROM {sensor_name}")
        total_records = cursor.fetchone()['total_records']
        
        cursor.execute(f"""
            SELECT participant_id, session_id, COUNT(*) as record_count
            FROM {sensor_name}
            GROUP BY participant_id, session_id
            ORDER BY participant_id, session_id
        """)
        
        participant_stats = []
        for row in cursor.fetchall():
            participant_stats.append({
                'participant_id': row['participant_id'],
                'session_id': row['session_id'],
                'record_count': row['record_count']
            })
        
        return {
            'sensor_name': sensor_name,
            'total_records': total_records,
            'participant_stats': participant_stats
        }
    
    def get_sensor_data(self, sensor_name, participant_id=None, session_id=None, limit=1000):
        """
        获取传感器数据
        
        Args:
            sensor_name: 传感器名称
            participant_id: 参与者ID（可选）
            session_id: 会话ID（可选）
            limit: 返回记录数限制
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        # 构建查询条件
        conditions = []
        params = []
        
        if participant_id:
            conditions.append("participant_id = ?")
            params.append(participant_id)
        
        if session_id:
            conditions.append("session_id = ?")
            params.append(session_id)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # 获取表结构
        cursor.execute(f"PRAGMA table_info({sensor_name})")
        columns = [row[1] for row in cursor.fetchall()]
        
        # 执行查询
        query = f"""
            SELECT * FROM {sensor_name}
            WHERE {where_clause}
            ORDER BY timestamp
            LIMIT ?
        """
        params.append(limit)
        
        cursor.execute(query, params)
        
        data = []
        for row in cursor.fetchall():
            record = {}
            for column in columns:
                record[column] = row[column]
            data.append(record)
        
        return {
            'sensor_name': sensor_name,
            'columns': columns,
            'data': data,
            'total_records': len(data)
        }
    
    def get_participant_overview(self, participant_id):
        """获取参与者的整体数据概览"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 获取参与者基本信息
        cursor.execute("""
            SELECT * FROM participants WHERE participant_id = ?
        """, (participant_id,))
        
        participant_info = cursor.fetchone()
        if not participant_info:
            return None
        
        # 获取可用的传感器
        sensors = self.get_available_sensors()
        
        # 获取每个传感器的数据统计
        sensor_stats = []
        for sensor in sensors:
            sensor_name = sensor['sensor_name']
            cursor.execute(f"""
                SELECT COUNT(*) as count, session_id
                FROM {sensor_name}
                WHERE participant_id = ?
                GROUP BY session_id
                ORDER BY session_id
            """, (participant_id,))
            
            session_stats = []
            for row in cursor.fetchall():
                session_stats.append({
                    'session_id': row['session_id'],
                    'record_count': row['count']
                })
            
            if session_stats:
                sensor_stats.append({
                    'sensor_name': sensor_name,
                    'description': sensor['description'],
                    'session_stats': session_stats
                })
        
        return {
            'participant_id': participant_id,
            'participant_info': dict(participant_info),
            'sensor_stats': sensor_stats
        }
    
    def search_data(self, query_type, search_params):
        """
        通用数据搜索接口
        
        Args:
            query_type: 查询类型 ('participants', 'sensors', 'data')
            search_params: 搜索参数字典
        """
        if query_type == 'participants':
            return self.get_participants()
        
        elif query_type == 'sensors':
            return self.get_available_sensors()
        
        elif query_type == 'data':
            sensor_name = search_params.get('sensor_name')
            participant_id = search_params.get('participant_id')
            session_id = search_params.get('session_id')
            limit = search_params.get('limit', 1000)
            
            if not sensor_name:
                return {'error': 'sensor_name is required'}
            
            return self.get_sensor_data(sensor_name, participant_id, session_id, limit)
        
        else:
            return {'error': 'Invalid query type'}
    
    def get_database_stats(self):
        """获取数据库整体统计信息"""
        conn = self.connect()
        cursor = conn.cursor()
        
        stats = {}
        
        # 参与者数量
        cursor.execute("SELECT COUNT(*) FROM participants")
        stats['total_participants'] = cursor.fetchone()[0]
        
        # 传感器类型数量
        cursor.execute("SELECT COUNT(*) FROM data_dictionary")
        stats['total_sensor_types'] = cursor.fetchone()[0]
        
        # 每个传感器的数据记录数
        sensor_stats = []
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('participants', 'data_dictionary')")
        
        for table in cursor.fetchall():
            table_name = table[0]
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                sensor_stats.append({
                    'sensor_name': table_name,
                    'record_count': count
                })
            except:
                continue
        
        stats['sensor_stats'] = sorted(sensor_stats, key=lambda x: x['record_count'], reverse=True)
        
        return stats

# 测试函数
def test_database():
    """测试数据库功能"""
    db_path = "/Users/naluwei/Code/AI-Programming-Education-Suite/ai-data-cleaning/fatigueset.db"
    
    db = FatigueSetDatabase(db_path)
    
    try:
        # 测试基本功能
        print("=== 数据库统计 ===")
        stats = db.get_database_stats()
        print(f"总参与者数: {stats['total_participants']}")
        print(f"传感器类型数: {stats['total_sensor_types']}")
        
        print("\n=== 参与者列表 ===")
        participants = db.get_participants()
        for p in participants[:5]:  # 显示前5个
            print(f"参与者 {p['participant_id']}: 低强度会话={p['low_session']}, 中强度会话={p['medium_session']}, 高强度会话={p['high_session']}")
        
        print(f"\n=== 可用传感器 ===")
        sensors = db.get_available_sensors()
        for sensor in sensors[:5]:  # 显示前5个
            print(f"{sensor['sensor_name']}: {sensor['description']}")
        
        print(f"\n=== 传感器数据统计 ===")
        if sensors:
            first_sensor = sensors[0]['sensor_name']
            summary = db.get_sensor_data_summary(first_sensor)
            if summary:
                print(f"{first_sensor}: {summary['total_records']} 条记录")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_database()