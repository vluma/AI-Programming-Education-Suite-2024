#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据抽取工具 - FatigueSet 数据集处理
用于将生理传感器数据抽取到 SQLite 数据库中
"""

import os
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FatigueSetDataExtractor:
    """FatigueSet 数据集抽取器"""
    
    def __init__(self, data_root_path, db_path):
        """
        初始化抽取器
        
        Args:
            data_root_path: 数据根目录路径
            db_path: SQLite 数据库文件路径
        """
        self.data_root_path = Path(data_root_path)
        self.db_path = db_path
        self.conn = None
        
        # 传感器数据类型定义（包含中文备注）
        self.sensor_definitions = {
            # Nokia Bell Labs eSense earable sensor
            'ear_acc_left': {
                'columns': ['timestamp', 'ax', 'ay', 'az'],
                'description': '左耳加速度计数据',
                'units': 'g (重力加速度单位，范围: -2 到 +2)',
                'sampling_rate': '100 Hz',
                'sensor_type': '耳戴式加速度计'
            },
            'ear_acc_right': {
                'columns': ['timestamp', 'ax', 'ay', 'az'],
                'description': '右耳加速度计数据',
                'units': 'g (重力加速度单位，范围: -2 到 +2)',
                'sampling_rate': '100 Hz',
                'sensor_type': '耳戴式加速度计'
            },
            'ear_gyro_left': {
                'columns': ['timestamp', 'gx', 'gy', 'gz'],
                'description': '左耳陀螺仪数据',
                'units': '度/秒 (角速度，范围: -500 到 +500)',
                'sampling_rate': '100 Hz',
                'sensor_type': '耳戴式陀螺仪'
            },
            'ear_gyro_right': {
                'columns': ['timestamp', 'gx', 'gy', 'gz'],
                'description': '右耳陀螺仪数据',
                'units': '度/秒 (角速度，范围: -500 到 +500)',
                'sampling_rate': '100 Hz',
                'sensor_type': '耳戴式陀螺仪'
            },
            'ear_ppg_left': {
                'columns': ['timestamp', 'green', 'ir', 'red'],
                'description': '左耳光电容积脉搏波(PPG)数据',
                'units': '无单位（光强度）',
                'sampling_rate': '100 Hz',
                'sensor_type': '耳戴式PPG传感器'
            },
            'ear_ppg_right': {
                'columns': ['timestamp', 'green', 'ir', 'red'],
                'description': '右耳光电容积脉搏波(PPG)数据',
                'units': '无单位（光强度）',
                'sampling_rate': '100 Hz',
                'sensor_type': '耳戴式PPG传感器'
            },
            
            # Muse S Headband
            'forehead_acc': {
                'columns': ['timestamp', 'ax', 'ay', 'az'],
                'description': '前额加速度计数据',
                'units': 'g (重力加速度单位，范围: -2 到 +2)',
                'sampling_rate': '52 Hz',
                'sensor_type': '前额加速度计'
            },
            'forehead_eeg_alpha_abs': {
                'columns': ['timestamp', 'TP9', 'AF7', 'AF8', 'TP10'],
                'description': '前额EEG α波段绝对功率',
                'units': 'Bels (贝尔)',
                'sampling_rate': '10 Hz',
                'sensor_type': '前额脑电图(EEG)'
            },
            'forehead_eeg_beta_abs': {
                'columns': ['timestamp', 'TP9', 'AF7', 'AF8', 'TP10'],
                'description': '前额EEG β波段绝对功率',
                'units': 'Bels (贝尔)',
                'sampling_rate': '10 Hz',
                'sensor_type': '前额脑电图(EEG)'
            },
            'forehead_eeg_delta_abs': {
                'columns': ['timestamp', 'TP9', 'AF7', 'AF8', 'TP10'],
                'description': '前额EEG δ波段绝对功率',
                'units': 'Bels (贝尔)',
                'sampling_rate': '10 Hz',
                'sensor_type': '前额脑电图(EEG)'
            },
            'forehead_eeg_gamma_abs': {
                'columns': ['timestamp', 'TP9', 'AF7', 'AF8', 'TP10'],
                'description': '前额EEG γ波段绝对功率',
                'units': 'Bels (贝尔)',
                'sampling_rate': '10 Hz',
                'sensor_type': '前额脑电图(EEG)'
            },
            'forehead_eeg_theta_abs': {
                'columns': ['timestamp', 'TP9', 'AF7', 'AF8', 'TP10'],
                'description': '前额EEG θ波段绝对功率',
                'units': 'Bels (贝尔)',
                'sampling_rate': '10 Hz',
                'sensor_type': '前额脑电图(EEG)'
            },
            'forehead_eeg_raw': {
                'columns': ['timestamp', 'TP9', 'AF7', 'AF8', 'TP10'],
                'description': '前额原始EEG波形',
                'units': 'μV (微伏，范围: 0.0 到 1682.815)',
                'sampling_rate': '256 Hz',
                'sensor_type': '前额脑电图(EEG)'
            },
            'forehead_gyro': {
                'columns': ['timestamp', 'gx', 'gy', 'gz'],
                'description': '前额陀螺仪数据',
                'units': '度/秒 (角速度，范围: -245 到 +245)',
                'sampling_rate': '52 Hz',
                'sensor_type': '前额陀螺仪'
            },
            
            # Muse S 设备事件和状态
            'muse_blinks': {
                'columns': ['timestamp', 'is_blink'],
                'description': '眨眼事件检测',
                'units': '1=眨眼，0=无眨眼',
                'sampling_rate': '10 Hz (检测到眨眼时)',
                'sensor_type': '眨眼检测'
            },
            'muse_jaw_clenches': {
                'columns': ['timestamp', 'is_clench'],
                'description': '咬牙事件检测',
                'units': '1=咬牙，0=无咬牙',
                'sampling_rate': '10 Hz (检测到咬牙时)',
                'sensor_type': '咬牙检测'
            },
            'muse_device_battery': {
                'columns': ['timestamp', 'battery_level_muse', 'battery_voltage_muse', 'adc_voltage_muse', 'temperature_muse'],
                'description': 'Muse设备电池和温度状态',
                'units': '百分比, mV, {-1=不可用}, °C',
                'sampling_rate': '0.1 Hz',
                'sensor_type': '设备状态监控'
            },
            'muse_device_fit': {
                'columns': ['timestamp', 'TP9', 'AF7', 'AF8', 'TP10'],
                'description': 'Muse设备各传感器接触质量',
                'units': '1=良好，2=中等，4=差',
                'sampling_rate': '10 Hz',
                'sensor_type': '设备贴合度检测'
            },
            'muse_device_touch': {
                'columns': ['timestamp', 'is_touching'],
                'description': '前额接触检测',
                'units': '1=接触，0=未接触',
                'sampling_rate': '10 Hz',
                'sensor_type': '接触检测'
            },
            
            # Zephyr BioHarness 3.0 chest monitor
            'chest_raw_acc': {
                'columns': ['timestamp', 'vertical', 'lateral', 'sagittal'],
                'description': '胸部原始加速度计数据',
                'units': '12位原始数据 (中心值2048，1g=83位)',
                'sampling_rate': '100 Hz',
                'sensor_type': '胸部加速度计'
            },
            'chest_bb_interval': {
                'columns': ['timestamp', 'duration'],
                'description': '呼吸事件时间间隔',
                'units': 'ms (毫秒)',
                'sampling_rate': '每次检测到呼吸时',
                'sensor_type': '呼吸检测'
            },
            'chest_physiology_summary': {
                'columns': ['timestamp', 'hr', 'br', 'posture', 'hr_confidence', 'hrv', 'is_hr_unreliable', 'is_br_unreliable', 'is_hrv_unreliable'],
                'description': '胸部生理参数汇总',
                'units': 'bpm {25:240}, 呼吸/分钟 {4:70}, 度 {−180:180}, 百分比, ms, 1=不可靠',
                'sampling_rate': '1 Hz',
                'sensor_type': '生理参数监控'
            },
            'chest_raw_breathing': {
                'columns': ['timestamp', 'breathing_waveform'],
                'description': '胸部原始呼吸波形',
                'units': '24位原始数据',
                'sampling_rate': '25 Hz',
                'sensor_type': '呼吸传感器'
            },
            'chest_raw_ecg': {
                'columns': ['timestamp', 'ecg_waveform'],
                'description': '胸部原始心电图',
                'units': '12位数据 (1位=0.0067025 mV)',
                'sampling_rate': '250 Hz',
                'sensor_type': '心电图(ECG)'
            },
            'chest_rr_interval': {
                'columns': ['timestamp', 'duration'],
                'description': 'R波间期',
                'units': 'ms (毫秒)',
                'sampling_rate': '每次检测到R波时',
                'sensor_type': '心电R波检测'
            },
            'chest_sensor_summary': {
                'columns': ['timestamp', 'acc_magnitude', 'acc_peak', 'acc_peak_vertical_angle', 'acc_peak_horizontal_angle', 'ecg_amp_uncalibrated', 'ecg_noise_uncalibrated'],
                'description': '胸部传感器汇总数据',
                'units': 'VMU {0:16}, g {0:16}, 度 {0:180}, 度 {−180:180}, V {0:0.05}, V {0:0.05}',
                'sampling_rate': '1 Hz',
                'sensor_type': '传感器汇总'
            },
            
            # Zephyr 活动汇总
            'zephyr_activity_summary': {
                'columns': ['timestamp', 'cumulative_impulse_load', 'walking_step_count', 'running_step_count', 'bound_count', 'jump_count', 'minor_impact_count', 'major_impact_count', 'avg_force_dev_rate', 'avg_step_impulse', 'avg_step_period', 'last_jump_flight_time'],
                'description': 'Zephyr活动汇总统计',
                'units': '牛顿, 步数, 步数, 跳跃次数, 冲击次数, 冲击力, 步态参数',
                'sampling_rate': '1 Hz',
                'sensor_type': '活动监控'
            },
            'zephyr_device_status': {
                'columns': ['timestamp', 'battery_voltage', 'battery_level', 'device_temperature', 'bluetooth_link_quality', 'bluetooth_rssi', 'bluetooth_tx_power', 'worn_confidence', 'is_button_press', 'is_not_fitted_to_garment'],
                'description': 'Zephyr设备状态',
                'units': 'V, 百分比, °C, 链接质量, dB, dBm, 佩戴置信度, 按钮状态, 贴合状态',
                'sampling_rate': '1 Hz',
                'sensor_type': '设备状态'
            },
            
            # Empatica E4 wristband
            'wrist_acc': {
                'columns': ['timestamp', 'ax', 'ay', 'az'],
                'description': '手腕加速度计数据',
                'units': 'g (重力加速度单位，范围: -2 到 +2)',
                'sampling_rate': '32 Hz',
                'sensor_type': '手腕加速度计'
            },
            'wrist_bvp': {
                'columns': ['timestamp', 'bvp'],
                'description': '手腕血容量脉搏',
                'units': '无单位（来自两种不同光反射测量的组合）',
                'sampling_rate': '64 Hz',
                'sensor_type': '手腕PPG传感器'
            },
            'wrist_eda': {
                'columns': ['timestamp', 'eda'],
                'description': '手腕皮肤电活动',
                'units': '微西门子 (μS)',
                'sampling_rate': '4 Hz',
                'sensor_type': '皮肤电传感器'
            },
            'wrist_hr': {
                'columns': ['timestamp', 'hr'],
                'description': '手腕心率（过去10秒平均值）',
                'units': 'bpm (次/分钟)',
                'sampling_rate': '1 Hz',
                'sensor_type': '心率传感器'
            },
            'wrist_ibi': {
                'columns': ['timestamp', 'duration'],
                'description': '心跳间期',
                'units': 'ms (毫秒)',
                'sampling_rate': '每次检测到心跳时',
                'sensor_type': '心跳检测'
            },
            'wrist_skin_temperature': {
                'columns': ['timestamp', 'temp'],
                'description': '手腕皮肤温度',
                'units': '°C (摄氏度)',
                'sampling_rate': '4 Hz',
                'sensor_type': '温度传感器'
            }
        }
        
    def create_database_schema(self):
        """创建数据库表结构"""
        logger.info("创建数据库表结构...")
        
        # 参与者元数据表
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS participants (
                participant_id TEXT PRIMARY KEY,
                low_session INTEGER,
                medium_session INTEGER,
                high_session INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                备注 TEXT DEFAULT '参与者会话安排信息'
            )
        ''')
        
        # 传感器数据表模板
        for sensor_name, sensor_info in self.sensor_definitions.items():
            columns_sql = []
            for i, column in enumerate(sensor_info['columns']):
                if column == 'timestamp':
                    columns_sql.append(f"{column} REAL")
                elif column.startswith('is_') or column in ['TP9', 'AF7', 'AF8', 'TP10', 'worn_confidence', 'is_button_press', 'is_not_fitted_to_garment']:
                    columns_sql.append(f"{column} INTEGER")
                else:
                    columns_sql.append(f"{column} REAL")
            
            # 添加通用字段
            columns_sql.extend([
                "participant_id TEXT",
                "session_id INTEGER",
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ])
            
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {sensor_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {', '.join(columns_sql)},
                    FOREIGN KEY (participant_id) REFERENCES participants (participant_id)
                )
            """
            
            self.conn.execute(create_table_sql)
            logger.info(f"创建表: {sensor_name}")
        
        # 数据字典表
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS data_dictionary (
                sensor_name TEXT PRIMARY KEY,
                description TEXT,
                units TEXT,
                sampling_rate TEXT,
                sensor_type TEXT,
                columns TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 插入数据字典
        for sensor_name, sensor_info in self.sensor_definitions.items():
            self.conn.execute('''
                INSERT OR REPLACE INTO data_dictionary 
                (sensor_name, description, units, sampling_rate, sensor_type, columns)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                sensor_name,
                sensor_info['description'],
                sensor_info['units'],
                sensor_info['sampling_rate'],
                sensor_info['sensor_type'],
                ','.join(sensor_info['columns'])
            ))
        
        self.conn.commit()
        logger.info("数据库表结构创建完成")
    
    def load_metadata(self):
        """加载参与者元数据"""
        metadata_path = self.data_root_path / 'metadata.csv'
        if not metadata_path.exists():
            logger.warning(f"元数据文件不存在: {metadata_path}")
            return
        
        logger.info("加载参与者元数据...")
        try:
            df = pd.read_csv(metadata_path)
            df.to_sql('participants', self.conn, if_exists='replace', index=False)
            self.conn.commit()
            logger.info(f"成功加载 {len(df)} 个参与者的元数据")
        except Exception as e:
            logger.error(f"加载元数据失败: {e}")
    
    def scan_data_files(self):
        """扫描所有数据文件"""
        logger.info("扫描数据文件...")
        data_files = []
        
        # 扫描所有参与者和会话文件夹
        for participant_dir in self.data_root_path.iterdir():
            if not participant_dir.is_dir() or not participant_dir.name.isdigit():
                continue
                
            participant_id = participant_dir.name
            
            for session_dir in participant_dir.iterdir():
                if not session_dir.is_dir() or not session_dir.name.isdigit():
                    continue
                    
                session_id = session_dir.name
                
                # 扫描传感器数据文件
                for sensor_file in session_dir.iterdir():
                    if sensor_file.is_file() and sensor_file.suffix in ['.csv', '.txt']:
                        sensor_name = sensor_file.stem
                        if sensor_name in self.sensor_definitions:
                            data_files.append({
                                'participant_id': participant_id,
                                'session_id': session_id,
                                'sensor_name': sensor_name,
                                'file_path': sensor_file
                            })
        
        logger.info(f"发现 {len(data_files)} 个数据文件")
        return data_files
    
    def process_data_file(self, file_info):
        """处理单个数据文件"""
        try:
            file_path = file_info['file_path']
            sensor_name = file_info['sensor_name']
            participant_id = file_info['participant_id']
            session_id = file_info['session_id']
            
            logger.info(f"处理文件: {file_path}")
            
            # 读取数据文件
            if file_path.suffix == '.csv':
                df = pd.read_csv(file_path)
            else:
                # 尝试不同的分隔符
                try:
                    df = pd.read_csv(file_path, sep='\t')
                except:
                    df = pd.read_csv(file_path, sep=' ')
            
            # 确保列名正确
            expected_columns = self.sensor_definitions[sensor_name]['columns']
            if len(df.columns) != len(expected_columns):
                logger.warning(f"列数不匹配: 期望 {len(expected_columns)}, 实际 {len(df.columns)}")
                # 尝试重命名列
                if len(df.columns) >= len(expected_columns):
                    df.columns = expected_columns[:len(df.columns)]
                else:
                    # 添加缺失的列
                    for col in expected_columns[len(df.columns):]:
                        df[col] = 0
                    df.columns = expected_columns
            else:
                df.columns = expected_columns
            
            # 添加参与者信息
            df['participant_id'] = participant_id
            df['session_id'] = session_id
            
            # 保存到数据库
            df.to_sql(sensor_name, self.conn, if_exists='append', index=False)
            
            logger.info(f"成功处理: {file_path} ({len(df)} 条记录)")
            return len(df)
            
        except Exception as e:
            logger.error(f"处理文件失败 {file_path}: {e}")
            return 0
    
    def extract_all_data(self):
        """抽取所有数据"""
        logger.info("开始数据抽取...")
        
        # 连接到数据库
        self.conn = sqlite3.connect(self.db_path)
        
        try:
            # 创建数据库结构
            self.create_database_schema()
            
            # 加载元数据
            self.load_metadata()
            
            # 扫描数据文件
            data_files = self.scan_data_files()
            
            if not data_files:
                logger.warning("未找到任何数据文件，请检查数据目录结构")
                return
            
            # 处理所有数据文件
            total_records = 0
            for file_info in data_files:
                records = self.process_data_file(file_info)
                total_records += records
            
            self.conn.commit()
            logger.info(f"数据抽取完成！总共处理了 {total_records} 条记录")
            
            # 显示数据库统计
            self.show_database_stats()
            
        finally:
            if self.conn:
                self.conn.close()
    
    def show_database_stats(self):
        """显示数据库统计信息"""
        logger.info("数据库统计信息:")
        
        # 统计每个表的数据量
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            logger.info(f"  {table_name}: {count} 条记录")

def main():
    """主函数"""
    # 配置路径
    data_root_path = "/Users/naluwei/Code/AI-Programming-Education-Suite/data/exercise-06"
    db_path = "/Users/naluwei/Code/AI-Programming-Education-Suite/ai-data-cleaning/fatigueset.db"
    
    # 创建抽取器
    extractor = FatigueSetDataExtractor(data_root_path, db_path)
    
    # 执行数据抽取
    extractor.extract_all_data()

if __name__ == "__main__":
    main()