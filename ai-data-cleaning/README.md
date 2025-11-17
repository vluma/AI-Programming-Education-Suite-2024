# FatigueSet 数据抽取工具

这是一个用于抽取和处理FatigueSet数据集的工具，可以将数据从文件夹结构抽取到SQLite数据库，并提供Web界面进行数据浏览。

## 功能特点

- **数据抽取**：循环处理所有文件夹中的数据文件
- **数据完整性**：保持原有数据字段，添加中文备注
- **多设备支持**：支持4种可穿戴设备的数据处理
- **Web浏览**：提供友好的前端界面浏览SQLite数据
- **数据字典**：内置完整的数据字典和字段说明

## 支持的设备类型

1. **Nokia Bell Labs eSense earable sensor** - 耳戴式传感器
2. **Muse S Headband** - 脑电波头带
3. **Zephyr BioHarness 3.0 chest monitor** - 胸带监测器
4. **Empatica E4 wristband** - 智能手环

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 1. 数据抽取

```bash
python data_extractor.py
```

### 2. 启动Web服务

```bash
python app.py
```

然后在浏览器中访问 `http://localhost:5000`

### 3. 数据库查询

```bash
python database.py
```

## 数据库结构

### 主要表格

- **participants**：参与者元数据
- **esense_***：eSense传感器数据
- **muse_***：Muse头带数据
- **zephyr_***：Zephyr胸带数据
- **empatica_***：Empatica手环数据
- **data_dictionary**：数据字典

## 文件说明

- `data_extractor.py` - 数据抽取主程序
- `database.py` - 数据库查询工具
- `app.py` - Web应用主程序
- `templates/index.html` - 前端界面模板
- `requirements.txt` - Python依赖包
- `fatigueset.db` - SQLite数据库文件（自动生成）

## 注意事项

- 确保数据文件夹路径正确配置
- 首次运行会自动创建SQLite数据库
- Web服务默认运行在5000端口
- 支持的数据格式：CSV、TXT、XLSX