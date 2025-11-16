# GitHub 仓库设置指南

## 手动创建GitHub仓库并推送代码

### 步骤1：在GitHub上创建新仓库

1. 登录你的GitHub账户
2. 点击右上角的 "+" 图标，选择 "New repository"
3. 填写仓库信息：
   - Repository name: `AI-Programming-Education-Suite`
   - Description: `面向编程小白的AI编程教育套件`
   - 选择 Public（公开）或 Private（私有）
   - 不要勾选 "Initialize this repository with a README"
   - 点击 "Create repository"

### 步骤2：将本地仓库连接到GitHub

在终端中执行以下命令（将YOUR_USERNAME替换为你的GitHub用户名）：

```bash
git remote add origin https://github.com/YOUR_USERNAME/AI-Programming-Education-Suite.git
```

### 步骤3：推送代码到GitHub

```bash
git branch -M main
git push -u origin main
```

### 步骤4：验证推送成功

访问 `https://github.com/YOUR_USERNAME/AI-Programming-Education-Suite` 查看你的仓库是否已经成功上传。

## 可选：安装GitHub CLI（用于未来操作）

如果你希望使用GitHub CLI工具，可以通过以下方式安装：

### macOS (使用Homebrew)
```bash
brew install gh
```

### 安装完成后登录
```bash
gh auth login
```

然后你就可以使用gh命令来管理GitHub仓库了。

## 项目结构说明

推送成功后，你的GitHub仓库将包含以下结构：

```
AI-Programming-Education-Suite/
├── .gitignore
├── README.md
├── GITHUB_SETUP.md
├── programming-guide/
│   ├── docs/
│   ├── assets/
│   ├── tutorials/
│   └── examples/
├── ai-data-cleaning/
│   ├── src/
│   ├── models/
│   ├── config/
│   ├── tests/
│   ├── examples/
│   └── docs/
└── ai-code-generator/
    ├── src/
    ├── templates/
    ├── models/
    ├── config/
    ├── tests/
    ├── examples/
    └── docs/
```

## 后续开发

现在你可以开始开发各个模块了。建议的开发顺序：

1. 首先完善编程入门文档模块
2. 然后开发AI数据清洗模块
3. 最后实现AI课题代码生成模块

每个模块都应该有自己的README.md文件和详细的文档。