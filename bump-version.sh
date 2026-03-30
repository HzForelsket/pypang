#!/bin/bash

# 自动增加版本号和提交的脚本
# 使用方法: ./bump-version.sh [patch|minor|major]

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 版本类型，默认为patch
VERSION_TYPE="${1:-patch}"

# 验证版本类型
if [[ ! "$VERSION_TYPE" =~ ^(patch|minor|major)$ ]]; then
    echo -e "${RED}❌ 错误：版本类型必须是 patch、minor 或 major${NC}"
    echo "使用方法: $0 [patch|minor|major]"
    exit 1
fi

# 获取当前版本
CURRENT_VERSION=$(grep -oP 'version = "\K[^"]+' pyproject.toml)
echo -e "${YELLOW}当前版本: ${CURRENT_VERSION}${NC}"

# 解析版本号
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# 计算新版本号
case $VERSION_TYPE in
    patch)
        PATCH=$((PATCH + 1))
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        ;;
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
echo -e "${GREEN}新版本: ${NEW_VERSION}${NC}"

# 检查是否有未提交的更改（除了bump脚本本身）
if ! git diff --quiet; then
    echo -e "${RED}❌ 错误：存在未提交的更改，请先提交或撤销${NC}"
    exit 1
fi

# 更新 pyproject.toml
echo -e "${YELLOW}更新 pyproject.toml...${NC}"
sed -i "s/version = \"${CURRENT_VERSION}\"/version = \"${NEW_VERSION}\"/" pyproject.toml

# 更新 pypang/__init__.py
echo -e "${YELLOW}更新 pypang/__init__.py...${NC}"
sed -i "s/__version__ = \"${CURRENT_VERSION}\"/__version__ = \"${NEW_VERSION}\"/" pypang/__init__.py

# 检查错误
if ! grep -q "version = \"${NEW_VERSION}\"" pyproject.toml; then
    echo -e "${RED}❌ 更新 pyproject.toml 失败${NC}"
    exit 1
fi

if ! grep -q "__version__ = \"${NEW_VERSION}\"" pypang/__init__.py; then
    echo -e "${RED}❌ 更新 pypang/__init__.py 失败${NC}"
    exit 1
fi

# 提交更改
echo -e "${YELLOW}提交版本更新...${NC}"
git add pyproject.toml pypang/__init__.py
git commit -m "release: v${NEW_VERSION}"

# 创建tag
echo -e "${YELLOW}创建tag: v${NEW_VERSION}...${NC}"
git tag -a "v${NEW_VERSION}" -m "release: v${NEW_VERSION}"

echo -e "${GREEN}✅ 成功！${NC}"
echo ""
echo "版本更新完成:"
echo "  旧版本: ${CURRENT_VERSION}"
echo "  新版本: ${NEW_VERSION}"
echo ""
echo "下一步，可以运行以下命令推送到远程仓库:"
echo "  git push pypang master"
echo "  git push pypang v${NEW_VERSION}"
echo ""
echo "或运行以下命令推送所有内容:"
echo "  git push pypang master --tags"
