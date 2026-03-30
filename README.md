# pypang

基于百度网盘开放平台接口实现,百度网盘命令行客户端，参考了 [bypy](https://github.com/houtianze/bypy) 的使用习惯，并补充了本地 Web UI 与纯 CLI 两套操作方式。

项目目标：

- 适配最新百度网盘开放平台接口
- 默认兼容 `bypy` 的授权配置与应用目录
- 支持无图形界面的服务器环境
- 同时提供 Web UI 和 CLI 上传下载能力

## 功能

- OAuth 授权，默认使用 `redirect_uri=oob`
- 兼容 `bypy` 默认应用信息
- 自动读取根目录 `config.json` 作为自定义配置
- 支持多配置下分别保存 token
- Web UI 文件浏览、上传、下载、重命名、移动、删除、建目录
- CLI 文件浏览、上传、下载、重命名、移动、删除、建目录
- 支持服务器端口映射后远程访问 Web UI
- Web UI 传输监控支持 `进行中 / 等待中 / 已完成` 三栏显示
- 超过百度开放平台单文件上限时自动按卷上传，卷名追加 `.001`、`.002` 等后缀

## 默认行为

如果没有 `config.json`：

- 默认使用 `bypy` 的应用配置
- 默认应用目录为 `/apps/bypy`
- 默认授权方式为 `oob`
- 默认按无会员档位运行；上传分片默认跟随账号身份自动取最高可用值，CLI/Web 下载并发默认取 `8`
- 单文件下载默认开启分段并发，默认连接数为 `4`，不支持 `Range` 的链路会自动回退为单连接下载

如果存在 `config.json`：

- 会自动解析其中的应用配置
- Web UI 下拉列表中可切换预设
- 每套配置分别保存自己的 token

## 支持的 `config.json` 格式

多应用格式：

```json
{
  "default": "myapp",
  "apps": [
    {
      "id": "bypy",
      "label": "bypy",
      "app_key": "q8WE4EpCsau1oS0MplgMKNBn",
      "secret_key": "PA4MhwB5RE7DacKtoP2i8ikCnNzAqYTD",
      "app_name": "bypy",
      "app_root": "/apps/bypy"
    },
    {
      "id": "myapp",
      "label": "我的应用",
      "app_key": "your-app-key",
      "secret_key": "your-secret-key",
      "app_name": "myapp",
      "app_root": "/apps/myapp"
    }
  ]
}
```

## 安装

### 从 PyPI 安装

```powershell
pip install pypang
```

安装后可直接使用：

```powershell
pypang serve
```

### 从源码安装

```powershell
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 首次配置

通过 `pip install pypang` 安装后，不需要修改 `site-packages` 里的文件。

推荐方式是直接使用命令写入本地运行配置：

```powershell
pypang config set --app-key your-app-key --secret-key your-secret-key --app-name myapp --app-root /apps/myapp --redirect-uri oob
```

配置会保存到本地运行状态文件中，默认路径为：

```text
~/.pypang/state.json
```

上传过程中用到的临时文件默认保存在：

```text
~/.pypang/tmp
```

如果你更习惯使用 `config.json`，可以在当前运行目录手动放置一个文件，例如：

```json
{
  "default": "myapp",
  "apps": [
    {
      "id": "myapp",
      "label": "我的应用",
      "app_key": "your-app-key",
      "secret_key": "your-secret-key",
      "app_name": "myapp",
      "app_root": "/apps/myapp"
    }
  ]
}
```

程序会优先读取当前工作目录下的 `config.json`。如果需要指定其他位置，可使用环境变量：

```powershell
$env:BAIDUPANWEB_LEGACY_CONFIG = "D:\\path\\to\\config.json"
```

## CLI 使用

### 1. 查看当前配置

```powershell
pypang config show
```

### 2. 设置配置

默认走 OOB 授权：

```powershell
pypang config set --app-name bypy --app-root /apps/bypy --redirect-uri oob
```

如果只想修改监听地址而不影响 token：

```powershell
pypang config set --listen-host 0.0.0.0 --listen-port 8080
```

如果需要启用会员能力对应的更大分片和更高下载并发，可额外设置：

```powershell
pypang config set --membership-tier vip --upload-chunk-mb 16 --cli-download-workers 4 --web-download-workers 4
```

如果要调整单文件并发下载：

```powershell
pypang config set --single-file-parallel-enabled --single-file-download-workers 4
```

### 3. 获取授权链接

```powershell
pypang auth url
```

浏览器打开后，百度会回到默认成功页：

```text
http://openapi.baidu.com/oauth/2.0/login_success
```

从页面中复制 `code`，再执行：

```powershell
pypang auth code <code>
```

### 4. 常用 CLI 命令

查看用户信息：

```powershell
pypang whoami
```

查看容量：

```powershell
pypang quota
```

列出目录：

```powershell
pypang ls /
```

创建目录：

```powershell
pypang mkdir /apps/bypy/docs
```

上传文件：

```powershell
pypang put .\local.txt /apps/bypy/docs/remote.txt
```

上传目标既可以是服务器文件，也可以是服务器目录：

```powershell
pypang put .\local.txt /apps/bypy/docs/
pypang put .\local.txt /apps/bypy/docs/remote.txt
```

下载文件：

```powershell
pypang get /apps/bypy/docs/remote.txt .\downloads\
```

下载命令默认支持断点续传；如果希望强制重新开始，可追加 `--no-resume`。

单文件下载默认会尝试分段并发；如果某次下载希望临时关闭，可追加：

```powershell
pypang get /apps/bypy/docs/remote.txt .\downloads\ --no-single-file-parallel
```

重命名：

```powershell
pypang rename /apps/bypy/docs/remote.txt archive.txt
```

移动：

```powershell
pypang mv /apps/bypy/docs/archive.txt /apps/bypy/history
```

删除：

```powershell
pypang rm /apps/bypy/history/archive.txt
```

命令别名：

- `ls` = `list`
- `put` = `upload`
- `get` = `download`
- `mv` = `move`
- `rm` = `delete`
- `whoami` = `info`

## Web UI 使用

### 本机启动

```powershell
pypang serve
```

或指定地址：

```powershell
pypang serve --host 127.0.0.1 --port 8080
```

浏览器访问：

```text
http://127.0.0.1:8080
```

### 无图形界面服务器启动

服务器上可直接对外监听：

```powershell
pypang config set --listen-host 0.0.0.0 --listen-port 8080
pypang serve
```

然后通过公网 IP、内网穿透、反向代理或 SSH 端口转发访问：

```text
http://服务器地址:8080
```

适合以下场景：

- Linux/Windows 无桌面服务器
- 家庭 NAS
- 云服务器
- 通过端口映射远程进入 Web UI 后上传下载文件

### Web UI 授权流程

当前默认使用 `oob`，所以即使服务器没有桌面环境也能工作：

1. 从本地浏览器打开 Web UI
2. 点击授权按钮
3. 在百度返回页复制 `code`
4. 回到 Web UI 手动提交 `code`
5. 授权完成后即可在网页中上传、下载和管理文件

## 主要接口适配

上传流程按最新文档实现：

```text
locateupload -> precreate -> superfile2 -> create
```

下载流程按最新文档实现：

```text
filemetas(dlink=1) -> dlink + access_token + User-Agent
```

命令行下载和 Web UI 的“保存到服务器”默认会在本地生成 `.part` 临时文件，并基于 `Range` 请求自动续传未完成部分。

当前下载恢复与校验行为：

- 下载任务开始前会先扫描目标目录，检查已有正式文件和 `.part` 文件
- 如果本地文件大小已满足远端大小，会优先做校验；校验通过则直接复用，不重复下载
- 如果校验失败，会删除错误文件后重新下载
- 远端 `md5` 合法时使用 MD5 校验；如果远端未提供合法 `md5`，则退回到大小校验
- Web UI 的“传输”卡片会显示 `进行中 / 等待中 / 已完成` 三栏；校验阶段会显示为 `校验中`

当前上传行为：

- 单文件未超过平台上限时，按现有单文件流程上传
- 单文件超过平台上限时，会在远端自动创建一个 `原文件名.parts` 目录，再按当前账号档位切割为多卷后顺序上传
- 多卷命名规则为原文件名后追加 `.001`、`.002`、`.003` 等后缀，例如 `movie.mkv.parts/movie.mkv.001`
- 多卷目录中会额外写入一个 `extract.sh`，下载后可在目录内执行它将所有卷顺序合并回原文件，并对常见压缩格式自动解压；解压成功后会删除分片，保留合并后的压缩文件
- 分卷阈值遵循百度网盘开放平台当前文档：普通用户 `4GB`、普通会员 `10GB`、超级会员 `20GB`

后端日志默认会输出下载与校验关键阶段，例如：

- 下载任务开始
- 文件开始下载
- 发现可复用文件并开始校验
- MD5 校验通过或失败
- 文件完成或任务失败

## 项目结构

```text
pypang/
  app.py
  cli.py
  client.py
  config.py
  storage.py
  static/
  templates/
app.list.json
config.json
README.md
requirements.txt
```

## 参考

- bypy: https://github.com/houtianze/bypy
- 百度网盘开放平台文档索引: https://pan.baidu.com/union/doc/nksg0sbfs
- OAuth 授权码模式: https://pan.baidu.com/union/doc/al0rwqzzl
- 文件列表: https://pan.baidu.com/union/doc/nksg0sat9
- 文件信息: https://pan.baidu.com/union/doc/Fksg0sbcm
- 上传域名: https://pan.baidu.com/union/doc/Mlvw5hfnr
- 预上传: https://pan.baidu.com/union/doc/3ksg0s9r7
- 分片上传: https://pan.baidu.com/union/doc/nksg0s9vi
- 创建文件: https://pan.baidu.com/union/doc/rksg0sa17
- 下载文件: https://pan.baidu.com/union/doc/pkuo3snyp
- 管理文件: https://pan.baidu.com/union/doc/mksg0s9l4
