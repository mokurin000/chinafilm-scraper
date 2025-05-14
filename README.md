# 国家电影局备案电影爬虫

## 运行程序

### 基于 uv

1. 安装 [uv](https://docs.astral.sh/uv/getting-started/installation/)
2. 在终端中运行 `uv run main.py`

> PyCharm: 终端在左下角或者屏幕下方
>
> VSCode: Ctrl + ``` ` ```

### 其他IDE

识别到 `pyproject.toml` 后，现代IDE应当主动建议建立虚拟环境并安装依赖。

之后运行 `main.py` 程序即可。

## 技术栈

- aiohttp: 发起异步 HTTP 请求
- beautifulsoup4: 解析 html
- loguru: 易用日志库
- polars: 导出表格

## 缓存机制

基于电影页面 URL ，缓存获取到的电影梗概。

`diskcache` 可以提供基于 sqlite3（即，基于 B+树）的缓存机制，避免重复请求，减缓目标服务器压力。

## 遇到的反爬

政府网站几乎不会有任何有意义的反爬机制。目前已知的只有进行 User-Agent 检查，手动指定 User-Agent 即可解决。
