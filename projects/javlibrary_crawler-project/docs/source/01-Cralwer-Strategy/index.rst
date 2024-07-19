Crawler Strategy
==============================================================================


Architecture
------------------------------------------------------------------------------
整个架构分三大块:

1. GitHub Action: 我们的爬虫程序就跑在 GitHub Action 中. 主要任务是从目标网站上下载 HTML 和图片.
2. AWS: 我们的云计算资源都在 AWS 上. 包括数据库, 对象, 文件存储.
3. Laptop: 我们的数据清洗, 搜索数据库, 最终的 GUI App 都跑在本地电脑上.

下面我们一块一块的详细展开说:

1.1 我们的爬虫程序运行环境是在 GitHub Action 中的 Ubuntu runner 中的. 详情请参考 `javlibrary_crawler.yml <https://github.com/angoraking/javadb-project/blob/main/.github/workflows/javlibrary_crawler.yml>`_
1.2 我们的爬虫本质上就是一个 Python 程序. 它会
2.1 下载下来的 HTML 会根据 URL 自动生成一个 S3 location, 并将压缩后的 HTML 存在 S3 上.
2.2 我们会使用 DynamoDB 作为 Status Tracking, 来跟踪每一个 URL 是否被成功下载了. 我们会对下载后的 HTML 进行基本的检查, 判断是不是一个包含数据的 HTML. 避免下载到 404 的文件.






Downloading and Extraction
------------------------------------------------------------------------------
在爬虫项目中, 除了从目标网站上下载 HTML 和图片数据的步骤, 其他步骤例如 提取数据, 清洗数据, 整理数据, 等本质上都可以离线进行. 所以在工程上, 我们的核心策略是将比较复杂的下载步骤和其他步骤分开.

1. 从 sitemap 下载所有待爬 URL 列表, 并将其存入 Status Tracking 数据库.
2. 爬虫逐一下载 URL 对应的 HTMl, 将 HTML 压缩后存入 S3 并更新 Status Tracking 数据库.
3. 后续的数据清洗步骤在下一节介绍.


Cloud Database and Local Database
------------------------------------------------------------------------------
由于这里面最耗时, 消耗计算资源的步骤就是下载 HTML, 所以我们选择用对开源 Repo 免费 GitHub Action 来运行我们的爬虫程序. 详情请参考 :ref:`run-crawler-on-github-action`. 由于爬虫运行在 GitHub Action 中, 所以 Status Tracking 数据库必须在云端. 这里我们选用了 DynamoDB 作为我们的 Status Tracking 数据库,

而后续的数据清洗步骤都是离线进行, 那么完全可以使用本地的 sqlite 数据库. 这样能大大节约成本.
