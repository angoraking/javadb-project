Run Crawler on GitHub Action
==============================================================================


Overview
------------------------------------------------------------------------------
通常这种爬虫项目应该以容器或是 AWS Lambda 的形式部署在服务器上运行. 但是为了省钱, 我决定把整个 GitHub Repo 开源, 这样就能免费使用 GitHub Action, 让 crawler 运行在 CI Job 中.

下面我们来仔细探讨一下用 GitHub 来跑爬虫程序是否可行呢?


可行性分析
------------------------------------------------------------------------------
**GitHub Action 对 Public Repo 是否完全免费**

是的, 官方 `About billing for GitHub Actions <https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions>`_ 一文中说了:

    GitHub Actions usage is free for standard GitHub-hosted runners in public repositories, and for self-hosted runners.

**GitHub Action 对 Public Repo 是否有总运行时间的限制**

没有, 根据 GitHub community 里的这个讨论 `Is there a limit on usage by public repos? <https://github.com/orgs/community/discussions/70492>`_ 提到了, GitHub 官方确认了 "Public repos do not have any minute limitation".

**Public Repo 的 GitHub Action runner 机器性能够用吗**

绝对够用, 根据官方 `Standard GitHub-hosted runners for Public repositories <https://docs.github.com/en/actions/using-github-hosted-runners/about-github-hosted-runners/about-github-hosted-runners#standard-github-hosted-runners-for-public-repositories>`_ 一文, Linux 的 ubuntu 有 4vCPU, 16GB 内存, 绝对够用了.

**GitHub Action 能使用 AWS 服务吗**

当然可以.

**我可以用 API 来运行, 停止, 查看 GitHub Action Workflow run 吗**

如果有这个功能, 那么我们就可以自己写编排程序来对 Workflow run 进行管理了.

- 运行 workflow: 可以, 请参考 `Create a workflow dispatch event <https://docs.github.com/en/rest/actions/workflows?apiVersion=2022-11-28#create-a-workflow-dispatch-event>`_ 这个 API 的文档. 这个 API 支持指定自定义的 input parameter 参数. 这个 API 可以返回 run id, 你可以用这个 run id 来查看这个 workflow run 的状态.
- 获得 workflow run 的状态: 可以, 请参考 `Get a workflow run <https://docs.github.com/en/rest/actions/workflow-runs?apiVersion=2022-11-28#get-a-workflow-run>`_ 这个 API 的文档.
- 手动停止 workflow run 的状态: 可以, 请参考 `Cancel a workflow run <https://docs.github.com/en/rest/actions/workflow-runs?apiVersion=2022-11-28#cancel-a-workflow-run>`_ 这个 API 的文档.
- 列出所有属于某个 workflow 的 workflow run: 可以, 请参考 `List workflow runs for a workflow <https://docs.github.com/en/rest/actions/workflow-runs?apiVersion=2022-11-28#list-workflow-runs-for-a-workflow>`_ 这个 API 的文档.


Crawler Orchestration
------------------------------------------------------------------------------
爬虫的本质是从一堆 todo URL list 中拿出一个 URL, 然后从对应的 HTMl 里面提取数据. 而很多爬虫项目都是分布式的. 如果要用 GitHub Action 来运行爬虫, 这里一个关键的问题就是如何避免多个并行爬虫同时对一个 URL 进行抓取.

下面是我的解决方案:

1. 同一时间只运行一个爬虫, 完全不并行执行. 因为对目标网站大量的并发请求会造成网站挂掉, 同时也可能会被封 IP. 所以并行运行多个爬虫是不好的. 我们索性就使用一个爬虫, 虽然慢一点, 只要爬取的速度能跟上网站更新的速度就够了.
2. 使用数据库来进行 Status tracking, 每个 URL 在数据库中就是一条 Record. 当对 URL 进行抓取时我们对其进行上锁, 并用数据库记录抓取是否成功.