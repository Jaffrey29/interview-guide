# Notes

学习 [Snailclimb/interview-guide](https://github.com/Snailclimb/interview-guide) 项目过程中整理的笔记，与代码在同一个 fork 仓库里，方便对照阅读。

## Mac 拉取

```bash
git clone https://github.com/Jaffrey29/interview-guide.git
cd interview-guide/Notes
```

要从原作者同步最新代码：

```bash
git remote add upstream https://github.com/Snailclimb/interview-guide.git
git fetch upstream
git merge upstream/master   # 或 git rebase upstream/master
```

## 目录

- [环境搭建.md](环境搭建.md)
- [Docker.md](Docker.md)
- [5. 多LLM实战.md](5.%20多LLM实战.md)
- [模拟面试.md](模拟面试.md)
- [简历管理.md](简历管理.md)
- [RAG.md](RAG.md)

笔记风格：接口复现式讲解，按 Controller -> Service -> Repository 的链路读代码。
