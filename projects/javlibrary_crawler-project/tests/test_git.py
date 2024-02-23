# -*- coding: utf-8 -*-

from javlibrary_crawler.git import git_repo


def test():
    _ = git_repo.git_branch_name
    _ = git_repo.semantic_branch_name


if __name__ == "__main__":
    from javlibrary_crawler.tests import run_cov_test

    run_cov_test(__file__, "javlibrary_crawler.git", preview=False)
