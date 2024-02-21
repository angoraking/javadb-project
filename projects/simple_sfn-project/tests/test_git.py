# -*- coding: utf-8 -*-

from simple_sfn.git import git_repo


def test():
    _ = git_repo.git_branch_name
    _ = git_repo.semantic_branch_name


if __name__ == "__main__":
    from simple_sfn.tests import run_cov_test

    run_cov_test(__file__, "simple_sfn.git", preview=False)
