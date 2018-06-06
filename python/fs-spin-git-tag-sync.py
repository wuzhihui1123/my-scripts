import copy
import json
import os
import time
from urllib.parse import urlparse

import gitlab
import requests


class Constant:
    ENV_TASK_PERIOD_SECOND = "TASK_PERIOD_SECOND"
    ENV_GITLAB_HOST = "GITLAB_HOST"
    ENV_GITLAB_PRIVATE_TOKEN = "GITLAB_PRIVATE_TOKEN"
    ENV_SPINNAKER_API_HOST = "SPINNAKER_API_HOST"
    ENV_SPINNAKER_USERNAME = "SPINNAKER_USERNAME"
    ENV_SPINNAKER_PASSWORD = "SPINNAKER_PASSWORD"
    ENV_SPINNAKER_PARAM_GIT_URL_NAME = "SPINNAKER_PARAM_GIT_URL_NAME"
    ENV_SPINNAKER_PARAM_BRANCH_OR_TAG = "SPINNAKER_PARAM_BRANCH_OR_TAG"


class GitLabApi:
    url = os.getenv(Constant.ENV_GITLAB_HOST, "<hidden>")
    private_token = os.getenv(Constant.ENV_GITLAB_PRIVATE_TOKEN, "<hidden>")
    client = None

    @staticmethod
    def login():
        """
        登录
        :return:
        """
        GitLabApi.client = gitlab.Gitlab(GitLabApi.url, private_token=GitLabApi.private_token)

    @staticmethod
    def get_project_path(project_url):
        project_path = urlparse(project_url).path
        if project_path.endswith(".git"):
            project_path = project_path[:len(project_path) - 4]
        if project_path.startswith("/"):
            project_path = project_path[1:]
        return project_path

    @staticmethod
    def get_branches(project_url):
        """
        获取所有分支
        :param project_url:
        :return:
        """
        project_path = GitLabApi.get_project_path(project_url)
        project = GitLabApi.client.projects.get(project_path)
        branch_obj_list = project.branches.list(page=1, per_page=100)
        return list(map(lambda branch: branch.name, branch_obj_list))

    @staticmethod
    def get_tags(project_url):
        project_path = GitLabApi.get_project_path(project_url)
        project = GitLabApi.client.projects.get(project_path)
        tags_obj_list = project.tags.list(page=1, per_page=100)
        return list(map(lambda branch: branch.name, tags_obj_list))


class SpinnakerGateApi:
    url = os.getenv(Constant.ENV_SPINNAKER_API_HOST, "<hidden>")
    username = os.getenv(Constant.ENV_SPINNAKER_USERNAME, "<hidden>")
    password = os.getenv(Constant.ENV_SPINNAKER_PASSWORD, "<hidden>")
    param_of_git_url = os.getenv(Constant.ENV_SPINNAKER_PARAM_GIT_URL_NAME, "git_url")
    param_of_branch_or_tag = os.getenv(Constant.ENV_SPINNAKER_PARAM_BRANCH_OR_TAG, "branch_or_tag")
    session = requests.Session()

    @staticmethod
    def login():
        """
        登录
        :return:
        """
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        params = {
            "username": SpinnakerGateApi.username,
            "password": SpinnakerGateApi.password,
            "submit": "Login"
        }
        SpinnakerGateApi.session.post(SpinnakerGateApi.url + "/login", params=params, headers=headers)

    @staticmethod
    def get_all_applications():
        """
        获取所有的application，包含spinnaker从kubernetes自动抓取的app
        :return:
        """
        resp = SpinnakerGateApi.session.get(SpinnakerGateApi.url + "/applications", timeout=5)
        return resp.json()

    @staticmethod
    def get_created_applications():
        """
        获取在spinnaker上被用户创建的application
        :return:
        """
        all_apps = SpinnakerGateApi.get_all_applications()
        # 用户创建的app包含一些属性，比如 创建时间（createTs），创建人邮箱（email）等，基于此进行过滤
        return list(filter(lambda x: "createTs" in x, all_apps))

    @staticmethod
    def get_pipelines(app_name):
        resp = SpinnakerGateApi.session.get(
            SpinnakerGateApi.url + "/applications/{app}/pipelineConfigs".format(app=app_name), timeout=5)
        return resp.json()

    @staticmethod
    def update_pipeline(data):
        """
        更新Pipeline
        :param data: pipeline 数据
        :return:
        """
        headers = {
            "Content-Type": "application/json;charset=UTF-8"
        }
        resp = SpinnakerGateApi.session.post(SpinnakerGateApi.url + "/pipelines", json=data, headers=headers,
                                             timeout=20)
        if resp.status_code != 200:
            raise Exception("pipeline update, status: {status_code}, response: {response_body}"
                            .format(status_code=resp.status_code, response_body=resp.text))


def update_pipeline_param_of_git(pipeline):
    """
    更新pipeline下对应参数中的git branch 和 tag 选项值
    :param pipeline:
    :return: （status,message）
    """
    status = "fail"
    message = ""

    class SkipException(Exception):
        def __init__(self, err):
            Exception.__init__(self, err)

    try:
        if "parameterConfig" not in pipeline:
            raise SkipException("pipeline attribute[parameterConfig] not exist")

        params = pipeline["parameterConfig"]

        def __get_param(param_name):
            ret = list(filter(lambda x: x["name"] == param_name, params))
            return ret[0] if len(ret) == 1 else None

        param_1 = SpinnakerGateApi.param_of_git_url
        param_2 = SpinnakerGateApi.param_of_branch_or_tag
        param_git_url = __get_param(param_1)
        param_branch_or_tag = __get_param(param_2)
        if not param_git_url:
            raise SkipException("pipeline/parameterConfig [{0}] not exist".format(param_1))
        if not param_branch_or_tag:
            raise SkipException("pipeline/parameterConfig [{0}] not exist".format(param_2))

        project_url = param_git_url["default"]
        if not project_url or not project_url.strip():
            raise SkipException("pipeline/parameterConfig [{0}] no default value".format(param_1))
        branches = GitLabApi.get_branches(project_url)
        tags = GitLabApi.get_tags(project_url)
        branches = list(map(lambda x: "branch-" + x, branches))
        options = branches + tags
        options = list(map(lambda x: {"value": x}, options))
        if not param_branch_or_tag["options"] == options:
            param_branch_or_tag["options"] = options
            SpinnakerGateApi.update_pipeline(pipeline)
            status = "success"
        else:
            raise SkipException("pipeline/parameterConfig [{0}] options value not change".format(param_2))
    except SkipException as se:
        status = "skip"
        message = str(se)
    except Exception as ex:
        # traceback.print_exc()
        message = str(ex)
    return status, message


if __name__ == "__main__":
    while True:
        print("=================  sync pipeline parameter start, time: {time} ================= ".format(
            time=time.strftime('%Y-%m-%d %H:%M:%S')))
        try:
            SpinnakerGateApi.login()
            GitLabApi.login()
            apps = SpinnakerGateApi.get_created_applications()
            print("|-- applications: {0}".format(json.dumps(apps)))
            for app in apps:
                try:
                    app_name = app["name"]
                    pipelines = SpinnakerGateApi.get_pipelines(app_name)
                    for pipeline in pipelines:
                        pipeline_name = pipeline["name"]
                        old = copy.deepcopy(pipeline)
                        sync_status, sync_message = update_pipeline_param_of_git(pipeline)
                        old_json = json.dumps(old) if not sync_status == "skip" else ""
                        new_json = json.dumps(pipeline) if sync_status == "success" else ""
                        print("|---- {app_name}/{pipe_name}/update {status} | {message} | old: {old} | new: {new}"
                              .format(app_name=app_name, pipe_name=pipeline_name, status=sync_status,
                                      message=sync_message, old=old_json, new=new_json))
                except Exception as ex1:
                    print(ex1)
        except Exception as ex2:
            print(ex2)
        print("===========================  sync pipeline parameter over ===========================")
        sleep_time = int(os.getenv(Constant.ENV_TASK_PERIOD_SECOND, 600))
        time.sleep(sleep_time)
