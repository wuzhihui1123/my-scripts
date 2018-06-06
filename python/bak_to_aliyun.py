#!/usr/bin/python
# -*- coding: utf-8 -*-
import math
import os
import smtplib
import subprocess
import time
import traceback
from email.mime.text import MIMEText
from tarfile import TarFile

import oss2
import shutil
from oas.ease.vault import Vault
from oas.oas_api import OASAPI

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
# 日志文件
__log_file__ = os.path.join(CURR_DIR, '<hidden>.log')

# 阿里云的一些配置信息
__aliyun__ = {
    'access_key': '<hidden>',
    'access_secret': '<hidden>',
    'oss_endpoint': 'http://oss-cn-beijing.aliyuncs.com',
    'oss_bucket': '<hidden>',
    'oas_server': 'cn-hangzhou.oas.aliyuncs.com',
    'oas_vault': '<hidden>'
}

__backup__ = {
    'web': ['/var/www/', '/etc/apache2/'],
    'database': ['/var/lib/mysql/', '/etc/mysql/my.cnf']
}

# 备份文件加密密码
__archive_password__ = "<hidden>"

# 邮件SMTP信息
__email_smtp__ = {
    'server': "smtp.163.com",
    'port': 25,
    'username': '<hidden>',
    'password': '<hidden>'
}
# 邮件内容模板
__email_template__ = u'''
    <p><b>备份信息：</b>     {bak_message}</p>
    <p><b>备份服务器：</b>     <hidden></p>
    <p><b>备份状态：</b>     {bak_status}</p>
    <p><b>备份日期：</b>     {bak_time}</p>
'''


def write_log(content):
    with open(__log_file__, 'a') as log_file:
        content = "{curr_time}  : {content}\n".format(curr_time=time.strftime('%Y-%m-%d %H:%M:%S'), content=content)
        log_file.write(content)
        print(content)


def human_size(nbytes):
    """
    获取阅读友好的容量尺度
    :param nbytes:
    :return:
    """
    try:
        suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        if nbytes == 0:
            return '0 B'
        i = 0
        while nbytes >= 1024 and i < len(suffixes) - 1:
            nbytes /= 1024.
            i += 1
        f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
        return '%s %s' % (f, suffixes[i])
    except:
        return 'un-know'


def log_exception(func):
    """
    记录异常到日志的装饰器函数， 在函数定义前面实用@log_exception 来装饰函数， 这样函数的异常信息会记录到日志文件中
    :param func:
    :return:
    """

    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except Exception, e:
            exc_track_info = traceback.format_exc()
            write_log(exc_track_info)

            subject = u'<hidden>数据备份失败'
            content = __email_template__.format(bak_message=u'服务器数据备份失败', bak_status=u'失败', bak_time=time.strftime('%Y-%m-%d %H:%M'))
            content = content + u'''
                <p><b>错误信息：</b><pre style='padding:10px;background-color:#eee'>{err_info}</pre></p>
            '''.format(err_info=exc_track_info)
            send_email(subject, content, '<hidden>')
            raise e

    return wrapper


@log_exception
def tar_gz_file(name, file_path_list):
    """
    打包并且压缩(gz)文件
    :param name: 打包文件名（不包含路径则打包当前脚本目录下）
    :param file_path_list: 需要打包文件的全路径名所组成的数组
    :return:
    """
    if not isinstance(file_path_list, list):
        raise TypeError('parameter [file_path_list] must be list, current type:{type}'.format(type=type(file_path_list)))
    for file_path in file_path_list:
        if not os.path.exists(file_path):
            raise Exception('file is not exist: {file_path}'.format(file_path=file_path))

    start_time = time.time()
    tarfile = TarFile.open(name, "w:gz")
    for file_path in file_path_list:
        tarfile.add(file_path)
    tarfile.close()
    use_time = math.floor(time.time() - start_time)

    message = "tar and gz file [{file_name}], use seconds:  {second}s".format(file_name=name, second=use_time)
    write_log(message)


@log_exception
def zip_file(name, file_path_list, password=None):
    """
    用zip打包并加密文件 （python的zipfile模块不能对创建的zip文件进行加密，这里采用7zip创建并加密zip文件）
    :param name: 打包文件名（不包含路径则打包当前脚本目录下）
    :param file_path_list: 需要打包文件的全路径名所组成的数组
    :param password: 加密密码
    :return:
    """
    if not isinstance(file_path_list, list):
        raise TypeError('parameter [file_path_list] must be list, current type:{type}'.format(type=type(file_path_list)))
    for file_path in file_path_list:
        if not os.path.exists(file_path):
            raise Exception('file is not exist: {file_path}'.format(file_path=file_path))

    start_time = time.time()
    os_command = ['7z', 'a', '-y']
    if password:
        os_command.append("-p{password}".format(password=password))
    os_command.append(name)

    for file_path in file_path_list:
        os_command.append(file_path)

    rc = subprocess.call(os_command)
    use_time = math.floor(time.time() - start_time)
    message = "zip file [{file_name}], use seconds:  {second}s".format(file_name=name, second=use_time)
    write_log(message)


@log_exception
def upload_to_aliyun_oss(file_path):
    """
    上传文件到阿里云对象存储服务器(oss)
    :param file_path: 文件路径名
    :return:
    """
    if not os.path.exists(file_path):
        raise Exception('file is not exist: {file_path}'.format(file_path=file_path))

    start_time = time.time()
    auth = oss2.Auth(__aliyun__.get('access_key'), __aliyun__.get('access_secret'))
    bucket = oss2.Bucket(auth, __aliyun__.get('oss_endpoint'), __aliyun__.get('oss_bucket'))
    with open(file_path, 'rb') as f:
        bucket.put_object(os.path.basename(f.name), f)

    use_time = math.floor(time.time() - start_time)
    message = "upload file to aliyun oss [{file_name}], use seconds:  {second}s".format(file_name=file_path, second=use_time)
    write_log(message)


@log_exception
def upload_to_aliyun_oas(file_path, desc=None):
    """
    上传文件到阿里云归档存储服务器(oss)
    :param file_path: 文件路径名
    :param desc:  归档文件的描述
    :return:  文件归档id
    """
    if not os.path.exists(file_path):
        raise Exception('file is not exist: {file_path}'.format(file_path=file_path))

    start_time = time.time()
    desc = desc if desc else os.path.basename(file_path)
    api = OASAPI(__aliyun__.get('oas_server'), __aliyun__.get('access_key'), __aliyun__.get('access_secret'))
    vault = Vault.create_vault(api, __aliyun__.get('oas_vault'))
    if not os.path.isfile(file_path):
        raise Exception("file is not exist [file]".format(file=file_path))
    size = os.path.getsize(file_path)
    if size < (200 * 1024 * 1024):
        archive_id = vault.upload_archive(file_path, desc)
    else:
        uploader = vault.initiate_uploader(file_path, desc)
        archive_id = uploader.start()

    use_time = math.floor(time.time() - start_time)
    message = "upload file to aliyun oas [{file_name}], use seconds:  {second}s".format(file_name=file_path, second=use_time)
    write_log(message)

    return archive_id


def send_email(subject, content, to_addrs):
    """
    邮件发送
    :param subject: 主题
    :param content: 内容
    :param to_addrs: 收件人列表
    :return:
    """
    if not isinstance(to_addrs, list):
        to_addrs = [to_addrs]

    msg = MIMEText(content, 'html', 'utf-8')
    msg['From'] = __email_smtp__.get('username')
    msg['To'] = ' , '.join(to_addrs)
    msg['Subject'] = subject

    server = smtplib.SMTP(__email_smtp__.get('server'), __email_smtp__.get('port'))
    server.login(__email_smtp__.get('username'), __email_smtp__.get('password'))
    server.sendmail(__email_smtp__.get('username'), to_addrs, msg.as_string())
    server.quit()

    message = "send email to {to_addrs}".format(to_addrs=str(to_addrs))
    write_log(message)


if __name__ == "__main__":
    write_log("=========================== start : backup data ==========================")

    data_dir = os.path.join(CURR_DIR, 'data_dir')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    archive_file_list = []

    try:
        bak_time = time.strftime('%Y%m%d%H%M')

        # 1，打包备份文件
        for archive_name, files in __backup__.items():
            archive_name = os.path.join(data_dir, '{base_name}_{bak_time}.tar.gz'.format(base_name=archive_name, bak_time=bak_time))
            tar_gz_file(archive_name, files)
            archive_file_list.append(archive_name)

        # 2，打包并加密所有的已打包的备份文件
        bak_file = os.path.join(data_dir, '<hidden>_bak_{bak_time}.zip'.format(bak_time=bak_time))
        zip_file(bak_file, archive_file_list, password=__archive_password__)

        # 3、上传到阿里云对象存储（oss）服务器。
        # 备注：当前阿里云的归档存储服务没有提供归档文件相应的查看和下载界面，所以这里用oss来存储备份文件，
        # 而且oss具有很好的生命周期规则可以设置，从而不需要自己来控制数据的过期删除。
        upload_to_aliyun_oss(bak_file)

        # 4、对备份状态进行邮件通知
        subject = u'<hidden>数据备份成功'
        content = __email_template__.format(bak_message=u'服务器数据已经备份并上传到阿里云对象存储（OSS）服务器上', bak_status=u'成功',
                                            bak_time=time.strftime('%Y-%m-%d %H:%M'))
        content = content + u'''
            <p><b>备份文件名：</b>   {bak_file}</p>
            <p><b>文件大小：</b>   {file_size}</p>
            <p><b>阿里云位置：</b>   {server_type} / {server_dir}</p>
        '''.format(bak_file=os.path.basename(bak_file), file_size=human_size(os.path.getsize(bak_file)),
                   server_type='OSS', server_dir=__aliyun__.get('oss_bucket'))

        try:
            '''获取硬盘信息'''
            process = subprocess.Popen(['df', '-h'], stdout=subprocess.PIPE)
            out, err = process.communicate()
            content = content + u'''
                <p><b>服务器硬盘信息：</b><pre style='padding:10px;background-color:#eee'>{disk_info}</pre></p>
            '''.format(disk_info=out)
        except Exception:
            pass
        send_email(subject, content, ['<hidden>', '<hidden>'])
    finally:
        # 5，删除备份过程中的中间文件
        shutil.rmtree(data_dir)

    write_log("=========================== end : backup data ==========================")
