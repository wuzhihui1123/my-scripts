#!/usr/bin/python
# -*- coding: utf-8 -*-
import math
import os
import shutil
import smtplib
import subprocess
import time
import traceback
from email.mime.text import MIMEText
from tarfile import TarFile

import oss2

_config_ = {
    "oss": {
        'access_key': '<hidden>',
        'access_secret': '<hidden>',
        'oss_endpoint': 'http://oss-cn-beijing.aliyuncs.com',
        'oss_bucket': '<hidden>',
    },
    "mysql": {
        'username': '<hidden>',
        'password': '<hidden>',
        'port': '3306'
    },
    "email": {
        "smtp": {
            'server': "smtp.163.com",
            'port': 465,
            'username': '<hidden>',
            'password': '<hidden>'
        }
    },
    "backup": {
        "host_ip": "<hidden>",
        'database': "<hidden>",
        "compress_pwd": "<hidden>",
        "email_addressee": ["<hidden>"]
    }
}

CURR_DIR = os.path.dirname(os.path.abspath(__file__))


def get_compress_pwd():
    return _config_.get("backup", {}).get("compress_pwd", "")


def get_host_ip():
    return _config_.get("backup", {}).get("host_ip", "").strip() or "ip-unknown"


def get_email_addressee():
    return _config_.get("backup", {}).get("email_addressee", [])


def get_backup_db():
    return _config_.get("backup", {}).get("database")


def prepare_runtime_env():
    try:
        subprocess.call(['pip', 'install', 'oss2'])
        subprocess.call(['apt-get', 'install', 'p7zip-full'])
    except:
        write_log("prepare_runtime_env fail")


def write_log(content):
    log_dir = os.path.join(CURR_DIR, 'logs')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    with open(os.path.join(log_dir, "{}.log".format(time.strftime("%Y-%m"))), 'a') as log_file:
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
        return 'unknown'


def log_exception(func):
    """
    记录异常到日志的装饰器函数， 在函数定义前面实用@log_exception 来装饰函数， 这样函数的异常信息会记录到日志文件中
    :param func:
    :return:
    """

    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except Exception as e:
            exc_track_info = traceback.format_exc()
            write_log(exc_track_info)

            subject = u'[X] 数据备份失败({}/{})'.format(get_host_ip(), get_backup_db())
            content = u'''
                <p><b>错误信息：</b><pre style='padding:10px;background-color:#eee'>{err_info}</pre></p>
            '''.format(err_info=exc_track_info)
            send_email(subject, content, get_email_addressee())
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

    oss_config = _config_.get("oss")
    if not oss_config:
        raise Exception('oss config miss!')

    host_ip = get_host_ip()
    oss_key = "{}/{}".format(host_ip, os.path.basename(file_path))

    start_time = time.time()
    auth = oss2.Auth(oss_config.get('access_key'), oss_config.get('access_secret'))
    bucket = oss2.Bucket(auth, oss_config.get('oss_endpoint'), oss_config.get('oss_bucket'))
    with open(file_path, 'rb') as f:
        bucket.put_object(oss_key, f)

    use_time = math.floor(time.time() - start_time)
    message = "upload file to aliyun success,  oss-key {}, file: {}, use seconds:  {}s".format(oss_key, file_path, use_time)
    write_log(message)
    return oss_key


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

    smtp_config = _config_.get("email", {}).get("smtp")
    if not smtp_config:
        raise Exception('email smtp config miss')

    msg = MIMEText(content, 'html', 'utf-8')
    msg['From'] = smtp_config.get('username')
    msg['To'] = ' , '.join(to_addrs)
    msg['Subject'] = subject

    server = smtplib.SMTP_SSL(smtp_config.get('server'), smtp_config.get('port'), timeout=10)
    server.login(smtp_config.get('username'), smtp_config.get('password'))
    server.sendmail(smtp_config.get('username'), to_addrs, msg.as_string())
    server.quit()

    message = "send email to {to_addrs}".format(to_addrs=str(to_addrs))
    write_log(message)


@log_exception
def dump_mysql(sql_file, db_name):
    """
    导出系统数据库数据
    """
    mysql_config = _config_.get("mysql", {})
    if not mysql_config:
        raise Exception('mysql config miss')

    if not db_name:
        raise Exception("db_name miss")

    start_time = time.time()
    os.popen("mysqldump -u {username} -p{password} -P {port} {db_name} > {sql_file}"
             .format(username=mysql_config.get('username'), password=mysql_config.get('password'), port=mysql_config.get('port'), db_name=db_name, sql_file=sql_file))
    use_time = math.floor(time.time() - start_time)
    message = "dump mysql data [{db_name}] to {sql_file}, use seconds:  {second}s".format(db_name=db_name, sql_file=sql_file, second=use_time)
    write_log(message)
    sql_file_size = os.path.getsize(sql_file)
    # 如果备份文件小于10Kb，则认为数据dump失败。 当dump数据库不存在时，会dump出没有数据的sql文件
    if sql_file_size < 10 * 1024:
        raise Exception("mysql dump fail, db:{db_name}".format(db_name=db_name))
    return sql_file


if __name__ == "__main__":
    write_log("=========================== start : backup data ==========================")
    prepare_runtime_env()

    data_dir = os.path.join(CURR_DIR, 'tmp_data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    backup_db = get_backup_db()
    archive_file_list = []
    try:
        bak_time = time.strftime('%Y%m%d%H%M')
        # 导出数据库数据并添加到备份列表中
        sql_file = os.path.join(data_dir, '{db_name}_{bak_time}.sql'.format(db_name=backup_db, bak_time=time.strftime('%Y%m%d%H%M')))
        dump_mysql(sql_file, backup_db)
        archive_file_list.append(sql_file)

        # 打包压缩和加密备份文件
        zip_name = "{}-{}.zip".format(backup_db, time.strftime('%Y%m%d%H%M'))
        zip_file_path = os.path.join(data_dir, zip_name)
        zip_file(zip_file_path, archive_file_list, password=get_compress_pwd())

        # 上传到阿里云对象存储（oss）服务器。
        # 备注：当前阿里云的归档存储服务没有提供归档文件相应的查看和下载界面，所以这里用oss来存储备份文件，
        # 而且oss具有很好的生命周期规则可以设置，从而不需要自己来控制数据的过期删除。
        oss_key = upload_to_aliyun_oss(zip_file_path)

        # 邮件通知备份结果
        subject = u'[√] 数据备份成功({})'.format(oss_key)
        content = u'''
                    <p><b>bucket：</b>   {oss_bucket}</p>
                    <p><b>备份路径：</b>   {oss_key}</p>
                    <p><b>文件大小：</b>   {file_size}</p>
                '''.format(oss_bucket=_config_.get("oss", {}).get('oss_bucket', "-"), oss_key=oss_key, file_size=human_size(os.path.getsize(zip_file_path)))

        try:
            '''获取磁盘信息'''
            process = subprocess.Popen(['df', '-h'], stdout=subprocess.PIPE)
            out, err = process.communicate()
            content = content + u'''
                        <p><b>服务器磁盘信息：</b><pre style='padding:10px;background-color:#eee'>{disk_info}</pre></p>
                    '''.format(disk_info=out)
        except Exception:
            pass
        send_email(subject, content, get_email_addressee())
    finally:
        # 删除备份过程中的中间文件
        shutil.rmtree(data_dir)
        pass

    write_log("=========================== end : backup data ==========================")
