# cosimporter
把py代码放在腾讯云上

# requirement
需安装腾讯云的python sdk
[cos-python-sdk-v5](https://github.com/tencentyun/cos-python-sdk-v5)

# 用法
首先把代码放到腾讯cos的code目录下：
```python
# test_remote.py
print('nice job')

```
然后再python的repl中执行：
```
>>> import cosimporter
>>> bucket = 'xxxx-xxxxx'
>>> secret_id = 'xxxxx'
>>> secret_key = 'xxxxx'
>>> region = 'ap-xxxx'
>>> token = ''
>>> cosimporter._install_cos(bucket, secret_id, secret_key, region, token)
>>> import sys
>>> sys.path.append('cos://code/')
>>> import test_remote
nice job
>>> import inspect
>>> inspect.getsourcefile(test_remote)
'cos://code/test_remote.py'
>>> inspect.getsource(test_remote)
"print('nice job')\n"
>>>
```
已支持, 源码, 字节码文件, 扩展文件（因平台后缀名不同， 例如win上只能加载.pyd文件， linux可以加载.so文件）
支持pkgutil.get_data()来获取数据文件内容

# 遗留问题
+ 目前只支持一个cos的账号，不支持多个cos的账号同时导入