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

# 遗留问题
+ 目前只支持py文件格式，暂不支持pyd, so等其他python可加载的格式
+ 目前只支持一个cos的账号，不支持多个cos的账号同时导入