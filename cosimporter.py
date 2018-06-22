from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client, CosServiceError
import sys
import logging
import sys
import importlib.abc
import imp
# from importlib._bootstrap import _load_module_shim

logging.basicConfig(
    # level=logging.DEBUG,
    format = '%(asctime)s - %(name)s - %(filename)s[%(lineno)d] - %(levelname)s - %(message)s',
    stream=sys.stdout)
log = logging.getLogger(__name__)

_cosclient = None
_cosbucket = None

def _install_cos(bucket, secret_id, secret_key, region, token=''):
    global _cosclient, _cosbucket
    config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region=region, Token=token)
    _cosclient = CosS3Client(config)
    _cosbucket = bucket
    install_path_hook()

def _format_path(path):
    return path.replace('\\', '/').strip('/')

def _path_join(*paths):
    return '/'.join(map(_format_path, paths))

_links = {}

def _get_links(key):
    global _cosclient, _cosbucket, _links
    key = _format_path(key) + '/'
    if key.startswith('cos://'):
        key = key[6:]
    if key in _links:
        return _links[key]
    log.debug('get links key: %s', key)
    ret = _cosclient.list_objects(_cosbucket, key, Delimiter="/", MaxKeys=100)
    contents = ret.get('Contents', set())
    commonPrefixes = ret.get('CommonPrefixes', set())
    l = len(key)
    links = {x['Key'][l:] for x in contents if x['Key']!=key}
    links |= {x['Prefix'][l:].rstrip('/') for x in commonPrefixes}
    log.debug('links: %s', links)
    _links[key] = links
    return links

class CosModuleLoader(importlib.abc.SourceLoader):
    def __init__(self, path):
        self._basepath = path
        self._source_cache = {}

    def module_repr(self, module):
        return '<cosmodule %r from %r>' % (module.__name__, module.__file__)

    # def load_module1(self, fullname): # 这个方法不能解决包内相对导入问题， 不实现使用默认方法
    #     log.debug('load_module: %s', fullname)
    #     return _load_module_shim(self, fullname)
    #     code = self.get_code(fullname)
    #     mod = sys.modules.setdefault(fullname, imp.new_module(fullname))
    #     mod.__name__ = fullname
    #     mod.__file__ = self.get_filename(fullname)
    #     mod.__loader__ = self
    #     mod.__package__ = fullname.rpartition('.')[0]
    #     if self.is_package:
    #         mod.__path__ = [self._basepath]
    #     log.debug('mod attr:%s', mod.__dict__)
    #     exec(code, mod.__dict__)
    #     return mod

    def get_code(self, fullname):
        src = self.get_source(fullname)
        return compile(src, self.get_filename(fullname), 'exec', dont_inherit=True)

    def get_data(self, path):
        path = _format_path(path)[6:]
        try:
            u = _cosclient.get_object(_cosbucket, path)
            data = u['Body'].get_raw_stream().read()
            log.debug('loade data: %r loaded', path)
            return data
        except CosServiceError as e:
            log.debug('loade data: %r failed. %s', path, e)
            raise ImportError("Can't load data %s" % path)

    def get_filename(self, fullname):
        filename = _path_join(self._basepath, fullname.split('.')[-1] + '.py')
        if not filename.startswith('cos://'):
            filename = 'cos://' + filename
        return filename

    def get_source(self, fullname):
        global _cosclient, _cosbucket
        filename = self.get_filename(fullname)
        log.debug('loader: reading %r', filename)
        if filename in self._source_cache:
            log.debug('loader: cached %r', filename)
            return self._source_cache[filename]
        source = self.get_data(filename).decode('utf-8')
        self._source_cache[filename] = source
        return source

    def is_package(self, fullname):
        return False

class CosPackageLoader(CosModuleLoader):
    def load_module(self, fullname):
        mod = super().load_module(fullname)
        mod.__path__ = [ self._basepath ]
        mod.__package__ = fullname

    def get_filename(self, fullname):
        filename = _path_join(self._basepath, '__init__.py')
        if not filename.startswith('cos://'):
            filename = 'cos://' + filename
        return filename

    def is_package(self, fullname):
        return True

class CosPathFinder(importlib.abc.PathEntryFinder):
    def __init__(self, baseurl):
        self._links = None
        self._loader = CosModuleLoader(baseurl)
        self._baseurl = baseurl

    def find_loader(self, fullname):
        log.debug('find_loader: %r', fullname)
        parts = fullname.split('.')
        basename = parts[-1]
        # Check link cache
        if self._links is None:
            self._links = [] # See discussion
            self._links = _get_links(self._baseurl)

        # Check if it's a package
        if basename in self._links:
            log.debug('find_loader: trying package %r', fullname)
            fullurl = _path_join(self._baseurl, basename)
            if not fullurl.startswith('cos://'):
                fullurl = 'cos://' + fullurl
            # Attempt to load the package (which accesses __init__.py)
            loader = CosPackageLoader(fullurl)
            try:
                loader.load_module(fullname)
                log.debug('find_loader: package %r loaded', fullname)
            except ImportError as e:
                import traceback as tb
                print(tb.format_exc())
                log.debug('find_loader: %r is a namespace package', fullname)
                loader = None
            return (loader, [fullurl])
        # A normal module
        filename = basename + '.py'
        if filename in self._links:
            log.debug('find_loader: module %r found', fullname)
            return (self._loader, [])
        else:
            log.debug('find_loader: module %r not found', fullname)
            return (None, [])

    def invalidate_caches(self):
        log.debug('invalidating link cache')
        self._links = None

_url_path_cache = {}

def handle_url(path):
    if path.startswith('cos://'):
        log.debug('Handle path? %s. [Yes]', path)
        path = path[6:]
        if path in _url_path_cache:
            finder = _url_path_cache[path]
        else:
            finder = CosPathFinder(path)
            _url_path_cache[path] = finder
        return finder
    else:
        log.debug('Handle path? %s. [No]', path)

def install_path_hook():
    sys.path_hooks.append(handle_url)
    sys.path_importer_cache.clear()
    log.debug('Installing handle_url')

def remove_path_hook():
    sys.path_hooks.remove(handle_url)
    sys.path_importer_cache.clear()
    log.debug('Removing handle_url')

if __name__ == '__main__':
    pass